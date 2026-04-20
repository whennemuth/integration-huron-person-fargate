# Post-Processing Delta Storage Merging

This directory contains functionality for the post-processing delta storage merging step of the Huron Person integration, called into play only if chunked processing is used for parallel processing. This step consolidates the chunked delta outputs - each representing a subset of the overall person population - from parallel processing into a single baseline file that can then be merged with the previous baseline to comprise the final delta for the current run.

## Overview

When the chunked processing triggers the "EndToEnd" flow, that EndToEnd module "believes" it is processing a full sync and is only being called once for the entire sync operation, for the entire person population returned by original call tothe source system.
But the processing of the data source is actually scoped to just the chunk file, representing a subset of the overall person population, and the "EndToEnd" flow is called once per chunk, with each chunk having its own isolated delta storage that it reads from and writes to.

When the "EndToEnd" flow executes the DeltaStorage.updatePreviousData method, it won't be be overwriting a global previous-input.ndjson file, but instead writing to a chunk-specific, unique delta storage path derived from the chunk ID (as part of parallel processing). Thus, no prior state is ever actually being overwritten, though we let the "EndToEnd" flow maintain the illusion that it is doing so.

What this means is that the "previous-input.ndjson" files written by each chunk are actually just intermediate outputs that represent the delta state for that chunk, and not the true baseline for the entire sync operation. The true baseline is only created after all chunks have completed and their outputs are consolidated together in this post-processing merging step.

## Architecture Diagrams

The following diagrams illustrate the two operational modes of the integration: without chunking (sequential processing) and with chunking (parallel processing).

### 1. Non-Chunked Sync (Sequential Processing - No Merging Required)

```mermaid
sequenceDiagram
    participant API as Source API
    participant Framework as Integration Framework<br/>(DataSource → EndToEnd → DeltaStorage → DataMapper)
    participant S3 as S3 Bucket
    participant Target as DataTarget (Huron API)
    
    Note over API,Target: Single Execution - Full Population
    
    API->>Framework: GET /api/people (Full Population)
    Note over Framework: DataSource receives full dataset
    
    Note over Framework: EndToEnd: Request previous baseline
    Framework->>S3: DeltaStorage.fetchPreviousData()<br/>Read delta-storage/person-full/previous-input.ndjson
    S3-->>Framework: Previous baseline (or empty if first run)
    
    Note over Framework: EndToEnd: Compute Delta<br/>(Compare hashes: current vs previous)<br/>Only changed/new records proceed
    
    Note over Framework: DataMapper: Transform delta records
    
    Framework->>Target: Bulk sync (changed records only)
    Target-->>Framework: Success
    
    Note over Framework: EndToEnd: Update baseline with current data
    Framework->>S3: DeltaStorage.updatePreviousData()<br/>OVERWRITE delta-storage/person-full/previous-input.ndjson
    Note over S3: Direct overwrite - no merging needed<br/>New baseline = current data
    S3-->>Framework: Success
    
    Note over Framework: Sync complete
```

### 2. Chunked Sync (Parallel Processing - Merging Required)

#### Overview Diagram 
Note: Each loop of "Phase 2" is essentially the same unchunked process depicted above, injected into a wrapper for parallel execution.

<i>(see detailed sequence diagrams below for each phase further down)</i>

```mermaid
sequenceDiagram
    participant API as Source API
    participant Chunker as Chunker<br/>(Phase 1)
    participant S3Chunks as S3 Chunks<br/>Bucket
    participant SQS as SQS Queue
    participant Processor as Processor<br/>(Phase 2)
    participant S3Delta as S3 Delta<br/>Bucket
    participant Merger as Merger<br/>(Phase 3)
    
    rect rgb(200, 230, 255)
    Note over API,SQS: Phase 1: Chunking
    API->>Chunker: GET /api/people<br/>(Full Population)
    Chunker->>Chunker: Break into N chunks<br/>(BigJsonFile)
    Chunker->>S3Chunks: Write N chunk files
    Chunker->>SQS: Send N chunk messages
    end
    
    rect rgb(255, 230, 200)
    Note over SQS,S3Delta: Phase 2: Parallel Processing (N tasks)
    loop For each chunk
        SQS->>Processor: Chunk message
        Processor->>S3Chunks: Read chunk file
        Processor->>S3Delta: Read shared baseline
        Processor->>Processor: EndToEnd: Compute Delta<br/>Map & Sync to Huron API
        Processor->>S3Delta: Write chunk-specific delta
    end
    end
    
    rect rgb(230, 255, 230)
    Note over SQS,Merger: Phase 3: Consolidation & Merging
    SQS->>Merger: All chunks complete
    Merger->>S3Delta: Read & consolidate all chunk deltas
    Merger->>S3Delta: Read existing integrated baseline
    Merger->>Merger: HashMapMerger:<br/>4-way merge logic
    Merger->>S3Delta: Write new integrated baseline
    Merger->>S3Delta: Cleanup chunk deltas
    end
```

#### Phase 1: Chunking (Detail)

```mermaid
sequenceDiagram
    participant API as Source API
    participant Chunker as Chunker<br/>(Phase 1)
    participant S3Chunks as S3 Chunks<br/>Bucket
    participant SQS as SQS Queue
    
    rect rgb(200, 230, 255)
    Note over API,SQS: Phase 1: Chunking - Break full dataset into processable chunks
    
    API->>Chunker: GET /api/people<br/>(Full Population)
    Note over Chunker: BigJsonFile.breakup()<br/>Stream large JSON<br/>Split into chunks
    
    Chunker->>S3Chunks: Write chunks/.../timestamp/<br/>chunk-0000.ndjson
    Chunker->>S3Chunks: Write chunks/.../timestamp/<br/>chunk-0001.ndjson
    Chunker->>S3Chunks: ...
    Chunker->>S3Chunks: Write chunks/.../timestamp/<br/>chunk-NNNN.ndjson
    
    Note over Chunker: Create metadata file<br/>with total chunk count
    Chunker->>S3Chunks: Write chunks/.../timestamp/<br/>metadata.json
    
    Chunker->>SQS: Send chunk-0000 message
    Chunker->>SQS: Send chunk-0001 message
    Chunker->>SQS: ...
    Chunker->>SQS: Send chunk-NNNN message
    
    Note over Chunker,SQS: N messages queued for processing
    end
```

#### Phase 2: Parallel Processing (Detail)

```mermaid
sequenceDiagram
    participant SQS as SQS Queue
    participant P1 as Processor<br/>Task 1
    participant P2 as Processor<br/>Task 2
    participant PN as Processor<br/>Task N
    participant S3Delta as S3 Delta<br/>Bucket
    
    rect rgb(255, 230, 200)
    Note over SQS,S3Delta: Phase 2: Parallel Processing - Each chunk runs EndToEnd independently
    
    par Parallel Execution
        SQS->>P1: Chunk 0 message
        P1->>P1: Read chunk-0000.ndjson<br/>from S3 Chunks
        P1->>S3Delta: Read integrated<br/>previous-input.ndjson<br/>(shared baseline)
        P1->>P1: EndToEnd:<br/>Compute Delta<br/>(chunk vs baseline)
        P1->>P1: DataMapper:<br/>Transform records
        P1->>P1: Sync to Huron API<br/>(chunk subset)
        P1->>S3Delta: Write deltas/.../timestamp/<br/>chunk-0000.ndjson<br/>(chunk-specific delta)
        Note over P1,S3Delta: S3 PutObject event triggers<br/>MergerSubscriber lambda<br/>("Are we there yet?" check)
    and
        SQS->>P2: Chunk 1 message
        P2->>P2: Read chunk-0001.ndjson<br/>from S3 Chunks
        P2->>S3Delta: Read integrated<br/>previous-input.ndjson<br/>(shared baseline)
        P2->>P2: EndToEnd:<br/>Compute Delta
        P2->>P2: DataMapper:<br/>Transform records
        P2->>P2: Sync to Huron API<br/>(chunk subset)
        P2->>S3Delta: Write deltas/.../timestamp/<br/>chunk-0001.ndjson
        Note over P2,S3Delta: S3 PutObject event triggers<br/>MergerSubscriber lambda<br/>("Are we there yet?" check)
    and
        SQS->>PN: Chunk N message
        PN->>PN: Read chunk-NNNN.ndjson<br/>from S3 Chunks
        PN->>S3Delta: Read integrated<br/>previous-input.ndjson<br/>(shared baseline)
        PN->>PN: EndToEnd:<br/>Compute Delta
        PN->>PN: DataMapper:<br/>Transform records
        PN->>PN: Sync to Huron API<br/>(chunk subset)
        PN->>S3Delta: Write deltas/.../timestamp/<br/>chunk-NNNN.ndjson<br/>(Last chunk!)
        Note over PN,S3Delta: S3 PutObject event triggers<br/>MergerSubscriber lambda<br/>Check: N chunks == total?<br/>YES → Send merge message to SQS
    end
    
    Note over SQS,S3Delta: MergerSubscriber lambda monitors chunk completion<br/>When last chunk detected, triggers Phase 3
    end
```

#### Phase 3: Consolidation & Merging (Detail)

```mermaid
sequenceDiagram
    participant SQS as SQS Queue
    participant Merger as Merger<br/>(Phase 3)
    participant HM as HashMap<br/>Merger
    participant S3Temp as S3 Delta Bucket:<br/>Timestamped Deltas<br/>(deltas/.../timestamp/)
    participant S3Base as S3 Delta Bucket:<br/>Integrated Baseline<br/>(delta-storage/.../)
    
    rect rgb(230, 255, 230)
    Note over SQS,S3Base: Phase 3: Consolidation & Merging - Combine chunks and merge with baseline
    
    SQS->>Merger: Merge trigger message<br/>(all chunks complete)
    
    Note over Merger,S3Temp: Step 1: Consolidate chunk deltas
    Merger->>S3Temp: Read chunk-0000.ndjson
    Merger->>S3Temp: Read chunk-0001.ndjson
    Merger->>S3Temp: ...
    Merger->>S3Temp: Read chunk-NNNN.ndjson
    
    Note over Merger: Concatenate all chunk deltas<br/>into single dataset
    
    Merger->>S3Temp: Write previous-input.ndjson<br/>(timestamped consolidated)
    
    Note over Merger,S3Base: Step 2: Merge with existing baseline
    Merger->>S3Base: Read previous-input.ndjson<br/>(existing integrated baseline)
    
    Merger->>HM: merge(existingBaseline,<br/>consolidatedChunks)
    
    Note over HM: HashMapMerger.merge()<br/>4-way merge logic:<br/>• Retained (baseline only)<br/>• Added (chunks only)<br/>• Updated (hash changed)<br/>• Unchanged (same hash)
    
    HM-->>Merger: Merged FieldSets<br/>+ statistics
    
    Merger->>S3Base: MERGE & OVERWRITE<br/>previous-input.ndjson
    
    Note over S3Base: New integrated baseline =<br/>merged(old baseline,<br/>consolidated chunks)
    
    Note over Merger,S3Temp: Step 3: Cleanup
    Merger->>S3Temp: Delete chunk-*.ndjson<br/>(cleanup chunk-specific deltas)
    
    Merger-->>SQS: Complete
    end
```

## Key Differences

### Non-Chunked Mode
- **Single execution**: One EndToEnd run for entire population
- **No merging**: Direct overwrite of previous-input.ndjson
- **Simple delta**: Current data becomes new baseline
- **No consolidation**: Single data source, single output

### Chunked Mode
- **Parallel execution**: N EndToEnd runs (one per chunk)
- **Merging required**: HashMapMerger combines old baseline with new consolidated chunks
- **4-way merge logic**: Tracks retained, added, updated, and unchanged records
- **Two-stage delta**: 
  1. Chunk-specific deltas (intermediate, timestamped)
  2. Integrated baseline (final, shared across all syncs)
- **Consolidation phase**: Merger (Phase 3) combines all chunk outputs before merging with baseline
- **Concurrent completion detection**: MergerSubscriber lambda monitors S3 PutObject events during processing to detect when all chunks are complete
