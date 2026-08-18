# Integration Runner

## Overview

The integration runner is used to "kick off" an integration run. It can be configured to do so in multiple ways/modes. 

Most modes assume parallel processing.
The fundamental flow common to all modes in parallel processing is depicted below:

```mermaid
flowchart LR
  S[Start] --> R
  subgraph R[Launch]
    direction TB
      A[.env Configuration] --> B[Runner]
      B -->|Message| Q1[Queue]
      Q1 -->|Metric assessment ↻| AL[Alarm ✓]
      AL -->|Queue depth above zero detected| E[Chunking Service]
      E -->|↑↑↑ Scales up and writes files| F[S3 Bucket]
      F -->|Appearance of files in bucket trigger... | L1[Lambda → ⚙]
      L1 -->|Message| Q2[Queue] 
      Q2 -->|Queue depth above zero detected| P[Processor service Triggered → ⚙]
      E <-->|Consumes messages and restocks with more to fuel upcoming tasks until done| Q1
  end
  R --> QD[Queue Depleted]
  QD --> C
  subgraph C[Completion]
    direction TB
      H[Queue] -->|Depleted of messages| J[Alarm ✗]
      J -->|Empty queue detected| K[Chunking Service]
      K -->|↓↓↓ Scales down to zero| L2[Processor service finishes]
      L2 -->|Merger service runs one task| END[ ⊘ End]
  end
```

## Modes of Operation (TODO: Complete these...)

### No concurrency

### Single person mode (testing)

### Concurrency mode

### Concurrency mode with queue seeding

### Source Simulator decorator mode