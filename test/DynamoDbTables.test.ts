/**
 * Tests the statistics table's secondary indexes as lib/DynamoDB.ts synthesizes them: the run-list-index that the
 * run reporting reads a page of runs from by key, which replaced the scan-only distinct-run-index.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { IContext } from '../context/IContext';
import { DynamoDbTables } from '../lib/DynamoDB';

const context = {
  STACK_ID: 'test-stack',
  REGION: 'us-east-2',
  ACCOUNT: '123456789012',
  PREVIOUS_STORAGE_TYPE: 'dynamodb',
  TAGS: { Landscape: 'preview', Service: 'integration', Function: 'person-sync' }
} as unknown as IContext;

describe('the statistics table', () => {
  const stack = new Stack(new App(), 'TestStack', { env: { account: context.ACCOUNT, region: context.REGION } });
  new DynamoDbTables({ scope: stack, id: 'Tables', props: { context } });
  const [table] = Object.values(Template.fromStack(stack).findResources('AWS::DynamoDB::Table', {
    Properties: { TableName: 'test-stack-statistics-preview' }
  }));
  const indexes: any[] = table.Properties.GlobalSecondaryIndexes;

  it('has the run-list-index: one partition of run markers, sorted by syncRunId, keys only', () => {
    expect(indexes.find(index => index.IndexName === 'run-list-index')).toEqual({
      IndexName: 'run-list-index',
      KeySchema: [{ AttributeName: 'runList', KeyType: 'HASH' }, { AttributeName: 'distinctRunId', KeyType: 'RANGE' }],
      Projection: { ProjectionType: 'KEYS_ONLY' }
    });
    expect(table.Properties.AttributeDefinitions).toEqual(expect.arrayContaining([
      { AttributeName: 'runList', AttributeType: 'S' },
      { AttributeName: 'distinctRunId', AttributeType: 'S' }
    ]));
  });

  it('has no distinct-run-index: its two indexes are the error index and the run list', () => {
    expect(indexes.map(index => index.IndexName)).toEqual(['errorType-timestamp-index', 'run-list-index']);
  });
});
