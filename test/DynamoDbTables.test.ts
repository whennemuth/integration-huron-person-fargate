/**
 * Tests the statistics table's secondary indexes as lib/DynamoDB.ts synthesizes them: the run-list-index
 * that the run reporting reads a page of runs from by key, beside the distinct-run-index it replaces.
 */
import { App, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
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
  const template = Template.fromStack(stack);

  it('has the run-list-index: one partition of run markers, sorted by syncRunId, keys only', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'test-stack-statistics-preview',
      GlobalSecondaryIndexes: Match.arrayWith([{
        IndexName: 'run-list-index',
        KeySchema: [{ AttributeName: 'runList', KeyType: 'HASH' }, { AttributeName: 'distinctRunId', KeyType: 'RANGE' }],
        Projection: { ProjectionType: 'KEYS_ONLY' }
      }]),
      // arrayWith matches in order: distinctRunId is defined first, by the distinct-run-index
      AttributeDefinitions: Match.arrayWith([
        { AttributeName: 'distinctRunId', AttributeType: 'S' },
        { AttributeName: 'runList', AttributeType: 'S' }
      ])
    });
  });

  it('keeps its other indexes, so this deploy adds exactly one', () => {
    const [table] = Object.values(template.findResources('AWS::DynamoDB::Table', {
      Properties: { TableName: 'test-stack-statistics-preview' }
    }));
    expect(table.Properties.GlobalSecondaryIndexes.map((index: any) => index.IndexName))
      .toEqual(['errorType-timestamp-index', 'distinct-run-index', 'run-list-index']);
  });
});
