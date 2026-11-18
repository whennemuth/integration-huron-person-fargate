import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, BillingMode, ProjectionType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { IGrantable } from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import { accessTableName } from '../src/authorization/RoleAssignmentStore';
import { auditTableName, PRINCIPAL_INDEX } from '../src/audit/AuditStore';

/**
 * A landscape dashboard's own tables (authorization-plan.md, "Infrastructure"): who holds which roles, and the audit
 * trail. Roles are per landscape (decision 6), so each dashboard stack owns its pair.
 *
 * Both are destroyed with the stack: a teardown deletes the role assignments and the audit trail, and after a
 * redeploy the first Admin is seeded again (decision 4; the fargate README's "Dashboard access").
 */
export class AccessTables extends Construct {
  readonly accessTable: Table;
  readonly auditTable: Table;

  constructor(scope: Construct, id: string, params: { stackId: string; landscape: string }) {
    super(scope, id);
    const { stackId, landscape } = params;

    // One item per role held (sk = the role's name), and one #PROFILE item per person seen
    this.accessTable = new Table(this, 'access', {
      tableName: accessTableName(stackId, landscape),
      partitionKey: { name: 'buPrincipal', type: AttributeType.STRING },
      sortKey: { name: 'sk', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Every record in one partition (pk = AUDIT), sorted by `<ISO time>#<id>`, and an index for one person's
    this.auditTable = new Table(this, 'audit', {
      tableName: auditTableName(stackId, landscape),
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      sortKey: { name: 'sk', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY
    });
    this.auditTable.addGlobalSecondaryIndex({
      indexName: PRINCIPAL_INDEX,
      partitionKey: { name: 'buPrincipal', type: AttributeType.STRING },
      sortKey: { name: 'sk', type: AttributeType.STRING },
      projectionType: ProjectionType.ALL
    });
  }

  /**
   * The dashboard Lambda reads and writes both: role changes, profiles and audit records. It deletes only role items
   * (a retracted role); the audit store has no delete.
   */
  grantTo(grantee: IGrantable): void {
    this.accessTable.grantReadWriteData(grantee);
    this.auditTable.grantReadWriteData(grantee);
  }

  /** The Lambda's environment variables naming them */
  get environment(): { ACCESS_TABLE_NAME: string; AUDIT_TABLE_NAME: string } {
    return { ACCESS_TABLE_NAME: this.accessTable.tableName, AUDIT_TABLE_NAME: this.auditTable.tableName };
  }
}
