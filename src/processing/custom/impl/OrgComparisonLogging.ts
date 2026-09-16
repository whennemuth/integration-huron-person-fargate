import { PersonRecordProcessor } from 'integration-huron-person';
import { AbstractPersonRecordProcessor, Customization } from '../AbstractCustomPersonProcessor';

/**
 * This custom person record processor looks at the rawData of a person record and does
 * the following:
 * - Determines if the person is deemed a student by checking the mapped title field for the
 *   literal value 'Student' (assigned in src\data-mapper\DataMapperTitle.ts from the SAME
 *   orgAssignments.personType that drove the organization/secondaryUnit assignment below - a
 *   more accurate signal than an independent raw.studentInfo check, which can false-positive
 *   for e.g. an employee who also happens to have non-empty studentInfo), and exits early if
 *   not a student.
 * - Compares the Primary and Secondary orgs for the student and exits early if there
 *   is no difference between them.
 * - Having determined to be a student with differing Primary and Secondary orgs, writes
 *   the following JSON structure to the specified DynamoDB table: 
 *     { personid: string, primaryOrg: string, secondaryOrg: string }
 * 
 * NOTE: The src\data-mapper\DataMapperOrg.ts module finds primary and secondary orgs as:
 * - Primary (employer/organization): /studentInfo/studentSemester[x]/studentSemesterInfo/degreeProgram[y]/academicOrganization/code
 * - Secondary (secondaryUnit/additionalUnit): /studentInfo/studentSemester[x]/studentSemesterInfo/degreeProgram[y]/academicGroup/code (only if mapped)
 */
export class OrgComparisonLoggingPersonProcessor extends AbstractPersonRecordProcessor {
  readonly customization = Customization.ORG_COMPARISON_LOGGING;

  processRecord: PersonRecordProcessor = async ({ raw, mapped, error }): Promise<void> => {
    if (error || !mapped) return;

    const title = mapped.fieldValues.find((f: any) => 'title' in f)?.title;
    if (title !== 'Student') return;

    const organization = mapped.fieldValues.find((f: any) => 'organization' in f)?.organization as { hrn?: string } | undefined;
    const secondaryUnit = mapped.fieldValues.find((f: any) => 'secondaryUnit' in f)?.secondaryUnit as { hrn?: string } | undefined;
    if (!secondaryUnit || organization?.hrn === secondaryUnit?.hrn) return;

    const personid = raw?.personid;
    await this.logEntry(this.customization, personid, {
      personid,
      primaryOrg: organization?.hrn,
      secondaryOrg: secondaryUnit?.hrn
    });
  }
}

