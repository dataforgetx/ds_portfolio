# About

This project automates an existing manual process to fix issues that our automatic purge process fails to address.

_Language Used:_ Oracle PL/SQL Procedures, Packages, Functions, and SQL

## The Problem

Source application must delete records per retention policies, but the data warehouse cannot delete such data because they’re used for historical reporting and aggregation. Instead, records are marked as purged:

- Set purge indicators (ind_case_purged = 1, ind_person_purged = 1, etc.)
- Replace PII (names, SSN, addresses) with 'PURGED'
- Preserve the record structure for reporting

But sometimes records that should be purged aren't because:

- Timing/race conditions
- Data inconsistencies
- Missing relationships
- Process failures

During the monthly ETL, immediately after the purge operation, a procedure is used to check if there are any records that remain to be purged. Then an email is sent to relevant parties to notify the results of the check. If unpurged records exist, a manual process needs to be execute to purge them. Below is the description of the manual process:

## Steps of Manual Purge Fix :

1. You must create a Datafix file as shown above for the error corresponding to the table where the Error occurred in the above Purge Check Results. The file can be named for the month in which the error occurred.  
   a. The first line in the file will always begin with a

```sql
   --RECTIFY: <tablename>
   Example
    --RECTIFY:table_name
```

2. Copy the Data fix file to the server. Make sure the data fix has execute permissions and write permissions.
3. Login to database as yourself (developer).
4. Request to open a tablespace corresponding to the table for which the data fix is going to run.
   Running the data fix will take care of it - exec mrs.open('file_name.sql');
5. This will open the tablespace for the table. Remember to jot down the episode id from DBMS output.
6. This will send a request for the DBA to open the tablespace. DBA (me or whoever is available and has admin rights) will open the Tablespace.
7. login to database as system_user.
8. Run the below commands to fix the purged records that showed as error. [Note: skip the comment or else errors out in TOAD]

```sql
   exec CAPS.a$purgeforce ('table1'); --Need to run with this table to fix other inv
   exec CAPS.a$purgeforce ('table2');
   --Check after datafix
   exec CAPS.a$PurgeCheck(FALSE);
```

9. Last step is to close the table space (when logged in as yourself) - exec mrs.close(‘[episode_id]’);
10. This will send a request for the DBA to Close the tablespace. DBA (me or whoever is available and has admin rights) will Close the Tablespace.

## The Automation

This project automates all the steps described from above. Here is the before and after comparison:

### Queue Table

| Component          | Before                             | After                                                                   |
| ------------------ | ---------------------------------- | ----------------------------------------------------------------------- |
| **Error Tracking** | Errors only in email notifications | Errors logged to `CAPS.a$purgecheck_errors_queue` table                 |
| **Error Status**   | No status tracking                 | Status tracked: PENDING, PROCESSING, COMPLETED, FAILED, SKIPPED, MANUAL |
| **Error History**  | No persistent history              | Full audit trail with timestamps, retry counts, error messages          |
| **Automation**     | Manual processing required         | Automated processing via queue                                          |

### Error Detection

| Component        | Before                             | After                                                       |
| ---------------- | ---------------------------------- | ----------------------------------------------------------- |
| **Detection**    | `CAPS.a$PurgeCheck` detects errors | `CAPS.a$PurgeCheck` detects errors                          |
| **Logging**      | Errors only in email               | Errors logged to queue table via `a$PurgeCheck_HandleError` |
| **Notification** | Email sent with results            | Email sent + errors queued for automated processing         |

### Error Fixing

| Component                 | Before                                                                               | After                                                       |
| ------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------- |
| **Process**               | Manual steps: create datafix file, open tablespace, run purgeforce, close tablespace | Automated: `a$PurgeCheckAndFix` orchestrates entire process |
| **Tablespace Management** | Manual DBA coordination                                                              | Automated polling with timeout                              |
| **Verification**          | Manual verification after fix                                                        | Automatic verification before closing tablespace            |
| **Prerequisites**         | Manual handling                                                                      | Automatic ordering (inv_sum before other inv\* tables)      |
| **Recovery**              | Manual investigation                                                                 | Automatic reset of stuck entries                            |

### Entry Point

| Component         | Before                             | After                                                                                       |
| ----------------- | ---------------------------------- | ------------------------------------------------------------------------------------------- |
| **sum_purge.sql** | `MRS.Purge;CAPS.a$PurgeCheck;`     | `MRS.Purge;CAPS.a$PurgeCheckAndFix(  p_auto_fix => TRUE,  p_run_purgecheck_first => TRUE);` |
| **Flow**          | Purge → Check → Email → Manual Fix | Purge → Check → Queue → Automated Fix → Verification → Summary Email                        |

---

### New Procedures - What They Do

| Procedure                             | Responsibility                                                                                                                                                                                                                       |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `CAPS.a$PurgeCheck_HandleError`       | Called by `a$PurgeCheck` when errors detected. Logs errors to queue table, prevents duplicates.                                                                                                                                      |
| `CAPS.a$PurgeCheck_PollTablespace`    | Single check function - queries `ram.a$rectifydb_log_master` to see if tablespace episode is in expected status (OPEN/CLOSED).                                                                                                       |
| `CAPS.a$PurgeCheck_WaitForTablespace` | Polling procedure - calls `a$PurgeCheck_PollTablespace` in loop with sleep intervals. Waits for DBA approval with timeout.                                                                                                           |
| `CAPS.a$PurgeCheckAndFix_SingleTable` | Single table fix - creates datafix file, calls `mrs.open()`, waits for DBA, runs `a$purgeforce`, verifies fix, calls `mrs.close()`, waits for DBA, updates queue.                                                                    |
| `CAPS.a$PurgeCheckAndFix`             | Main orchestration - resets stuck entries, runs `a$PurgeCheck`, queries queue, orders tables (handles prerequisites), calls `a$PurgeCheckAndFix_SingleTable` for each, re-runs `a$PurgeCheck` for verification, sends summary email. |
| `CAPS.a$PurgeCheck_ResetStuckEntries` | Recovery procedure - finds entries stuck in PROCESSING status, resets them to PENDING (or FAILED/MANUAL) so they can be reprocessed. Called automatically by `a$PurgeCheckAndFix` at startup.                                        |

---
