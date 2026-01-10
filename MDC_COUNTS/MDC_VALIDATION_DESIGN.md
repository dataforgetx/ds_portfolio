# MDC Validation System - Design Document

## 1. Overview

This document outlines the design for automating manual ETL validation scripts and consolidating them with the existing `mdc_counts.sql` procedure. The system will validate table counts, column counts, perform table comparisons, calculate percentage changes, and distribute results via HTML email to assigned analysts.

## 2. Architecture

### 2.1 Package Structure

- **`PKG_MDC_VALIDATION`** - Main validation package containing all procedures
- **`TBL_MDC_VALIDATION_CONFIG`** - Configuration table for tables, rules, and email assignments
- **`TBL_MDC_VALIDATION_RESULTS`** - Results storage table (optional, for email generation and historical tracking)
- Email utility procedures for generating and sending HTML emails

### 2.2 Main Components

1. **Configuration Management** - Table-driven configuration for all validations
2. **Validation Engine** - Executes all validation types
3. **Results Processing** - Calculates percentages, applies thresholds, flags violations
4. **Email Generation** - Creates HTML emails with color-coded severity indicators
5. **Email Distribution** - Sends results to assigned analysts

## 3. Configuration Table Structure

### 3.1 TBL_MDC_VALIDATION_CONFIG

```sql
CREATE TABLE TBL_MDC_VALIDATION_CONFIG (
    CONFIG_ID              NUMBER PRIMARY KEY,
    OWNER                  VARCHAR2(10),      -- CAPS, CCL, SWI, HR, PEI
    TABLE_NAME             VARCHAR2(128),
    VALIDATION_TYPE        VARCHAR2(50),      -- ROW_COUNT, COLUMN_COUNT, TABLE_COMPARE, etc.
    IS_ACTIVE              CHAR(1) DEFAULT 'Y', -- Y/N
    GROUP_BY_COLUMNS       VARCHAR2(500),     -- Comma-separated: metric_type,cd_program,cd_svc_type
    COMPARE_TO_TABLE       VARCHAR2(128),     -- For table comparisons: e.g., FAD_SUM compares to FAD_FACT
    THRESHOLD_PCT          NUMBER DEFAULT 10, -- Default 10, override per table (e.g., FCL tables use 2-3%)
    EMAIL_ADDRESS          VARCHAR2(255),     -- Placeholder: 'analyst1@example.com', 'analyst2@example.com'
    NOTES                  VARCHAR2(4000),    -- Validation rules, special handling notes
    PRIORITY               VARCHAR2(20),      -- HIGH, MEDIUM, LOW
    TIME_LOAD_RANGE_START  NUMBER,            -- Default: CurrentTL-2, override if needed
    TIME_LOAD_RANGE_END    NUMBER,            -- Default: CurrentTL
    CREATED_DATE           DATE DEFAULT SYSDATE,
    UPDATED_DATE           DATE
);
```

### 3.2 Configuration Notes

- **Multiple Config Rows**: Tables with multiple grouping dimensions get multiple config rows
- **Table Comparison Pairs**: Table comparison pairs get separate config entries
- **Email Sharing**: Email addresses can be shared across multiple tables
- **Special Thresholds**: FCL tables use 2-3% threshold (not default 10%)
- **Grouping Columns**: Examples include `metric_type`, `cd_program`, `cd_svc_type`, `cd_stage_program`, `nbr_federal_year`, `cd_federal_period`

### 3.3 Example Configuration Entries

```
OWNER: CAPS, TABLE: FAD_SUM
  - VALIDATION_TYPE: ROW_COUNT
  - COMPARE_TO_TABLE: FAD_FACT
  - THRESHOLD_PCT: 10
  - EMAIL_ADDRESS: analyst1@example.com

OWNER: CAPS, TABLE: FAD_SUM
  - VALIDATION_TYPE: COLUMN_COUNT
  - THRESHOLD_PCT: 10
  - EMAIL_ADDRESS: analyst1@example.com

OWNER: CAPS, TABLE: FCL_PMC_CHILD
  - VALIDATION_TYPE: ROW_COUNT
  - THRESHOLD_PCT: 2  (special threshold per script comments)
  - EMAIL_ADDRESS: analyst2@example.com

OWNER: CAPS, TABLE: MTR_TREND_MNTH_SUM
  - VALIDATION_TYPE: ROW_COUNT
  - GROUP_BY_COLUMNS: metric_type
  - THRESHOLD_PCT: 10
  - EMAIL_ADDRESS: analyst3@example.com

OWNER: CAPS, TABLE: INT_SUM
  - VALIDATION_TYPE: ROW_COUNT
  - GROUP_BY_COLUMNS: cd_program
  - THRESHOLD_PCT: 10
  - EMAIL_ADDRESS: analyst4@example.com
```

## 4. Validation Types

### 4.1 Existing mdc_counts.sql Validations

These validations are preserved from the existing procedure:

1. **Tables without statistics** - Identifies tables that haven't been analyzed
2. **Indexes without statistics** - Identifies indexes that haven't been analyzed
3. **Invalid objects** - Finds invalid database objects (procedures, functions, views, etc.)
4. **ARCH table comparisons** - Compares table counts to ARCH table counts
5. **Data volume consistency checks** - Validates consistent data volume across time loads
6. **Current time load counts** - Lists record count for current time load in every table
7. **CCL comparison** - Compares CCL.FACILITY_FACT vs CCL.CCL_FACILITY_SUM record counts

### 4.2 Row Count Validations (from manual scripts)

- **Basic row counts** by `id_time_load`
- **Row counts with grouping** (metric_type, cd_program, cd_svc_type, etc.)
- **Row counts with dt_load_process grouping** (where applicable, e.g., FPS tables)
- **Difference calculations** (current vs prior time load using LAG functions)

### 4.3 Table Comparison Validations (from manual scripts)

- FAD_SUM vs FAD_FACT
- FAD_INTERESTS_DIM vs FAD_SUM/FAD_FACT
- CCL.CCL_FACILITY_SUM vs CCL.FACILITY_FACT
- Other FACT vs SUM comparisons
- Match/no-match indicators with detailed counts

### 4.4 Column Count Validations (NEW)

- For each table, count **non-null values per column** by `id_time_load`
- Output format: **Column Name (X-axis) × Time Load (Y-axis)** matrix
- Track column population trends over time
- Calculate percentage changes per column

### 4.5 Percentage Change Validations (NEW)

- Calculate % change: `((current - prior) / prior) * 100`
- Apply table-specific thresholds (default 10%, overrides like 2-3% for FCL)
- Flag violations with clear indicators
- Handle division by zero cases (when prior = 0)

## 5. Results Storage

### 5.1 TBL_MDC_VALIDATION_RESULTS

```sql
CREATE TABLE TBL_MDC_VALIDATION_RESULTS (
    RESULT_ID          NUMBER PRIMARY KEY,
    RUN_DATE           DATE DEFAULT SYSDATE,
    OWNER              VARCHAR2(10),
    TABLE_NAME         VARCHAR2(128),
    VALIDATION_TYPE    VARCHAR2(50),
    TIME_LOAD          NUMBER,
    COLUMN_NAME        VARCHAR2(128),        -- Nullable, for column counts
    GROUP_BY_VALUE     VARCHAR2(500),        -- For grouped validations
    COUNT_VALUE        NUMBER,
    PRIOR_COUNT        NUMBER,               -- For % change calculations
    PCT_CHANGE         NUMBER,
    STATUS             VARCHAR2(20),         -- PASS, WARNING, ERROR
    MESSAGE            VARCHAR2(4000),
    EMAIL_ADDRESS      VARCHAR2(255),        -- From config
    SEVERITY           VARCHAR2(20),         -- HIGH, MEDIUM, LOW
    COMPARE_TABLE      VARCHAR2(128),        -- For table comparisons
    COMPARE_COUNT      NUMBER,               -- For table comparisons
    MATCH_STATUS       VARCHAR2(50)          -- MATCH, NO_MATCH, DOES_NOT_EXIST
);
```

### 5.2 Benefits of Results Storage

- Historical tracking of validation results
- Easier email generation (query results table)
- Audit trail for compliance
- Performance metrics over time
- Trend analysis capabilities

## 6. Main Execution Flow

### 6.1 RUN_FULL_VALIDATION Procedure

```
1. INITIALIZE
   - Get current time load from TIME_DIM
   - Set time load ranges (default: CurrentTL-2 to CurrentTL)
   - Handle special cases (November/October for WARE/QAWH)
   - Clear/initialize results table (if used)
   - Set up spooling/output

2. RUN EXISTING MDC_COUNTS VALIDATIONS
   - Tables/indexes without stats
   - Invalid objects
   - ARCH comparisons
   - Data volume consistency
   - Current time load counts
   - CCL comparison
   - Store results in results table

3. RUN ROW COUNT VALIDATIONS (from config)
   - Loop through active config entries with VALIDATION_TYPE = 'ROW_COUNT'
   - Execute row count queries (with grouping if GROUP_BY_COLUMNS specified)
   - Calculate differences using LAG functions
   - Store results in results table

4. RUN TABLE COMPARISON VALIDATIONS
   - Identify tables with COMPARE_TO_TABLE set
   - Execute comparison queries
   - Flag mismatches
   - Store results in results table

5. RUN COLUMN COUNT VALIDATIONS (NEW)
   - For each active table
   - Get column list from ALL_TAB_COLUMNS (exclude BLOB, CLOB if needed)
   - Count non-null values per column per time load
   - Calculate % change per column
   - Store results in results table

6. CALCULATE PERCENTAGE CHANGES (NEW)
   - For all row counts and column counts
   - Compare current vs prior time load
   - Apply thresholds from config
   - Flag violations (> threshold)
   - Update STATUS and SEVERITY in results table

7. GENERATE EMAIL REPORTS
   - Group results by EMAIL_ADDRESS
   - Format HTML email content with color coding
   - Generate email per analyst
   - Send emails (using UTL_MAIL or external procedure)

8. OUTPUT SUMMARY
   - DBMS_OUTPUT summary
   - Total tables validated
   - Total issues found
   - Email distribution summary
```

## 7. Email Distribution System

### 7.1 Email Utility Procedure

**`SEND_VALIDATION_RESULTS`** procedure:

- Groups results by `EMAIL_ADDRESS` from config
- Generates formatted HTML email per analyst
- Includes all assigned tables and their validation results
- Color-codes based on severity thresholds

### 7.2 HTML Email Format Specifications

#### 7.2.1 Color Coding Scheme

- **Green**: % change ≤ 5% (acceptable, no action needed)
- **Yellow**: % change > 5% and ≤ 10% (warning, review recommended)
- **Red**: % change > 10% (error/violation, requires investigation)

#### 7.2.2 HTML Structure

**Email Content Sections:**

1. **Header Section**

   - Run date and time
   - Analyst name/email
   - Total assigned tables
   - Database name

2. **Executive Summary**

   - Total issues found
   - Breakdown by severity (High/Medium/Low)
   - Count of tables with violations
   - Quick action items

3. **High Priority Issues Section**

   - Red violations first (sorted by severity)
   - Table name, time load, % change, details
   - Direct links to detailed sections (if applicable)

4. **Row Count Validations Section**

   - Table name
   - Time load
   - Current count
   - Prior count
   - % change (color-coded)
   - Status (PASS/WARNING/ERROR)
   - Grouping values (if applicable)

5. **Column Count Validations Section**

   - Table name
   - Column name
   - Time load
   - Non-null count
   - Prior count
   - % change (color-coded)
   - Status

6. **Table Comparisons Section**

   - Table1 vs Table2
   - Time load
   - Table1 count
   - Table2 count
   - Match status (MATCH/NO_MATCH/DOES_NOT_EXIST)
   - Difference

7. **Detailed Results Section**
   - Expandable/collapsible sections (if interactive HTML)
   - Full details for each validation
   - Historical trends (if available)

#### 7.2.3 HTML Table Example

```html
<table
  border="1"
  cellpadding="5"
  cellspacing="0"
  style="border-collapse: collapse; width: 100%;"
>
  <thead>
    <tr style="background-color: #f0f0f0; font-weight: bold;">
      <th>Table</th>
      <th>Time Load</th>
      <th>Count</th>
      <th>Prior Count</th>
      <th>% Change</th>
      <th>Status</th>
    </tr>
  </thead>
  <tbody>
    <!-- Green row: <=5% -->
    <tr style="background-color: #90EE90;">
      <td>FAD_SUM</td>
      <td>100388</td>
      <td>5000</td>
      <td>4950</td>
      <td>1.01%</td>
      <td>PASS</td>
    </tr>
    <!-- Yellow row: 5-10% -->
    <tr style="background-color: #FFFFE0;">
      <td>FAD_SUM</td>
      <td>100387</td>
      <td>5200</td>
      <td>5000</td>
      <td>4.00%</td>
      <td>WARNING</td>
    </tr>
    <!-- Red row: >10% -->
    <tr style="background-color: #FFB6C1;">
      <td>FAD_SUM</td>
      <td>100386</td>
      <td>6000</td>
      <td>5000</td>
      <td>20.00%</td>
      <td>ERROR</td>
    </tr>
  </tbody>
</table>
```

#### 7.2.4 Email Subject Line Format

```
MDC Validation Results - [Date] - [Analyst Name/Email]
```

Example: `MDC Validation Results - 2024-01-15 - analyst1@example.com`

### 7.3 Email Implementation Options

**Option A: Oracle UTL_MAIL (if available)**

- Native Oracle email functionality
- Requires SMTP server configuration
- Direct PL/SQL implementation
- Simple to implement

**Option B: External procedure/Java**

- Call external email service
- More flexible
- Requires additional setup
- Better for complex email formatting

**Option C: Write to file + external script**

- Generate formatted HTML files per analyst
- External script sends emails
- Separation of concerns
- Easier to test and debug

**Recommendation:** Start with Option C (file generation), add Option A later if needed.

## 8. Special Handling Requirements

### 8.1 From Script Analysis

- **FCL tables**: 2-3% threshold (not default 10%)
- **FPS tables**: Handle `dt_load_process` grouping
- **HIST tables**: Different time load ranges (yearly vs monthly)
- **Tables with multiple grouping dimensions**: Create multiple config rows
- **Tables with @ware links**: Handle schema links appropriately
- **Tables with special date ranges**: Configurable time load ranges
- **NYTD_FACT and HIST tables**: BaseTL = CurrentTL (not CurrentTL-2)
- **HIST_DIM tables**: BaseTL = CurrentTL-2
- **November/October special cases**: BaseTL = CurrentTL-13 for WARE/QAWH

### 8.2 Table-Specific Rules

**FCL_PMC_CHILD:**

- Typically, change > 2% up/down may need investigation
- December months may have > 2% change (holiday time off, adoption month)
- Exits bounce around but there will always be SOME exits

**FCL_PMC_CHILD_PLCMT:**

- Row count may not match other counts (children with no placement event)
- Count of PIDs should match fcl_pmc_child count
- Change of 3% up or down may need research

**FAD tables:**

- FAD_SUM should match FAD_FACT
- FAD_INTERESTS_DIM should match FAD_SUM and FAD_FACT

**CCL tables:**

- CCL.CCL_FACILITY_SUM should match CCL.FACILITY_FACT for all time loads

## 9. Configuration Data Population

### 9.1 Initial Data Load Strategy

1. **Extract all tables** from manual scripts in `current/` folder
2. **Identify grouping columns** from GROUP BY clauses in queries
3. **Identify table comparison pairs** from comparison queries
4. **Set thresholds** based on script comments and validation rules
5. **Add placeholder email addresses** (to be filled manually)
6. **Categorize by owner** (CAPS, CCL, SWI, HR, PEI)

### 9.2 Tables to Include (from manual scripts)

**CAPS Schema:**

- FAD tables (FAD_SUM, FAD_FACT, FAD_INTERESTS_DIM, FAD_PRINC_DIM, FAD_TRAINING_DIM, FAD_WORKER_DIM)
- FCL tables (FCL_PMC_CHILD, FCL_PMC_CHILD_PLCMT, FCL_13_TMC_CHILD_PLCMT)
- FPS tables (FPS_REG_SUM, FPS_CNTY_SUM, FPS_CNTY_TAB_SUM)
- HIST tables (CSA_EPISODE_HIST_FACT, SXV_HIST_DIM, VC_NOTF_HIST_DIM, TRAFFICKING_HIST_DIM, MSNG_CHILD_HIST_DIM, MSNG_CHILD_RCVRY_HIST_DIM, PCSP_FACT, RISK_REASMNT_HIST_DIM, CPS_RA_HIST_DIM, FP_HIST_DIM, FP_PARTCPNT_HIST_DIM)
- Metric tables (MTR_TREND_MNTH_SUM, MTR_TREND_FY_HIST_SUM, MTR_TRENDING_SUM, MTR_TREND_WRK_MNTH_SUM, MTR_TREND_WRK_FY_HIST_SUM, MTR_TRENDING_WRK_SUM)
- ICPC tables (ICPC_DOCUMENT_HIST_FACT, ICPC_RQST_HIST_FACT, ICPC_STAT_HIST_FACT)
- TARE tables (TARE_CHILD_CHAR_SUM, TARE_CHILD_SUM, TARE_FAMILY_SUM, TARE_INQUIRY_SUM)
- APS tables (APS_ASMT_HIST_FACT, APS_ASMT_RORAFLW_HIST_DIM, APS_ASMT_RSPNS_HIST_DIM, APS_SP_MNTRNG_HIST_DIM, APS_SP_PROB_ACTN_HIST_DIM, APS_SRVC_PLAN_HIST_FACT)
- CSL tables (CSL_SUM, CSL_AVG_REG_CNTY_SUM, CSL_DAILY_KIN_WRKR_SUM, CSL_DAILY_LSP_WRKR_SUM, CSL_DAILY_SFI_WRKR_SUM, CSL_DAILY_WKLD_SUM, CSL_DAILY_WORKER_SUM)
- PCS/CHL/CMN tables (PCS_CNTRTR_SFTY_ALLEG_SUM, PCS_CNTRTR_SFTY_SUM, PCS_DAILY_SBLNG_SUM, PCS_PLCMT_SUM, CHL_DAILY_WORKER_SUM, CMN_CONTACT_APS_SUM, CMN_CONTACT_CPS_HIST_DIM, CMN_CONTACT_CPS_SUM, CMN_FGDM_SUM, CMN_PCSP_SUM)
- INT/CALL tables (CALL_SUM, INT_SUM, INT_AFC_FACT, INT_APS_FACT, INT_CALL_FACT, INT_LIC_FACT, INT_CASE_SPC_FACT, INT_CPS_FACT)
- Other tables (SVC_SUM, CASE_READ_SUM, CASE_IMAGE_AUDIT_SUM, PP_SUM, AFCARS tables, NCANDS_FACT, FN tables)

**CCL Schema:**

- All CCL tables from Part 1, 2, 3 scripts (facility_fact, ccl_facility_sum, inv_hist_fact, corrv_act_hist_dim, waiver_hist_fact, bkgrnd_chk_hist_dim, etc.)

**SWI Schema:**

- All SWI tables from SWI*Counts_CD.sql (AVY*_ tables, SWI\__ tables)

## 10. Output Enhancements

### 10.1 DBMS_OUTPUT Sections

1. **Configuration Summary**

   - Total tables configured
   - Total analysts assigned
   - Threshold distribution
   - Active vs inactive validations

2. **Existing mdc_counts Results**

   - Tables/indexes without stats
   - Invalid objects
   - ARCH comparisons
   - Data volume consistency
   - Current time load counts
   - CCL comparison

3. **Row Count Validations** (grouped by owner)

   - All row count results
   - Grouped validations
   - Difference calculations

4. **Column Count Validations** (grouped by table)

   - Column population trends
   - % changes per column

5. **% Change Violations** (sorted by severity)

   - All violations > threshold
   - Color-coded indicators

6. **Table Comparison Results**

   - Match/mismatch summary
   - Detailed comparison counts

7. **Email Distribution Summary**
   - Emails sent per analyst
   - Total emails generated
   - Email generation status

### 10.2 Error Handling

- Graceful handling of missing tables
- Error logging for failed validations
- Continue processing on individual table errors
- Summary of errors at end of execution

## 11. Implementation Phases

### Phase 1: Configuration Setup

- Create config table structure
- Populate from manual scripts analysis
- Add placeholder email addresses
- Set up initial thresholds and rules

### Phase 2: Core Validation Engine

- Implement row count validations
- Implement table comparison validations
- Integrate existing mdc_counts.sql validations
- Basic results storage

### Phase 3: New Validations

- Column count implementation
- % change calculation engine
- Threshold enforcement logic
- Status and severity assignment

### Phase 4: Email System

- Results grouping by email address
- HTML email formatting with color coding
- Email file generation (Option C)
- Email sending capability (Option A or B)

### Phase 5: Testing & Refinement

- Test with sample data
- Validate email distribution
- Performance optimization
- Error handling refinement
- User acceptance testing

## 12. Key Design Decisions

- **Configuration-driven**: All tables/rules in config table for easy maintenance
- **Modular design**: Each validation type is separate procedure for maintainability
- **Extensible**: Easy to add new validation types without major refactoring
- **Email-ready**: Results structured for efficient email generation
- **Backward compatible**: Preserves all existing mdc_counts.sql functionality
- **Flexible thresholds**: Per-table configuration allows special cases
- **Multi-dimensional**: Supports grouping columns for complex validations
- **Historical tracking**: Results table enables trend analysis
- **HTML email**: Professional, color-coded output for easy review
- **Analyst assignment**: Table-based email distribution for accountability

## 13. Technical Considerations

### 13.1 Performance

- Use bulk operations where possible
- Index results table for efficient queries
- Consider partitioning results table by date
- Optimize dynamic SQL execution
- Cache frequently accessed config data

### 13.2 Security

- Secure email addresses in config table
- Audit trail for configuration changes
- Access control for validation execution
- Secure email transmission (if using UTL_MAIL)

### 13.3 Maintainability

- Clear naming conventions
- Comprehensive comments
- Version control for config changes
- Documentation for special cases
- Regular config table maintenance procedures

## 14. Future Enhancements

- Web interface for viewing results
- Dashboard for validation trends
- Automated threshold adjustment based on historical data
- Integration with monitoring/alerting systems
- Machine learning for anomaly detection
- Real-time validation during ETL process
