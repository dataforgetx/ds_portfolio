# About

This project automates the ETL validation processes currently done manually by analysts. The ETL, known as 'Monthly Data Compilation (MDC)', pulls data 
from various sources and transforms and then loads them into our oracle data warehouse. This automation does not touch our databricks data pipelines, as those are 
separate processes stored and managed in our azure infrastructure. 

## Goals
This automation attempts to accomplish the following:
1. Automate the existing manual validation processes by creating necessary database objects (table, procedures, functions, and packages) to be integrated into ETL pipeline.
2. Expand the number of validation checks by adding column-level counts and corresponding percentage of change to avoid gross checks that often fail to detect abnormal changes in columns (current validation only includes at table counts).
3. Optimize validation reports by including more conspicuous warning icons to avoid human errors such as failing to detect abnormal counts even if they exist.
4. Optimize validation reports by formatting validation results in easily digestible and visually appealing way (in html) and sending the html reports to appropriate analysts.

## Design 
The idea is to combine existing validation logic and add additional validation checks, specifically column counts and % in change.
For details, please see `MDC_VALIDATION_DESIGN` document. The whole process follows the following steps:
1. INITIALIZE
   - Get current time load from TIME_DIM
   - Set time load ranges (default: CurrentTL-2 to CurrentTL)
   - Handle special cases (November/October for WARE/QAWH)
   - Clear/initialize results table (if used)
   - Set up spooling/output

1. RUN EXISTING MDC_COUNTS VALIDATIONS
   - Tables/indexes without stats
   - Invalid objects
   - ARCH comparisons
   - Data volume consistency
   - Current time load counts
   - CCL comparison
   - Store results in results table

1. RUN EXISTING ROW COUNT VALIDATIONS
   - Loop through active config entries with VALIDATION_TYPE = 'ROW_COUNT'
   - Execute row count queries (with grouping if GROUP_BY_COLUMNS specified)
   - Calculate differences using LAG functions
   - Store results in results table

1. RUN EXISTING TABLE COMPARISON VALIDATIONS
   - Identify tables with COMPARE_TO_TABLE set
   - Execute comparison queries
   - Flag mismatches
   - Store results in results table

1. RUN COLUMN COUNT VALIDATIONS (NEW)
   - For each active table
   - Get column list from ALL_TAB_COLUMNS (exclude BLOB, CLOB if needed)
   - Count non-null values per column per time load
   - Calculate % change per column
   - Store results in results table

1. CALCULATE PERCENTAGE CHANGES (NEW)
   - For all row counts and column counts
   - Compare current vs prior time load
   - Apply thresholds from config
   - Flag violations (> threshold)
   - Update STATUS and SEVERITY in results table

1. GENERATE EMAIL REPORTS
   - Group results by EMAIL_ADDRESS
   - Format HTML email content with color coding
   - Generate email per analyst
   - Send emails (using UTL_MAIL or external procedure)

1. OUTPUT SUMMARY
   - DBMS_OUTPUT summary
   - Total tables validated
   - Total issues found
   - Email distribution summary

