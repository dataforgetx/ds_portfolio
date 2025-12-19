## About

This project improves an existing oracle stored procedure that is used to research code references in existing 
data engineering codebase in DFPS. The old procedure, `mrs.xref`, only searches through ODSI Tech Team (OTT)'s codebase,
which is only a portion of the codebase that is involved in the ETL and data warehouse work in DFPS. 

This project expands the search by incorporating the codebase from IT data team(IDT), because ODSI Tech Team and IT data
team are the two primary stakeholders in the ETL and data warehousing work in DFPS.

### Step 1: Parse codebase 
A shell script is written to parse IDT's codebase to include sql and shell scripts and then parsed content is saved. 
Due to the size of the codebase, `awk` is used instead of standard shell loops to speed up the processing.

### Step 2: Load to staging table
Parsed data is loaded into a staging table in oracle database using SQL*LOADER.

### Step 3: Expand procedure
A new oracle stored procedure is created to expand the search by including the newly created staging table.

### Step 4: Automate process
The shell script is automated using a cron job on linux server to sync the staging table with the codebase, so that
the search results will be current.

## Usage
```sql
--positional arguments
exec mrs.xref2('token')

--named arguments
begin
  mrs.xref2(
    p_token        => 'token',
    p_search_etl   => 'Y',
    p_detail       => 'N',
    p_all_codetype => 'N'
  );
end;
```




