-----------------------------------------------------------------------
-- SQL*Loader Control File for ETL Files Staging Table
-- Loads pipe-delimited file created by sync_etl_files.sh
-----------------------------------------------------------------------

OPTIONS (
    ERRORS=1000,
    DIRECT=FALSE,
    SKIP=0
)

LOAD DATA
INFILE '/rhome/fuh2/mrs/crossref2/etl_files_export.dat'
BADFILE '/rhome/fuh2/mrs/crossref2/etl_files_export.bad'
DISCARDFILE '/rhome/fuh2/mrs/crossref2/etl_files_export.dsc'
APPEND
INTO TABLE ram.a$ETL_Files_Staging
FIELDS TERMINATED BY '|'
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
    filename        CHAR(200),
    file_path       CHAR(500),
    line_num        INTEGER EXTERNAL,
    line_content    CHAR(4000),
    file_type       CHAR(10),
    load_timestamp  DATE "YYYY-MM-DD HH24:MI:SS"
)

