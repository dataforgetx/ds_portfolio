# About 
This project automates the workflow (currently manually done by analysts using R) by which DFPS sends data to DPS and receives data back from DPS for data processing. 

The automated workflow is as follows:

## Step 1: Send file to DPS

- `send2dps.py` file to pull data from oracle database and does some data wrangling and spits out two files
  - end_export.xlsx (for reference and archive)
  - End_txt_file.txt (to be sent to DPS)
- send the end_txt_file.txt to DPS via SFTP
- Archive both files by moving them to a directory for storage and review.
- Logs processing information for auditing and debugging.

## Step 2: Receive files from DPS to process it

- DPS processes the data (takes two hours usually) and leaves two processed files in ‘TO_DFPS’ directory in the same SFTP server.
  - Dfps-missing-person-results.txt
  - Dfps-missing-person-counts.txt
- Fetch those two files via SFTP
- `receivedps.py` pulls data from multiple data sources (the two files fetched from dps, sa_98_data, file sent to DPS, a county-region lookup file from database) and spits out three files listed below (FY means fiscal year, qtr means quarter, 2025 fiscal year lasts from Sep/2024 to Aug/2025)
  - IMPACT_not_in_DPS_FY_QTR.xlsx
  - DPS_not_in_IMPACT_FY_QTR.xlsx
  - DPS_total_runaway_events_FY_QTR.xlsx
- Send the three files to a designated network drive location so that stakeholders from other DFPS departments can access them. 
- Archive the two files fetched from DFPS by moving them to a directory for storage and review
- Logs processing information for auditing and debugging.

### Files:

- `send2dps.py` - script that prepares files and send file to DPS.
- `receivedps.py` - script that retrieve files from DPS and does additional processing and upload to sharepoint and email internal stakeholders.
- `util.py` - scrip that handles send/retrieve files through SFTP, connect to database, archive files, and other helper methods that can be called by previous two scripts.
- `dps_config.yml` - file for environment configs (sql, database configs, and directories) if necessary.
- shell scripts are called by cron jobs for automated processing.
