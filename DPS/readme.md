This project automates the workflow (currently manually done by analysts using R) by which DFPS sends data to DPS and receives data back from DPS for data processing. The workflow is as follows:

1. Send file to DPS

- `send2dps.py` file to pull data from oracle database and does some data wrangling and spits out two files
  - end_export.xlsx (for reference and archive)
  - End_txt_file.txt (to be sent to DPS)
- send the end_txt_file.txt to DPS via SFTP
- The SFTP details are here:
  - address: 'url'
  - folder: ‘from_folder’
  - username: 'username'
  - password: 'password'
- Archive both files by moving them to a directory for storage and review.
  - Archive directory: /home/scripts/dps_batch/archive/to_dps/
- Logs produced during processing should be stored here:
  - Log directory: /home/scripts/dps_batch/logs/to_dps/

2. Receive files from DPS to process it

- DPS processes the data (takes two hours usually) and leaves two processed files in ‘TO_DFPS’ directory in the same SFTP server.
  - Dfps-missing-person-results.txt
  - Dfps-missing-person-counts.txt
- Fetch those two files via SFTP
- `receivedps.py` pulls data from multiple data sources (the two files fetched from dps, sa_98_data, file sent to DPS, a county-region lookup file from database) and spits out three files listed below (FY means fiscal year, qtr means quarter, 2025 fiscal year lasts from Sep/2024 to Aug/2025)
  - IMPACT_not_in_DPS_FY_QTR.xlsx
  - DPS_not_in_IMPACT_FY_QTR.xlsx
  - DPS_total_runaway_events_FY_QTR.xlsx
- sa_98 data, instead of a flat file as shown in the R script, can be retrieved from the database using: `select * from caps.qtr_dps_cvs_pkg.get_sa_98(fy_start_dt, fy_end_dt)`, `fy_start_dt` is the start date of the fiscal year, for instance, for fy 2025, that would be `09/01/2024`, and `fy_end_dt` would be end date for the fiscal year, for fy 2025, that would be `08/31/2025`.
- county-region lookup table can be retrieved from `select cnty_name, sub_reg "region" from caps.cnty_reg_tableau`.
- Email the three files to `somebody@dfps.texas.gov`
- Archive the two files fetched from DFPS by moving them to a directory for storage and review
  - Archive directory: /home/scripts/dps_batch/archive/from_dps/
- Logs produced during processing should be stored here:
  - Log directory: /home/scripts/dps_batch/logs/from_dps/

Files:

- `send2dps.py` - script that prepares files and send file to DPS
- `receivedps.py` - script that retrieve files from DPS and does additional processing and upload to sharepoint and email internal stakeholders.
- `util.py` - scrip that handles send/retrieve files through SFTP, connect to database, archive files, and other helper methods that can be called by previous two scripts.
- `dps_config.yml` - file for environment configs (sql, database configs, and directories) if necessary
- shell scripts are called by cron jobs for automated processing
