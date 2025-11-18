CREATE TABLE caps.STG_DPS_RESULTS
(
    CPS_NAME          VARCHAR2(100),   -- CPS name (last,first format)
    FULL_NAME         VARCHAR2(100),   -- Full name from DPS
    CPS_DOB           DATE,            -- Date of birth (YYYY-MM-DD format)
    S                 VARCHAR2(1),     -- Sex code (F/M)
    R                 VARCHAR2(1),     -- Race code
    E                 VARCHAR2(1),     -- Ethnicity code
    DOE               DATE,            -- Date of entry
    LAST_CONT         DATE,            -- Last contact date (YYYY-MM-DD format)
    HR                VARCHAR2(10),    -- HR code
    ORI               VARCHAR2(20),    -- ORI code (e.g., TX2270000)
    ORI_DESC          VARCHAR2(200),   -- ORI description (e.g., TRAVIS CO SO AUSTIN)
    COUNTY_NAME       VARCHAR2(100),   -- County name
    ORI_PHONE         VARCHAR2(20),    -- ORI phone number
    NIC               VARCHAR2(20),    -- NIC number
    STS               VARCHAR2(10),   -- Status code (ACTV, CLRD, CANC, LOC)
    LOCATE_DTE        DATE,            -- Locate date (YYYY-MM-DD format)
    CLR_CAN_DTE       DATE,            -- Clear/Cancel date (YYYY-MM-DD format)
    dt_loaded         DATE             -- Timestamp when record was loaded (SYSDATE)
);

-- Add comments to columns for documentation
COMMENT ON TABLE caps.STG_DPS_RESULTS IS 'Staging table for DPS missing person results. Contains only most recent data loaded from DPS files.';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.CPS_NAME IS 'CPS name in last,first format';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.FULL_NAME IS 'Full name from DPS system';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.CPS_DOB IS 'Date of birth';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.S IS 'Sex code (F/M)';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.R IS 'Race code';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.E IS 'Ethnicity code';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.DOE IS 'Date of entry';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.LAST_CONT IS 'Last contact date';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.HR IS 'HR code';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.ORI IS 'ORI (Originating Agency Identifier) code';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.ORI_DESC IS 'ORI description/agency name';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.COUNTY_NAME IS 'County name';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.ORI_PHONE IS 'ORI phone number';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.NIC IS 'NIC (National Incident-Based Reporting System) number';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.STS IS 'Status code (ACTV=Active, CLRD=Cleared, CANC=Cancelled, LOC=Located)';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.LOCATE_DTE IS 'Date person was located';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.CLR_CAN_DTE IS 'Date case was cleared or cancelled';
COMMENT ON COLUMN caps.STG_DPS_RESULTS.dt_loaded IS 'Timestamp when record was loaded into staging table';

-- Grant necessary permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON caps.STG_DPS_RESULTS TO PUBLIC;

