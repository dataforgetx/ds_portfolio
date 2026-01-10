-- ============================================================================
-- MDC Validation System - Complete Database Setup Script
-- ============================================================================
-- Purpose: Creates all database objects for Phase 1.1
--          Run this script to set up the complete database structure
-- ============================================================================

SET ECHO ON
SET FEEDBACK ON
SET VERIFY OFF

PROMPT ============================================================================
PROMPT Creating MDC Validation System Database Objects
PROMPT ============================================================================

PROMPT
PROMPT Step 1: Creating configuration table...
@01_create_config_table.sql

PROMPT
PROMPT Step 2: Creating results table...
@02_create_results_table.sql

PROMPT
PROMPT Step 3: Creating sequences...
@03_create_sequences.sql

PROMPT
PROMPT ============================================================================
PROMPT Database objects created successfully!
PROMPT ============================================================================
PROMPT
PROMPT Next steps:
PROMPT 1. Populate TBL_MDC_VALIDATION_CONFIG with table configurations
PROMPT 2. Update email addresses in configuration table
PROMPT 3. Create validation package (Phase 2)
PROMPT ============================================================================

