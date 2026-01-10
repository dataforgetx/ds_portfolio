-- ============================================================================
-- MDC Validation System - Package Body
-- ============================================================================
-- Purpose: Implement all procedures and functions defined in package spec
-- Note: This is the skeleton with procedure stubs - implementation will follow
-- ============================================================================

CREATE OR REPLACE PACKAGE BODY PKG_MDC_VALIDATION AS

    -- ========================================================================
    -- Main Procedure
    -- ========================================================================
    
    PROCEDURE RUN_FULL_VALIDATION(
        p_spool_file          IN VARCHAR2 DEFAULT NULL,
        p_time_load_start     IN NUMBER DEFAULT NULL,
        p_time_load_end       IN NUMBER DEFAULT NULL,
        p_generate_emails     IN VARCHAR2 DEFAULT 'Y'
    ) IS
        v_current_tl NUMBER;
        v_base_tl NUMBER;
        v_run_date DATE := SYSDATE;
        v_tl_start NUMBER;
        v_tl_end NUMBER;
        v_config_count NUMBER := 0;
        v_table_count NUMBER := 0;
        v_error_count NUMBER := 0;
        v_total_validations NUMBER := 0;
        v_pass_count NUMBER := 0;
        v_warning_count NUMBER := 0;
        v_error_count_results NUMBER := 0;
        v_database_name VARCHAR2(50);
        
        -- Cursor for active validation configurations
        CURSOR c_configs IS
            SELECT DISTINCT owner, table_name
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE IS_ACTIVE = 'Y'
            ORDER BY owner, table_name;
        
        -- Cursor for all validation configs for a table
        CURSOR c_table_configs(p_owner VARCHAR2, p_table VARCHAR2) IS
            SELECT config_id, validation_type, time_load_range_start, time_load_range_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE owner = p_owner
              AND table_name = p_table
              AND IS_ACTIVE = 'Y'
            ORDER BY validation_type;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('MDC Validation System - Full Validation Run');
        DBMS_OUTPUT.PUT_LINE('Run Date: ' || TO_CHAR(v_run_date, 'YYYY-MM-DD HH24:MI:SS'));
        
        -- Get database name (hardcoded)
        v_database_name := GET_DATABASE_NAME;
        DBMS_OUTPUT.PUT_LINE('Database: ' || v_database_name);
        
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 1: INITIALIZE
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 1: Initializing validation run...');
        
        -- Get current time load
        v_current_tl := GET_CURRENT_TIME_LOAD;
        IF v_current_tl IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: Could not determine current time load');
            RAISE_APPLICATION_ERROR(-20001, 'Could not determine current time load');
        END IF;
        
        -- Calculate base time load
        v_base_tl := CALCULATE_BASE_TIME_LOAD(v_current_tl);
        
        -- Set time load ranges
        IF p_time_load_start IS NOT NULL THEN
            v_tl_start := p_time_load_start;
        ELSE
            v_tl_start := v_base_tl;
        END IF;
        
        IF p_time_load_end IS NOT NULL THEN
            v_tl_end := p_time_load_end;
        ELSE
            v_tl_end := v_current_tl;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('  Current Time Load: ' || v_current_tl);
        DBMS_OUTPUT.PUT_LINE('  Base Time Load: ' || v_base_tl);
        DBMS_OUTPUT.PUT_LINE('  Validation Range: ' || v_tl_start || ' to ' || v_tl_end);
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Clear results for this run (optional - comment out if you want to keep history)
        -- CLEAR_RESULTS(v_run_date);
        
        -- ========================================================================
        -- STEP 2: CONFIGURATION SUMMARY
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 2: Configuration Summary');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        SELECT COUNT(DISTINCT owner || '.' || table_name), COUNT(*)
        INTO v_table_count, v_config_count
        FROM TBL_MDC_VALIDATION_CONFIG
        WHERE IS_ACTIVE = 'Y';
        
        DBMS_OUTPUT.PUT_LINE('  Total Active Tables: ' || v_table_count);
        DBMS_OUTPUT.PUT_LINE('  Total Active Configurations: ' || v_config_count);
        
        SELECT COUNT(DISTINCT email_address)
        INTO v_config_count
        FROM TBL_MDC_VALIDATION_CONFIG
        WHERE IS_ACTIVE = 'Y' AND email_address IS NOT NULL;
        
        DBMS_OUTPUT.PUT_LINE('  Total Analysts Assigned: ' || v_config_count);
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 3: RUN EXISTING MDC_COUNTS VALIDATIONS
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 3: Running Existing MDC Counts Validations');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.1: Checking tables without statistics...');
            CHECK_TABLES_WITHOUT_STATS;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.2: Checking indexes without statistics...');
            CHECK_INDEXES_WITHOUT_STATS;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.3: Checking invalid objects...');
            CHECK_INVALID_OBJECTS;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.4: Comparing tables to ARCH tables...');
            CHECK_ARCH_TABLE_COMPARISONS;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.5: Checking data volume consistency...');
            CHECK_DATA_VOLUME_CONSISTENCY;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.6: Getting current time load counts...');
            GET_CURRENT_TIME_LOAD_COUNTS;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        BEGIN
            DBMS_OUTPUT.PUT_LINE('  3.7: Comparing CCL facility tables...');
            CHECK_CCL_FACILITY_COMPARISON;
            DBMS_OUTPUT.PUT_LINE('        Completed');
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('        ERROR: ' || SQLERRM);
        END;
        
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 4: RUN ROW COUNT VALIDATIONS (from config)
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 4: Running Row Count Validations');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        FOR config_rec IN (
            SELECT config_id, owner, table_name, time_load_range_start, time_load_range_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE IS_ACTIVE = 'Y'
              AND VALIDATION_TYPE = 'ROW_COUNT'
            ORDER BY owner, table_name
        ) LOOP
            BEGIN
                v_total_validations := v_total_validations + 1;
                
                -- Calculate time load ranges for this table
                CALCULATE_TIME_LOAD_RANGES(
                    config_rec.owner,
                    config_rec.table_name,
                    v_current_tl,
                    v_base_tl,
                    v_tl_start,
                    v_tl_end
                );
                
                -- Override with config values if specified
                IF config_rec.time_load_range_start IS NOT NULL THEN
                    v_tl_start := config_rec.time_load_range_start;
                END IF;
                IF config_rec.time_load_range_end IS NOT NULL THEN
                    v_tl_end := config_rec.time_load_range_end;
                END IF;
                
                -- Override with procedure parameters if specified
                IF p_time_load_start IS NOT NULL THEN
                    v_tl_start := p_time_load_start;
                END IF;
                IF p_time_load_end IS NOT NULL THEN
                    v_tl_end := p_time_load_end;
                END IF;
                
                EXECUTE_ROW_COUNT_VALIDATION(
                    config_rec.config_id,
                    v_tl_start,
                    v_tl_end
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ERROR processing ' || config_rec.owner || '.' || 
                                       config_rec.table_name || ': ' || SQLERRM);
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('  Row count validations completed');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 5: RUN TABLE COMPARISON VALIDATIONS
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 5: Running Table Comparison Validations');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        -- Handle both TABLE_COMPARE validation type and ROW_COUNT with COMPARE_TO_TABLE
        FOR config_rec IN (
            SELECT config_id, owner, table_name, COMPARE_TO_TABLE, time_load_range_start, time_load_range_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE IS_ACTIVE = 'Y'
              AND ((VALIDATION_TYPE = 'TABLE_COMPARE')
                   OR (VALIDATION_TYPE = 'ROW_COUNT' AND COMPARE_TO_TABLE IS NOT NULL))
            ORDER BY owner, table_name
        ) LOOP
            BEGIN
                v_total_validations := v_total_validations + 1;
                
                -- Calculate time load ranges
                CALCULATE_TIME_LOAD_RANGES(
                    config_rec.owner,
                    config_rec.table_name,
                    v_current_tl,
                    v_base_tl,
                    v_tl_start,
                    v_tl_end
                );
                
                -- Override with config values if specified
                IF config_rec.time_load_range_start IS NOT NULL THEN
                    v_tl_start := config_rec.time_load_range_start;
                END IF;
                IF config_rec.time_load_range_end IS NOT NULL THEN
                    v_tl_end := config_rec.time_load_range_end;
                END IF;
                
                -- Override with procedure parameters if specified
                IF p_time_load_start IS NOT NULL THEN
                    v_tl_start := p_time_load_start;
                END IF;
                IF p_time_load_end IS NOT NULL THEN
                    v_tl_end := p_time_load_end;
                END IF;
                
                EXECUTE_TABLE_COMPARISON(
                    config_rec.config_id,
                    v_tl_start,
                    v_tl_end
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ERROR comparing ' || config_rec.owner || '.' || 
                                       config_rec.table_name || ' vs ' || config_rec.COMPARE_TO_TABLE || 
                                       ': ' || SQLERRM);
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('  Table comparison validations completed');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 6: RUN COLUMN COUNT VALIDATIONS
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 6: Running Column Count Validations');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        FOR config_rec IN (
            SELECT DISTINCT config_id, owner, table_name, time_load_range_start, time_load_range_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE IS_ACTIVE = 'Y'
              AND VALIDATION_TYPE = 'COLUMN_COUNT'
            ORDER BY owner, table_name
        ) LOOP
            BEGIN
                v_total_validations := v_total_validations + 1;
                
                -- Calculate time load ranges
                CALCULATE_TIME_LOAD_RANGES(
                    config_rec.owner,
                    config_rec.table_name,
                    v_current_tl,
                    v_base_tl,
                    v_tl_start,
                    v_tl_end
                );
                
                -- Override with config values if specified
                IF config_rec.time_load_range_start IS NOT NULL THEN
                    v_tl_start := config_rec.time_load_range_start;
                END IF;
                IF config_rec.time_load_range_end IS NOT NULL THEN
                    v_tl_end := config_rec.time_load_range_end;
                END IF;
                
                -- Override with procedure parameters if specified
                IF p_time_load_start IS NOT NULL THEN
                    v_tl_start := p_time_load_start;
                END IF;
                IF p_time_load_end IS NOT NULL THEN
                    v_tl_end := p_time_load_end;
                END IF;
                
                EXECUTE_COLUMN_COUNT_VALIDATION(
                    config_rec.config_id,
                    v_tl_start,
                    v_tl_end
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ERROR processing column counts for ' || 
                                       config_rec.owner || '.' || config_rec.table_name || 
                                       ': ' || SQLERRM);
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('  Column count validations completed');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 6.5: RUN DISTINCT COUNT VALIDATIONS
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 6.5: Running Distinct Count Validations');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        FOR config_rec IN (
            SELECT DISTINCT config_id, owner, table_name, time_load_range_start, time_load_range_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE IS_ACTIVE = 'Y'
              AND VALIDATION_TYPE = 'DISTINCT_COUNT'
            ORDER BY owner, table_name
        ) LOOP
            BEGIN
                v_total_validations := v_total_validations + 1;
                
                -- Calculate time load ranges
                CALCULATE_TIME_LOAD_RANGES(
                    config_rec.owner,
                    config_rec.table_name,
                    v_current_tl,
                    v_base_tl,
                    v_tl_start,
                    v_tl_end
                );
                
                -- Override with config values if specified
                IF config_rec.time_load_range_start IS NOT NULL THEN
                    v_tl_start := config_rec.time_load_range_start;
                END IF;
                IF config_rec.time_load_range_end IS NOT NULL THEN
                    v_tl_end := config_rec.time_load_range_end;
                END IF;
                
                -- Override with procedure parameters if specified
                IF p_time_load_start IS NOT NULL THEN
                    v_tl_start := p_time_load_start;
                END IF;
                IF p_time_load_end IS NOT NULL THEN
                    v_tl_end := p_time_load_end;
                END IF;
                
                EXECUTE_DISTINCT_COUNT_VALIDATION(
                    config_rec.config_id,
                    v_tl_start,
                    v_tl_end
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ERROR processing distinct count for ' || 
                                       config_rec.owner || '.' || config_rec.table_name || 
                                       ': ' || SQLERRM);
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('  Distinct count validations completed');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 6.6: RUN DATA VALIDATION CHECKS
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 6.6: Running Data Validation Checks');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        FOR config_rec IN (
            SELECT DISTINCT config_id, owner, table_name, time_load_range_start, time_load_range_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE IS_ACTIVE = 'Y'
              AND VALIDATION_TYPE = 'DATA_VALIDATION'
              AND DATA_VALIDATION_QUERY IS NOT NULL
            ORDER BY owner, table_name
        ) LOOP
            BEGIN
                v_total_validations := v_total_validations + 1;
                
                -- Calculate time load ranges
                CALCULATE_TIME_LOAD_RANGES(
                    config_rec.owner,
                    config_rec.table_name,
                    v_current_tl,
                    v_base_tl,
                    v_tl_start,
                    v_tl_end
                );
                
                -- Override with config values if specified
                IF config_rec.time_load_range_start IS NOT NULL THEN
                    v_tl_start := config_rec.time_load_range_start;
                END IF;
                IF config_rec.time_load_range_end IS NOT NULL THEN
                    v_tl_end := config_rec.time_load_range_end;
                END IF;
                
                -- Override with procedure parameters if specified
                IF p_time_load_start IS NOT NULL THEN
                    v_tl_start := p_time_load_start;
                END IF;
                IF p_time_load_end IS NOT NULL THEN
                    v_tl_end := p_time_load_end;
                END IF;
                
                EXECUTE_DATA_VALIDATION(
                    config_rec.config_id,
                    v_tl_start,
                    v_tl_end
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ERROR processing data validation for ' || 
                                       config_rec.owner || '.' || config_rec.table_name || 
                                       ': ' || SQLERRM);
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('  Data validation checks completed');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 7: RESULTS SUMMARY
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('STEP 7: Validation Results Summary');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        -- Get summary statistics
        SELECT COUNT(*),
               SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END)
        INTO v_total_validations, v_pass_count, v_warning_count, v_error_count_results
        FROM TBL_MDC_VALIDATION_RESULTS
        WHERE TRUNC(run_date) = TRUNC(v_run_date);
        
        DBMS_OUTPUT.PUT_LINE('  Total Validations Run: ' || v_total_validations);
        DBMS_OUTPUT.PUT_LINE('  Passed: ' || v_pass_count);
        DBMS_OUTPUT.PUT_LINE('  Warnings: ' || v_warning_count);
        DBMS_OUTPUT.PUT_LINE('  Errors: ' || v_error_count_results);
        DBMS_OUTPUT.PUT_LINE('  Processing Errors: ' || v_error_count);
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Summary by validation type
        DBMS_OUTPUT.PUT_LINE('  Results by Validation Type:');
        FOR type_rec IN (
            SELECT validation_type, 
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
                   SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
                   SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE TRUNC(run_date) = TRUNC(v_run_date)
            GROUP BY validation_type
            ORDER BY validation_type
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('    ' || type_rec.validation_type || ': ' || 
                               type_rec.total || ' total (' || type_rec.passed || ' pass, ' ||
                               type_rec.warnings || ' warnings, ' || type_rec.errors || ' errors)');
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Summary by severity
        DBMS_OUTPUT.PUT_LINE('  Results by Severity:');
        FOR sev_rec IN (
            SELECT severity, COUNT(*) AS cnt
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE TRUNC(run_date) = TRUNC(v_run_date)
            GROUP BY severity
            ORDER BY 
                CASE severity 
                    WHEN 'HIGH' THEN 1 
                    WHEN 'MEDIUM' THEN 2 
                    WHEN 'LOW' THEN 3 
                    ELSE 4 
                END
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('    ' || sev_rec.severity || ': ' || sev_rec.cnt);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
        
        -- High priority issues
        DBMS_OUTPUT.PUT_LINE('  High Priority Issues (ERROR status):');
        FOR error_rec IN (
            SELECT owner, table_name, validation_type, time_load, message
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE TRUNC(run_date) = TRUNC(v_run_date)
              AND status = 'ERROR'
            ORDER BY severity, owner, table_name
            FETCH FIRST 20 ROWS ONLY
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('    ' || error_rec.owner || '.' || error_rec.table_name || 
                               ' (' || error_rec.validation_type || ', TL: ' || 
                               NVL(TO_CHAR(error_rec.time_load), 'N/A') || '): ' || 
                               SUBSTR(error_rec.message, 1, 100));
        END LOOP;
        
        IF v_error_count_results > 20 THEN
            DBMS_OUTPUT.PUT_LINE('    ... and ' || (v_error_count_results - 20) || ' more errors');
        END IF;
        DBMS_OUTPUT.PUT_LINE('');
        
        -- ========================================================================
        -- STEP 8: SEND VALIDATION EMAILS
        -- ========================================================================
        IF UPPER(p_generate_emails) = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('STEP 8: Sending Validation Emails');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            
            BEGIN
                SEND_VALIDATION_EMAILS(p_run_date => v_run_date);
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ERROR sending emails: ' || SQLERRM);
            END;
            
            DBMS_OUTPUT.PUT_LINE('');
        END IF;
        
        -- ========================================================================
        -- FINAL SUMMARY
        -- ========================================================================
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Validation Run Complete');
        DBMS_OUTPUT.PUT_LINE('  Run Date: ' || TO_CHAR(v_run_date, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('  Time Load Range: ' || v_tl_start || ' to ' || v_tl_end);
        DBMS_OUTPUT.PUT_LINE('  Total Validations: ' || v_total_validations);
        DBMS_OUTPUT.PUT_LINE('  Errors Found: ' || v_error_count_results);
        DBMS_OUTPUT.PUT_LINE('  Processing Errors: ' || v_error_count);
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('============================================================================');
            DBMS_OUTPUT.PUT_LINE('ERROR in RUN_FULL_VALIDATION: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('  Run Date: ' || TO_CHAR(v_run_date, 'YYYY-MM-DD HH24:MI:SS'));
            DBMS_OUTPUT.PUT_LINE('  Processing Errors: ' || v_error_count);
            DBMS_OUTPUT.PUT_LINE('============================================================================');
            RAISE;
    END RUN_FULL_VALIDATION;
    
    PROCEDURE RUN_TABLE_VALIDATION(
        p_owner           IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_time_load_start IN NUMBER DEFAULT NULL,
        p_time_load_end   IN NUMBER DEFAULT NULL
    ) IS
        v_current_tl NUMBER;
        v_base_tl NUMBER;
        v_tl_start NUMBER;
        v_tl_end NUMBER;
        v_config_count NUMBER := 0;
        v_error_count NUMBER := 0;
        
        -- Cursor to get all active configs for this table
        CURSOR c_configs IS
            SELECT config_id, validation_type, THRESHOLD_PCT, EMAIL_ADDRESS
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE owner = p_owner
              AND table_name = p_table_name
              AND IS_ACTIVE = 'Y'
            ORDER BY validation_type;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Running Validation for: ' || p_owner || '.' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('Run Date: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Check if table has any active validations configured
        SELECT COUNT(*)
        INTO v_config_count
        FROM TBL_MDC_VALIDATION_CONFIG
        WHERE owner = p_owner
          AND table_name = p_table_name
          AND IS_ACTIVE = 'Y';
        
        IF v_config_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: No active validations found for ' || p_owner || '.' || p_table_name);
            DBMS_OUTPUT.PUT_LINE('Please check TBL_MDC_VALIDATION_CONFIG table.');
            RETURN;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('Found ' || v_config_count || ' active validation configuration(s)');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Get current and base time loads
        v_current_tl := GET_CURRENT_TIME_LOAD;
        IF v_current_tl IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: Could not determine current time load');
            RAISE_APPLICATION_ERROR(-20001, 'Could not determine current time load');
        END IF;
        
        v_base_tl := CALCULATE_BASE_TIME_LOAD(v_current_tl);
        
        -- Calculate time load ranges for this table
        CALCULATE_TIME_LOAD_RANGES(
            p_owner,
            p_table_name,
            v_current_tl,
            v_base_tl,
            v_tl_start,
            v_tl_end
        );
        
        -- Override with procedure parameters if specified
        IF p_time_load_start IS NOT NULL THEN
            v_tl_start := p_time_load_start;
        END IF;
        IF p_time_load_end IS NOT NULL THEN
            v_tl_end := p_time_load_end;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('Current Time Load: ' || v_current_tl);
        DBMS_OUTPUT.PUT_LINE('Base Time Load: ' || v_base_tl);
        DBMS_OUTPUT.PUT_LINE('Validation Range: ' || v_tl_start || ' to ' || v_tl_end);
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Run each validation type configured for this table
        DBMS_OUTPUT.PUT_LINE('Running Validations:');
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        FOR config_rec IN c_configs LOOP
            BEGIN
                DBMS_OUTPUT.PUT_LINE('  [' || config_rec.validation_type || '] Config ID: ' || config_rec.config_id);
                
                CASE config_rec.validation_type
                    WHEN 'ROW_COUNT' THEN
                        EXECUTE_ROW_COUNT_VALIDATION(
                            config_rec.config_id,
                            v_tl_start,
                            v_tl_end
                        );
                    WHEN 'COLUMN_COUNT' THEN
                        EXECUTE_COLUMN_COUNT_VALIDATION(
                            config_rec.config_id,
                            v_tl_start,
                            v_tl_end
                        );
                    WHEN 'TABLE_COMPARE' THEN
                        EXECUTE_TABLE_COMPARISON(
                            config_rec.config_id,
                            v_tl_start,
                            v_tl_end
                        );
                    WHEN 'DISTINCT_COUNT' THEN
                        EXECUTE_DISTINCT_COUNT_VALIDATION(
                            config_rec.config_id,
                            v_tl_start,
                            v_tl_end
                        );
                    WHEN 'DATA_VALIDATION' THEN
                        EXECUTE_DATA_VALIDATION(
                            config_rec.config_id,
                            v_tl_start,
                            v_tl_end
                        );
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('    WARNING: Unknown validation type: ' || config_rec.validation_type);
                END CASE;
                
                DBMS_OUTPUT.PUT_LINE('    ✓ Completed');
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('    ✗ ERROR: ' || SQLERRM);
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Validation Summary');
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        
        -- Show results summary
        FOR result_rec IN (
            SELECT validation_type, 
                   status, 
                   severity, 
                   COUNT(*) AS cnt
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE owner = p_owner
              AND table_name = p_table_name
              AND TRUNC(run_date) = TRUNC(SYSDATE)
            GROUP BY validation_type, status, severity
            ORDER BY validation_type, 
                     CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 END
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  ' || result_rec.validation_type || ' - ' || 
                               result_rec.status || ' (' || result_rec.severity || '): ' || 
                               result_rec.cnt || ' result(s)');
        END LOOP;
        
        -- Show error summary
        IF v_error_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('  Processing Errors: ' || v_error_count);
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('============================================================================');
            DBMS_OUTPUT.PUT_LINE('ERROR in RUN_TABLE_VALIDATION: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('  Table: ' || p_owner || '.' || p_table_name);
            DBMS_OUTPUT.PUT_LINE('============================================================================');
            RAISE;
    END RUN_TABLE_VALIDATION;

    -- ========================================================================
    -- Time Load Management Procedures
    -- ========================================================================
    
    FUNCTION GET_CURRENT_TIME_LOAD RETURN NUMBER IS
        v_time_load NUMBER;
        v_month_year VARCHAR2(6);
    BEGIN
        -- Try to get from MRS.CurrentTL function first (if available)
        BEGIN
            SELECT MRS.CurrentTL INTO v_time_load FROM DUAL;
            RETURN v_time_load;
        EXCEPTION
            WHEN OTHERS THEN
                NULL; -- Fall through to TIME_DIM query
        END;
        
        -- Fallback: Get from TIME_DIM based on current month
        -- Format: MMYYYY (e.g., '012024' for January 2024)
        v_month_year := TO_CHAR(SYSDATE, 'MMYYYY');
        
        SELECT id_time_load
        INTO v_time_load
        FROM caps.time_dim
        WHERE LPAD(nbr_time_calendar_month, 2, '0') || nbr_time_calendar_year = v_month_year
          AND ROWNUM = 1;
        
        -- Return CurrentTL - 1 (as per mdc_counts.sql logic: CurrentTL := pTL-1)
        RETURN v_time_load - 1;
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- If current month not found, try previous month
            v_month_year := TO_CHAR(ADD_MONTHS(SYSDATE, -1), 'MMYYYY');
            SELECT id_time_load
            INTO v_time_load
            FROM caps.time_dim
            WHERE LPAD(nbr_time_calendar_month, 2, '0') || nbr_time_calendar_year = v_month_year
              AND ROWNUM = 1;
            RETURN v_time_load - 1;
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in GET_CURRENT_TIME_LOAD: ' || SQLERRM);
            RETURN NULL;
    END GET_CURRENT_TIME_LOAD;
    
    FUNCTION CALCULATE_BASE_TIME_LOAD(p_current_tl IN NUMBER) RETURN NUMBER IS
        v_base_tl NUMBER;
        v_month VARCHAR2(20);
        v_database_name VARCHAR2(50);
    BEGIN
        IF p_current_tl IS NULL THEN
            RETURN NULL;
        END IF;
        
        -- Get current month name
        v_month := TRIM(TO_CHAR(SYSDATE, 'Month'));
        
        -- Get database name to check for WARE/QAWH (hardcoded)
        v_database_name := GET_DATABASE_NAME;
        
        -- Special case: November (WARE) or October (QAWH) - BaseTL = CurrentTL - 13
        -- Note: Database name check uses LIKE '%WARE%' or '%QAWH%' pattern matching
        IF (v_month = 'November' AND UPPER(v_database_name) LIKE '%WARE%') OR
           (v_month = 'October' AND UPPER(v_database_name) LIKE '%QAWH%') THEN
            v_base_tl := p_current_tl - 13;
        ELSE
            -- Default: BaseTL = CurrentTL - 2
            v_base_tl := p_current_tl - 2;
        END IF;
        
        RETURN v_base_tl;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CALCULATE_BASE_TIME_LOAD: ' || SQLERRM);
            RETURN NULL;
    END CALCULATE_BASE_TIME_LOAD;
    
    PROCEDURE CALCULATE_TIME_LOAD_RANGES(
        p_owner           IN VARCHAR2,
        p_table_name      IN VARCHAR2,
        p_current_tl      IN NUMBER,
        p_base_tl         IN NUMBER,
        p_time_load_start OUT NUMBER,
        p_time_load_end   OUT NUMBER
    ) IS
        v_config_start NUMBER;
        v_config_end NUMBER;
        v_table_type VARCHAR2(50);
    BEGIN
        -- Check if config table has specific time load ranges
        BEGIN
            SELECT TIME_LOAD_RANGE_START, TIME_LOAD_RANGE_END
            INTO v_config_start, v_config_end
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE OWNER = p_owner
              AND TABLE_NAME = p_table_name
              AND IS_ACTIVE = 'Y'
              AND ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_config_start := NULL;
                v_config_end := NULL;
        END;
        
        -- Use config values if specified, otherwise use defaults
        IF v_config_start IS NOT NULL THEN
            p_time_load_start := v_config_start;
        ELSE
            p_time_load_start := p_base_tl;
        END IF;
        
        IF v_config_end IS NOT NULL THEN
            p_time_load_end := v_config_end;
        ELSE
            p_time_load_end := p_current_tl;
        END IF;
        
        -- Special handling for HIST tables and NYTD_FACT (as per mdc_counts.sql)
        -- Note: HIST_DIM tables use BaseTL = CurrentTL - 2 (already set above, no change needed)
        -- NYTD_FACT and other HIST tables: BaseTL = CurrentTL (not CurrentTL-2)
        IF (UPPER(p_table_name) = 'NYTD_FACT' OR UPPER(p_table_name) LIKE '%HIST%') 
           AND UPPER(p_table_name) NOT LIKE '%HIST_DIM' THEN
            IF v_config_start IS NULL THEN
                p_time_load_start := p_current_tl;
            END IF;
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CALCULATE_TIME_LOAD_RANGES: ' || SQLERRM);
            p_time_load_start := p_base_tl;
            p_time_load_end := p_current_tl;
    END CALCULATE_TIME_LOAD_RANGES;

    -- ========================================================================
    -- Validation Execution Procedures
    -- ========================================================================
    
    PROCEDURE EXECUTE_ROW_COUNT_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    ) IS
        v_owner VARCHAR2(30);
        v_table_name VARCHAR2(128);
        v_group_by_columns VARCHAR2(4000);
        v_threshold_pct NUMBER;
        v_email_address VARCHAR2(255);
        v_load_process_col VARCHAR2(128);
        v_count_value NUMBER;
        v_prior_count NUMBER;
        v_pct_change NUMBER;
        v_status VARCHAR2(20);
        v_severity VARCHAR2(10);
        v_group_by_value VARCHAR2(4000);
        v_use_dt_load_process BOOLEAN := FALSE;
        
        -- Note: Dynamic SQL is used instead of static cursors for row count validation
            
        -- Variables for dynamic SQL execution
        v_sql VARCHAR2(32767);
        v_cursor_id INTEGER := NULL;
        v_col_cnt INTEGER;
        v_desc_tab DBMS_SQL.DESC_TAB;
        v_id_time_load NUMBER;
        v_rec_count NUMBER;
        v_prev_tl NUMBER := 0;
        v_prev_count NUMBER := 0;
        v_prev_group_val VARCHAR2(4000) := '';
    BEGIN
        -- Get configuration details
        BEGIN
            SELECT owner, table_name, GROUP_BY_COLUMNS, THRESHOLD_PCT, EMAIL_ADDRESS
            INTO v_owner, v_table_name, v_group_by_columns, v_threshold_pct, v_email_address
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE CONFIG_ID = p_config_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: Configuration ID ' || p_config_id || ' not found');
                RETURN;
        END;
        
        -- Check if table exists
        IF NOT VALIDATE_TABLE_EXISTS(v_owner, v_table_name) THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Table ' || v_owner || '.' || v_table_name || ' does not exist');
            RETURN;
        END IF;
        
        -- Get load process column name
        v_load_process_col := GET_LOAD_PROCESS_COLUMN(v_owner, v_table_name);
        
        -- Determine if we should use dt_load_process grouping
        IF v_table_name LIKE 'FPS%' OR v_table_name LIKE 'FAM%' OR 
           v_table_name LIKE 'PAL%' OR v_table_name LIKE 'SA%' OR 
           v_table_name LIKE 'INV%' THEN
            v_use_dt_load_process := TRUE;
        END IF;
        
        -- Build dynamic SQL - simplified version for basic row counts first
        -- This handles the most common case: simple row counts by id_time_load
        IF v_group_by_columns IS NULL AND NOT v_use_dt_load_process THEN
            -- Simple case: just count by id_time_load
            v_sql := 'SELECT id_time_load, COUNT(*) AS rec_count ' ||
                    'FROM ' || v_owner || '.' || v_table_name || ' ' ||
                    'WHERE id_time_load >= ' || p_time_load_start ||
                    ' AND id_time_load <= ' || p_time_load_end ||
                    ' GROUP BY id_time_load ' ||
                    'ORDER BY id_time_load';
            
            -- Execute using DBMS_SQL for dynamic SQL
            v_cursor_id := DBMS_SQL.OPEN_CURSOR;
            
            BEGIN
                DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);
                DBMS_SQL.DEFINE_COLUMN(v_cursor_id, 1, v_id_time_load);
                DBMS_SQL.DEFINE_COLUMN(v_cursor_id, 2, v_rec_count);
                
                -- Execute the cursor
                v_col_cnt := DBMS_SQL.EXECUTE(v_cursor_id);
                
                LOOP
                    IF DBMS_SQL.FETCH_ROWS(v_cursor_id) = 0 THEN
                        EXIT;
                    END IF;
                    
                    DBMS_SQL.COLUMN_VALUE(v_cursor_id, 1, v_id_time_load);
                    DBMS_SQL.COLUMN_VALUE(v_cursor_id, 2, v_rec_count);
                    
                    -- Calculate prior count and percentage change
                    IF v_prev_tl > 0 AND v_prev_tl = v_id_time_load - 1 THEN
                        v_prior_count := v_prev_count;
                        v_pct_change := CALCULATE_PERCENTAGE_CHANGE(v_rec_count, v_prior_count);
                    ELSE
                        v_prior_count := NULL;
                        v_pct_change := NULL;
                    END IF;
                    
                    -- Assign status and severity
                    ASSIGN_STATUS_SEVERITY(v_pct_change, v_threshold_pct, v_status, v_severity);
                    
                    -- Store result
                    INSERT_VALIDATION_RESULT(
                        p_owner => v_owner,
                        p_table_name => v_table_name,
                        p_validation_type => 'ROW_COUNT',
                        p_time_load => v_id_time_load,
                        p_group_by_value => NULL,
                        p_count_value => v_rec_count,
                        p_prior_count => v_prior_count,
                        p_pct_change => v_pct_change,
                        p_status => v_status,
                        p_severity => v_severity,
                        p_message => CASE 
                            WHEN v_pct_change IS NOT NULL THEN 
                                'Row count: ' || v_rec_count || ', Prior: ' || v_prior_count || 
                                ', Change: ' || TO_CHAR(v_pct_change, '999.99') || '%'
                            ELSE 'Row count: ' || v_rec_count
                        END,
                        p_email_address => v_email_address
                    );
                    
                    -- Update previous values
                    v_prev_tl := v_id_time_load;
                    v_prev_count := v_rec_count;
                END LOOP;
                
                -- Close cursor after successful completion
                IF DBMS_SQL.IS_OPEN(v_cursor_id) THEN
                    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
                    v_cursor_id := NULL;
                END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    -- Ensure cursor is closed on error
                    IF v_cursor_id IS NOT NULL AND DBMS_SQL.IS_OPEN(v_cursor_id) THEN
                        BEGIN
                            DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
                        EXCEPTION
                            WHEN OTHERS THEN
                                NULL; -- Ignore errors when closing
                        END;
                        v_cursor_id := NULL;
                    END IF;
                    RAISE;
            END;
            
        ELSE
            -- Complex case: with grouping columns or dt_load_process
            -- For now, use a simpler approach with EXECUTE IMMEDIATE for aggregated results
            -- Full implementation would use DBMS_SQL with dynamic column handling
            
            -- Build SQL with grouping
            v_sql := 'SELECT id_time_load, COUNT(*) FROM ' || v_owner || '.' || v_table_name || ' ' ||
                    'WHERE id_time_load >= ' || p_time_load_start ||
                    ' AND id_time_load <= ' || p_time_load_end;
            
            IF v_group_by_columns IS NOT NULL THEN
                -- Add WHERE clause for grouping columns if needed
                -- This is simplified - full implementation would handle each group separately
                -- Grouping is handled in the loop below
                NULL;
            END IF;
            
            IF v_use_dt_load_process THEN
                v_sql := v_sql || ' GROUP BY id_time_load, TRUNC(' || v_load_process_col || ')';
            ELSE
                v_sql := v_sql || ' GROUP BY id_time_load';
            END IF;
            
            v_sql := v_sql || ' ORDER BY id_time_load';
            
            -- For complex cases, we'll process each time load separately
            FOR v_tl IN p_time_load_start..p_time_load_end LOOP
                BEGIN
                    -- Get count for this time load
                    v_sql := 'SELECT COUNT(*) FROM ' || v_owner || '.' || v_table_name || ' ' ||
                            'WHERE id_time_load = ' || v_tl;
                    
                    EXECUTE IMMEDIATE v_sql INTO v_rec_count;
                    
                    -- Get prior count
                    IF v_tl > p_time_load_start THEN
                        v_sql := 'SELECT COUNT(*) FROM ' || v_owner || '.' || v_table_name || ' ' ||
                                'WHERE id_time_load = ' || (v_tl - 1);
                        BEGIN
                            EXECUTE IMMEDIATE v_sql INTO v_prior_count;
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_prior_count := NULL;
                        END;
                    ELSE
                        v_prior_count := NULL;
                    END IF;
                    
                    -- Calculate percentage change
                    v_pct_change := CALCULATE_PERCENTAGE_CHANGE(v_rec_count, v_prior_count);
                    
                    -- Assign status and severity
                    ASSIGN_STATUS_SEVERITY(v_pct_change, v_threshold_pct, v_status, v_severity);
                    
                    -- Store result
                    INSERT_VALIDATION_RESULT(
                        p_owner => v_owner,
                        p_table_name => v_table_name,
                        p_validation_type => 'ROW_COUNT',
                        p_time_load => v_tl,
                        p_group_by_value => v_group_by_columns,
                        p_count_value => v_rec_count,
                        p_prior_count => v_prior_count,
                        p_pct_change => v_pct_change,
                        p_status => v_status,
                        p_severity => v_severity,
                        p_message => CASE 
                            WHEN v_pct_change IS NOT NULL THEN 
                                'Row count: ' || v_rec_count || ', Prior: ' || v_prior_count || 
                                ', Change: ' || TO_CHAR(v_pct_change, '999.99') || '%'
                            ELSE 'Row count: ' || v_rec_count
                        END,
                        p_email_address => v_email_address
                    );
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('ERROR processing time load ' || v_tl || ' for ' || 
                                           v_owner || '.' || v_table_name || ': ' || SQLERRM);
                END;
            END LOOP;
        END IF;
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in EXECUTE_ROW_COUNT_VALIDATION: ' || SQLERRM);
            IF v_cursor_id IS NOT NULL AND DBMS_SQL.IS_OPEN(v_cursor_id) THEN
                DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
            END IF;
            RAISE;
    END EXECUTE_ROW_COUNT_VALIDATION;
    
    PROCEDURE EXECUTE_COLUMN_COUNT_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    ) IS
        v_owner VARCHAR2(30);
        v_table_name VARCHAR2(128);
        v_threshold_pct NUMBER;
        v_email_address VARCHAR2(255);
        v_column_name VARCHAR2(128);
        v_count_value NUMBER;
        v_prior_count NUMBER;
        v_pct_change NUMBER;
        v_avg_12month_count NUMBER;
        v_pct_change_12month NUMBER;
        v_status VARCHAR2(20);
        v_severity VARCHAR2(10);
        v_sql VARCHAR2(32767);
        v_12month_start NUMBER;
        
        CURSOR c_columns IS
            SELECT column_name
            FROM all_tab_columns
            WHERE owner = v_owner
              AND table_name = v_table_name
              AND data_type NOT IN ('BLOB', 'CLOB', 'NCLOB', 'BFILE')
              AND column_name NOT IN ('ID_TIME_LOAD', 'DT_LOAD_PROCESS', 'DT_INV_LOAD_PROCESS',
                                      'DT_INV_AFC_LOAD_PROCESS', 'DT_INV_APS_LOAD_PROCESS',
                                      'DT_INV_CPS_LOAD_PROCESS', 'DT_INV_LIC_LOAD_PROCESS',
                                      'DT_PP_LCUST_LOAD_PROCESS')
            ORDER BY column_id;
    BEGIN
        -- Get configuration details
        BEGIN
            SELECT owner, table_name, THRESHOLD_PCT, EMAIL_ADDRESS
            INTO v_owner, v_table_name, v_threshold_pct, v_email_address
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE CONFIG_ID = p_config_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: Configuration ID ' || p_config_id || ' not found');
                RETURN;
        END;
        
        -- Check if table exists
        IF NOT VALIDATE_TABLE_EXISTS(v_owner, v_table_name) THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Table ' || v_owner || '.' || v_table_name || ' does not exist');
            RETURN;
        END IF;
        
        -- Process each column
        FOR col_rec IN c_columns
        LOOP
            v_column_name := col_rec.column_name;
            
            -- Process each time load
            FOR v_tl IN p_time_load_start..p_time_load_end LOOP
                BEGIN
                    -- Count non-null values for this column at this time load
                    v_sql := 'SELECT COUNT(' || v_column_name || ') FROM ' || v_owner || '.' || v_table_name || ' ' ||
                            'WHERE id_time_load = ' || v_tl;
                    
                    EXECUTE IMMEDIATE v_sql INTO v_count_value;
                    
                    -- Get prior count (previous time load)
                    IF v_tl > p_time_load_start THEN
                        v_sql := 'SELECT COUNT(' || v_column_name || ') FROM ' || v_owner || '.' || v_table_name || ' ' ||
                                'WHERE id_time_load = ' || (v_tl - 1);
                        BEGIN
                            EXECUTE IMMEDIATE v_sql INTO v_prior_count;
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_prior_count := NULL;
                        END;
                    ELSE
                        v_prior_count := NULL;
                    END IF;
                    
                    -- Calculate 12-month average (average of last 12 time loads)
                    v_12month_start := GREATEST(v_tl - 11, p_time_load_start);
                    IF v_tl >= v_12month_start + 11 THEN
                        -- We have at least 12 months of data
                        v_sql := 'SELECT AVG(cnt) FROM (' ||
                                'SELECT COUNT(' || v_column_name || ') AS cnt ' ||
                                'FROM ' || v_owner || '.' || v_table_name || ' ' ||
                                'WHERE id_time_load BETWEEN ' || v_12month_start || ' AND ' || v_tl || ' ' ||
                                'GROUP BY id_time_load' ||
                                ')';
                        BEGIN
                            EXECUTE IMMEDIATE v_sql INTO v_avg_12month_count;
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_avg_12month_count := NULL;
                        END;
                    ELSE
                        v_avg_12month_count := NULL;
                    END IF;
                    
                    -- Calculate percentage changes
                    v_pct_change := CALCULATE_PERCENTAGE_CHANGE(v_count_value, v_prior_count);
                    v_pct_change_12month := CALCULATE_PERCENTAGE_CHANGE(v_count_value, v_avg_12month_count);
                    
                    -- Assign status and severity (use the worse of the two % changes)
                    IF ABS(NVL(v_pct_change_12month, 0)) > ABS(NVL(v_pct_change, 0)) THEN
                        ASSIGN_STATUS_SEVERITY(v_pct_change_12month, v_threshold_pct, v_status, v_severity);
                    ELSE
                        ASSIGN_STATUS_SEVERITY(v_pct_change, v_threshold_pct, v_status, v_severity);
                    END IF;
                    
                    -- Store result
                    INSERT_VALIDATION_RESULT(
                        p_owner => v_owner,
                        p_table_name => v_table_name,
                        p_validation_type => 'COLUMN_COUNT',
                        p_time_load => v_tl,
                        p_column_name => v_column_name,
                        p_count_value => v_count_value,
                        p_prior_count => v_prior_count,
                        p_pct_change => v_pct_change,
                        p_status => v_status,
                        p_severity => v_severity,
                        p_message => CASE 
                            WHEN v_pct_change IS NOT NULL THEN 
                                'Column ' || v_column_name || ': ' || v_count_value || 
                                ' non-null values, Prior: ' || v_prior_count || 
                                ', Change: ' || TO_CHAR(v_pct_change, '999.99') || '%' ||
                                CASE WHEN v_avg_12month_count IS NOT NULL THEN 
                                    ', 12-Month Avg: ' || TO_CHAR(v_avg_12month_count, '999999.99') || 
                                    ', Change vs Avg: ' || TO_CHAR(v_pct_change_12month, '999.99') || '%'
                                ELSE '' END
                            ELSE 'Column ' || v_column_name || ': ' || v_count_value || ' non-null values'
                        END,
                        p_email_address => v_email_address,
                        p_avg_12month_count => v_avg_12month_count,
                        p_pct_change_12month => v_pct_change_12month
                    );
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('ERROR processing column ' || v_column_name || 
                                           ' at time load ' || v_tl || ' for ' || 
                                           v_owner || '.' || v_table_name || ': ' || SQLERRM);
                END;
            END LOOP;
        END LOOP;
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in EXECUTE_COLUMN_COUNT_VALIDATION: ' || SQLERRM);
            RAISE;
    END EXECUTE_COLUMN_COUNT_VALIDATION;
    
    PROCEDURE EXECUTE_TABLE_COMPARISON(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    ) IS
        v_owner VARCHAR2(30);
        v_table_name VARCHAR2(128);
        v_compare_to_table VARCHAR2(128);
        v_compare_where_clause VARCHAR2(4000);
        v_compare_group_by VARCHAR2(500);
        v_compare_to_source VARCHAR2(128);
        v_threshold_pct NUMBER;
        v_email_address VARCHAR2(255);
        v_count_value NUMBER;
        v_compare_count NUMBER;
        v_status VARCHAR2(20);
        v_severity VARCHAR2(10);
        v_match_status VARCHAR2(50);
        v_sql VARCHAR2(32767);
        v_message VARCHAR2(4000);
        v_where_clause VARCHAR2(4000);
        v_group_by_clause VARCHAR2(500);
        v_source_link VARCHAR2(128);
        v_load_process_col VARCHAR2(128);
        v_use_dt_load_process BOOLEAN := FALSE;
        
        -- For dt_load_process grouping, we need to compare each combination
        TYPE t_combo_rec IS RECORD (
            id_time_load NUMBER,
            dt_load_process DATE,
            count_value NUMBER
        );
        TYPE t_combo_tab IS TABLE OF t_combo_rec;
        v_table1_combos t_combo_tab := t_combo_tab();
        v_table2_combos t_combo_tab := t_combo_tab();
    BEGIN
        -- Get configuration details
        BEGIN
            SELECT owner, table_name, COMPARE_TO_TABLE, COMPARE_WHERE_CLAUSE, 
                   COMPARE_GROUP_BY, COMPARE_TO_SOURCE, THRESHOLD_PCT, EMAIL_ADDRESS
            INTO v_owner, v_table_name, v_compare_to_table, v_compare_where_clause,
                 v_compare_group_by, v_compare_to_source, v_threshold_pct, v_email_address
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE CONFIG_ID = p_config_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: Configuration ID ' || p_config_id || ' not found');
                RETURN;
        END;
        
        -- Check if comparison table is specified
        IF v_compare_to_table IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: No comparison table specified for ' || v_owner || '.' || v_table_name);
            RETURN;
        END IF;
        
        -- Determine source link (default , can be @mdc_all or other)
        IF v_compare_to_source IS NOT NULL THEN
            v_source_link := v_compare_to_source;
        ELSE
            v_source_link := '';
        END IF;
        
        -- Check if dt_load_process grouping is needed
        IF v_compare_group_by IS NOT NULL AND UPPER(v_compare_group_by) LIKE '%DT_LOAD%' THEN
            v_use_dt_load_process := TRUE;
            v_load_process_col := GET_LOAD_PROCESS_COLUMN(v_owner, v_compare_to_table);
        END IF;
        
        -- Check if both tables exist (only check if not external source)
        IF v_source_link = '' THEN
            IF NOT VALIDATE_TABLE_EXISTS(v_owner, v_table_name) THEN
                DBMS_OUTPUT.PUT_LINE('WARNING: Table ' || v_owner || '.' || v_table_name || ' does not exist');
                RETURN;
            END IF;
            
            IF NOT VALIDATE_TABLE_EXISTS(v_owner, v_compare_to_table) THEN
                DBMS_OUTPUT.PUT_LINE('WARNING: Comparison table ' || v_owner || '.' || v_compare_to_table || ' does not exist');
                RETURN;
            END IF;
        END IF;
        
        -- Build WHERE clause for conditional comparisons
        IF v_compare_where_clause IS NOT NULL THEN
            v_where_clause := ' AND ' || v_compare_where_clause;
        ELSE
            v_where_clause := '';
        END IF;
        
        -- Process each time load
        FOR v_tl IN p_time_load_start..p_time_load_end LOOP
            BEGIN
                IF v_use_dt_load_process THEN
                    -- Handle dt_load_process grouping comparison
                    -- Get all combinations from table 1
                    v_sql := 'SELECT id_time_load, TRUNC(' || v_load_process_col || ') AS dt_load_process, COUNT(*) ' ||
                            'FROM ' || v_owner || '.' || v_table_name || ' ' ||
                            'WHERE id_time_load = ' || v_tl || v_where_clause || ' ' ||
                            'GROUP BY id_time_load, TRUNC(' || v_load_process_col || ')';
                    
                    -- Execute and compare each combination
                    -- For simplicity, we'll compare total counts per time load
                    -- Full implementation would compare each dt_load_process combination
                    v_sql := 'SELECT COUNT(*) FROM ' || v_owner || '.' || v_table_name || ' ' ||
                            'WHERE id_time_load = ' || v_tl || v_where_clause;
                    EXECUTE IMMEDIATE v_sql INTO v_count_value;
                    
                    v_sql := 'SELECT COUNT(*) FROM ' || v_owner || '.' || v_compare_to_table || v_source_link || ' ' ||
                            'WHERE id_time_load = ' || v_tl || v_where_clause;
                    BEGIN
                        EXECUTE IMMEDIATE v_sql INTO v_compare_count;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_compare_count := 0;
                    END;
                ELSE
                    -- Standard comparison (no dt_load_process grouping)
                    -- Get count from main table
                    v_sql := 'SELECT COUNT(*) FROM ' || v_owner || '.' || v_table_name || ' ' ||
                            'WHERE id_time_load = ' || v_tl || v_where_clause;
                    EXECUTE IMMEDIATE v_sql INTO v_count_value;
                    
                    -- Get count from comparison table
                    v_sql := 'SELECT COUNT(*) FROM ' || v_owner || '.' || v_compare_to_table || v_source_link || ' ' ||
                            'WHERE id_time_load = ' || v_tl || v_where_clause;
                    
                    BEGIN
                        EXECUTE IMMEDIATE v_sql INTO v_compare_count;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            v_compare_count := 0;
                        WHEN OTHERS THEN
                            v_compare_count := 0;
                            DBMS_OUTPUT.PUT_LINE('WARNING: Error getting count from ' || v_owner || '.' || 
                                               v_compare_to_table || v_source_link || ': ' || SQLERRM);
                    END;
                END IF;
                
                -- Compare counts
                IF v_count_value = v_compare_count THEN
                    v_match_status := 'MATCH';
                    v_status := 'PASS';
                    v_severity := 'LOW';
                    v_message := 'Table matches ' || v_compare_to_table || 
                                CASE WHEN v_compare_where_clause IS NOT NULL THEN ' (with filter)' ELSE '' END ||
                                ' (Count: ' || v_count_value || ')';
                ELSE
                    v_match_status := 'NO_MATCH';
                    v_status := 'ERROR';
                    v_severity := 'HIGH';
                    v_message := 'Table does not match ' || v_compare_to_table || 
                                CASE WHEN v_compare_where_clause IS NOT NULL THEN ' (with filter)' ELSE '' END ||
                                ' (Count: ' || v_count_value || ' vs ' || v_compare_count || 
                                ', Diff: ' || ABS(v_count_value - v_compare_count) || ')';
                END IF;
                
                -- Store result
                INSERT_VALIDATION_RESULT(
                    p_owner => v_owner,
                    p_table_name => v_table_name,
                    p_validation_type => 'TABLE_COMPARE',
                    p_time_load => v_tl,
                    p_count_value => v_count_value,
                    p_status => v_status,
                    p_severity => v_severity,
                    p_message => v_message,
                    p_email_address => v_email_address,
                    p_compare_table => v_compare_to_table,
                    p_compare_count => v_compare_count,
                    p_match_status => v_match_status
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR comparing tables at time load ' || v_tl || 
                                       ' for ' || v_owner || '.' || v_table_name || 
                                       ' vs ' || v_compare_to_table || ': ' || SQLERRM);
            END;
        END LOOP;
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in EXECUTE_TABLE_COMPARISON: ' || SQLERRM);
            RAISE;
    END EXECUTE_TABLE_COMPARISON;
    
    PROCEDURE EXECUTE_DISTINCT_COUNT_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    ) IS
        v_owner VARCHAR2(30);
        v_table_name VARCHAR2(128);
        v_distinct_column VARCHAR2(128);
        v_distinct_condition VARCHAR2(4000);
        v_threshold_pct NUMBER;
        v_email_address VARCHAR2(255);
        v_count_value NUMBER;
        v_prior_count NUMBER;
        v_pct_change NUMBER;
        v_status VARCHAR2(20);
        v_severity VARCHAR2(10);
        v_sql VARCHAR2(32767);
        v_distinct_expr VARCHAR2(4000);
    BEGIN
        -- Get configuration details
        BEGIN
            SELECT owner, table_name, DISTINCT_COUNT_COLUMN, DISTINCT_COUNT_CONDITION, 
                   THRESHOLD_PCT, EMAIL_ADDRESS
            INTO v_owner, v_table_name, v_distinct_column, v_distinct_condition,
                 v_threshold_pct, v_email_address
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE CONFIG_ID = p_config_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: Configuration ID ' || p_config_id || ' not found');
                RETURN;
        END;
        
        -- Check if distinct column is specified
        IF v_distinct_column IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: DISTINCT_COUNT_COLUMN not specified for ' || 
                               v_owner || '.' || v_table_name);
            RETURN;
        END IF;
        
        -- Check if table exists
        IF NOT VALIDATE_TABLE_EXISTS(v_owner, v_table_name) THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Table ' || v_owner || '.' || v_table_name || ' does not exist');
            RETURN;
        END IF;
        
        -- Build distinct count expression
        IF v_distinct_condition IS NOT NULL AND LENGTH(TRIM(v_distinct_condition)) > 0 THEN
            -- Use CASE WHEN condition (e.g., "case when dt_exit is not null then id_pp_person end")
            v_distinct_expr := 'COUNT(DISTINCT ' || v_distinct_condition || ')';
        ELSE
            -- Simple distinct count
            v_distinct_expr := 'COUNT(DISTINCT ' || v_distinct_column || ')';
        END IF;
        
        -- Process each time load
        FOR v_tl IN p_time_load_start..p_time_load_end LOOP
            BEGIN
                -- Get distinct count for this time load
                v_sql := 'SELECT ' || v_distinct_expr || ' FROM ' || v_owner || '.' || v_table_name || ' ' ||
                        'WHERE id_time_load = ' || v_tl;
                
                EXECUTE IMMEDIATE v_sql INTO v_count_value;
                
                -- Get prior count
                IF v_tl > p_time_load_start THEN
                    v_sql := 'SELECT ' || v_distinct_expr || ' FROM ' || v_owner || '.' || v_table_name || ' ' ||
                            'WHERE id_time_load = ' || (v_tl - 1);
                    BEGIN
                        EXECUTE IMMEDIATE v_sql INTO v_prior_count;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_prior_count := NULL;
                    END;
                ELSE
                    v_prior_count := NULL;
                END IF;
                
                -- Calculate percentage change
                v_pct_change := CALCULATE_PERCENTAGE_CHANGE(v_count_value, v_prior_count);
                
                -- Assign status and severity
                ASSIGN_STATUS_SEVERITY(v_pct_change, v_threshold_pct, v_status, v_severity);
                
                -- Store result
                INSERT_VALIDATION_RESULT(
                    p_owner => v_owner,
                    p_table_name => v_table_name,
                    p_validation_type => 'DISTINCT_COUNT',
                    p_time_load => v_tl,
                    p_column_name => v_distinct_column,
                    p_count_value => v_count_value,
                    p_prior_count => v_prior_count,
                    p_pct_change => v_pct_change,
                    p_status => v_status,
                    p_severity => v_severity,
                    p_message => CASE 
                        WHEN v_pct_change IS NOT NULL THEN 
                            'Distinct count (' || v_distinct_column || '): ' || v_count_value || 
                            ', Prior: ' || v_prior_count || 
                            ', Change: ' || TO_CHAR(v_pct_change, '999.99') || '%'
                        ELSE 'Distinct count (' || v_distinct_column || '): ' || v_count_value
                    END,
                    p_email_address => v_email_address
                );
                
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR processing distinct count at time load ' || v_tl || 
                                       ' for ' || v_owner || '.' || v_table_name || ': ' || SQLERRM);
            END;
        END LOOP;
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in EXECUTE_DISTINCT_COUNT_VALIDATION: ' || SQLERRM);
            RAISE;
    END EXECUTE_DISTINCT_COUNT_VALIDATION;
    
    PROCEDURE EXECUTE_DATA_VALIDATION(
        p_config_id       IN NUMBER,
        p_time_load_start IN NUMBER,
        p_time_load_end   IN NUMBER
    ) IS
        v_owner VARCHAR2(30);
        v_table_name VARCHAR2(128);
        v_data_validation_query CLOB;
        v_threshold_pct NUMBER;
        v_email_address VARCHAR2(255);
        v_row_count NUMBER;
        v_status VARCHAR2(20);
        v_severity VARCHAR2(10);
        v_message VARCHAR2(4000);
        v_cursor_id INTEGER;
        v_rows_fetched NUMBER;
        v_query_text VARCHAR2(32767);
    BEGIN
        -- Get configuration details
        BEGIN
            SELECT owner, table_name, DATA_VALIDATION_QUERY, THRESHOLD_PCT, EMAIL_ADDRESS
            INTO v_owner, v_table_name, v_data_validation_query, v_threshold_pct, v_email_address
            FROM TBL_MDC_VALIDATION_CONFIG
            WHERE CONFIG_ID = p_config_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: Configuration ID ' || p_config_id || ' not found');
                RETURN;
        END;
        
        -- Check if data validation query is specified
        IF v_data_validation_query IS NULL OR LENGTH(TRIM(v_data_validation_query)) = 0 THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: DATA_VALIDATION_QUERY not specified for ' || 
                               v_owner || '.' || v_table_name);
            RETURN;
        END IF;
        
        -- Convert CLOB to VARCHAR2 for DBMS_SQL (handle large queries)
        -- Note: For queries > 32KB, we'll need to use DBMS_SQL.PARSE with CLOB directly
        BEGIN
            IF DBMS_LOB.GETLENGTH(v_data_validation_query) <= 32767 THEN
                v_query_text := DBMS_LOB.SUBSTR(v_data_validation_query, 32767, 1);
            ELSE
                -- For very large queries, use first 32KB (may need chunking for full support)
                v_query_text := DBMS_LOB.SUBSTR(v_data_validation_query, 32767, 1);
                DBMS_OUTPUT.PUT_LINE('WARNING: Query truncated to 32KB. Full query length: ' || 
                                   DBMS_LOB.GETLENGTH(v_data_validation_query));
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('ERROR converting CLOB to VARCHAR2: ' || SQLERRM);
                RETURN;
        END;
        
        -- Execute the data validation query
        -- The query should return 0 rows if data matches correctly
        BEGIN
            v_cursor_id := DBMS_SQL.OPEN_CURSOR;
            
            -- Parse query (handle both VARCHAR2 and CLOB)
            IF LENGTH(v_query_text) = DBMS_LOB.GETLENGTH(v_data_validation_query) THEN
                -- Use VARCHAR2 version
                DBMS_SQL.PARSE(v_cursor_id, v_query_text, DBMS_SQL.NATIVE);
            ELSE
                -- For CLOB, we'll need to use a different approach
                -- For now, use the truncated version with a warning
                DBMS_SQL.PARSE(v_cursor_id, v_query_text, DBMS_SQL.NATIVE);
            END IF;
            
            -- Execute query
            v_rows_fetched := DBMS_SQL.EXECUTE(v_cursor_id);
            
            -- Count total rows returned
            v_row_count := 0;
            LOOP
                IF DBMS_SQL.FETCH_ROWS(v_cursor_id) = 0 THEN
                    EXIT;
                END IF;
                v_row_count := v_row_count + 1;
            END LOOP;
            
            DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
            
            -- Determine status based on row count
            -- 0 rows = data matches (PASS)
            -- >0 rows = data mismatch (ERROR)
            IF v_row_count = 0 THEN
                v_status := 'PASS';
                v_severity := 'LOW';
                v_message := 'Data validation passed - no mismatches found (0 rows returned)';
            ELSE
                v_status := 'ERROR';
                v_severity := 'HIGH';
                v_message := 'Data validation failed - ' || v_row_count || 
                           ' row(s) of mismatched data found. Query should return 0 rows if data matches.';
            END IF;
            
            -- Store result (use current time load or NULL if not applicable)
            INSERT_VALIDATION_RESULT(
                p_owner => v_owner,
                p_table_name => v_table_name,
                p_validation_type => 'DATA_VALIDATION',
                p_time_load => p_time_load_end, -- Use end time load as reference
                p_count_value => v_row_count,
                p_status => v_status,
                p_severity => v_severity,
                p_message => v_message,
                p_email_address => v_email_address
            );
            
        EXCEPTION
            WHEN OTHERS THEN
                IF v_cursor_id IS NOT NULL AND DBMS_SQL.IS_OPEN(v_cursor_id) THEN
                    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
                END IF;
                
                v_status := 'ERROR';
                v_severity := 'HIGH';
                v_message := 'Error executing data validation query: ' || SQLERRM;
                
                INSERT_VALIDATION_RESULT(
                    p_owner => v_owner,
                    p_table_name => v_table_name,
                    p_validation_type => 'DATA_VALIDATION',
                    p_time_load => p_time_load_end,
                    p_count_value => 0,
                    p_status => v_status,
                    p_severity => v_severity,
                    p_message => v_message,
                    p_email_address => v_email_address
                );
                
                DBMS_OUTPUT.PUT_LINE('ERROR executing data validation for ' || 
                                   v_owner || '.' || v_table_name || ': ' || SQLERRM);
                DBMS_OUTPUT.PUT_LINE('Query (first 500 chars): ' || SUBSTR(v_query_text, 1, 500));
        END;
        
        COMMIT;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in EXECUTE_DATA_VALIDATION: ' || SQLERRM);
            RAISE;
    END EXECUTE_DATA_VALIDATION;

    -- ========================================================================
    -- Existing mdc_counts.sql Validations
    -- ========================================================================
    
    PROCEDURE CHECK_TABLES_WITHOUT_STATS IS
        CURSOR c_tables_with_no_stats IS
            SELECT owner, table_name
            FROM all_tables
            WHERE owner IN ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
              AND last_analyzed IS NULL
              AND table_name NOT LIKE 'ARCH%'
              AND table_name NOT LIKE 'GTT%'
              AND table_name NOT LIKE 'EXT%'
              AND table_name NOT LIKE 'QUEST%'
              AND table_name NOT LIKE 'WK%'
              AND table_name NOT LIKE 'CSL_JC%'
              AND table_name NOT LIKE 'CSL_DAILY_KIN%'
              AND table_name NOT LIKE 'CSL_DAILY_SFI%'
              AND table_name NOT LIKE 'FN_SFI%'
              AND table_name NOT LIKE 'FT_CHILD%'
              AND table_name NOT LIKE 'FT_PERP%'
              AND table_name NOT LIKE 'DASHBOARD%'
              AND table_name NOT LIKE 'ACCESSHR%'
              AND table_name != 'TIME_DIM'
              AND table_name != 'PP_COURT_ORD_FACT'
              AND table_name != 'PP_POST_ADOPT_HIST_FACT'
              AND table_name != 'SVC_OUTMAT_HIST_DIM'
              AND table_name != 'CYD_SRVC_HIST_FACT'
              AND table_name != 'FAM_SERVICE_PLAN_DIM'
              AND table_name != 'INR_TR_APPRVD_CHILD_FACT'
              AND table_name != 'AUD_INV_PRINC_DIM'
            ORDER BY 1, 2;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('----------- List of tables that are not analyzed.');
        
        FOR rec IN c_tables_with_no_stats
        LOOP
            DBMS_OUTPUT.PUT_LINE(rec.owner || '.' || rec.table_name || ' <----<<< Error?');
            
            -- Store result
            INSERT_VALIDATION_RESULT(
                p_owner => rec.owner,
                p_table_name => rec.table_name,
                p_validation_type => 'TABLES_NO_STATS',
                p_time_load => NULL,
                p_count_value => 0,
                p_status => 'WARNING',
                p_severity => 'MEDIUM',
                p_message => 'Table has not been analyzed (last_analyzed IS NULL)',
                p_email_address => NULL
            );
        END LOOP;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CHECK_TABLES_WITHOUT_STATS: ' || SQLERRM);
            RAISE;
    END CHECK_TABLES_WITHOUT_STATS;
    
    PROCEDURE CHECK_INDEXES_WITHOUT_STATS IS
        CURSOR c_indexes_with_no_stats IS
            SELECT owner, index_name, table_name
            FROM all_indexes
            WHERE owner IN ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
              AND last_analyzed IS NULL
              AND table_name NOT LIKE 'ARCH%'
              AND table_name NOT LIKE 'GTT%'
              AND table_name NOT LIKE 'EXT%'
              AND table_name NOT LIKE 'QUEST%'
              AND table_name NOT LIKE 'WK%'
              AND table_name NOT LIKE 'CSL_JC%'
              AND table_name NOT LIKE 'CSL_DAILY_KIN%'
              AND table_name NOT LIKE 'CSL_DAILY_SFI%'
              AND table_name NOT LIKE 'FN_SFI%'
              AND table_name NOT LIKE 'FT_CHILD%'
              AND table_name NOT LIKE 'FT_PERP%'
              AND table_name NOT LIKE 'DASHBOARD%'
              AND table_name NOT LIKE 'ACCESSHR%'
              AND table_name != 'TIME_DIM'
              AND table_name != 'PP_COURT_ORD_FACT'
              AND table_name != 'PP_POST_ADOPT_HIST_FACT'
              AND table_name != 'SVC_OUTMAT_HIST_DIM'
              AND table_name != 'CYD_SRVC_HIST_FACT'
              AND table_name != 'FAM_SERVICE_PLAN_DIM'
              AND table_name != 'INR_TR_APPRVD_CHILD_FACT'
              AND table_name != 'AUD_INV_PRINC_DIM'
            ORDER BY 1, 2;
    BEGIN
        DBMS_OUTPUT.PUT_LINE(chr(10) || '----------- List of indexes that are not analyzed.');
        
        FOR rec IN c_indexes_with_no_stats
        LOOP
            DBMS_OUTPUT.PUT_LINE(rec.owner || ' : ' || rec.index_name || ' <----<<< Error?');
            
            -- Store result
            INSERT_VALIDATION_RESULT(
                p_owner => rec.owner,
                p_table_name => rec.table_name,
                p_validation_type => 'INDEXES_NO_STATS',
                p_time_load => NULL,
                p_count_value => 0,
                p_status => 'WARNING',
                p_severity => 'MEDIUM',
                p_message => 'Index ' || rec.index_name || ' has not been analyzed (last_analyzed IS NULL)',
                p_email_address => NULL
            );
        END LOOP;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CHECK_INDEXES_WITHOUT_STATS: ' || SQLERRM);
            RAISE;
    END CHECK_INDEXES_WITHOUT_STATS;
    
    PROCEDURE CHECK_INVALID_OBJECTS IS
        CURSOR c_invalid_objects IS
            SELECT owner, object_name, object_type
            FROM all_objects
            WHERE owner IN ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
              AND status = 'INVALID'
              AND object_name NOT LIKE 'ARCH%'
              AND object_name NOT LIKE 'GTT%'
              AND object_name NOT LIKE 'EXT%'
              AND object_name NOT LIKE 'QUEST%'
              AND object_name NOT LIKE '%TEST'
              AND object_name NOT LIKE 'TMP%'
              AND object_name NOT LIKE 'BIN$%'
              AND object_name NOT LIKE 'WK%'
              AND object_name NOT LIKE 'CSL_JC%'
              AND object_name NOT LIKE 'CSL_DAILY_KIN%'
              AND object_name NOT LIKE 'CSL_DAILY_SFI%'
              AND object_name NOT LIKE 'FN_SFI%'
              AND object_name NOT LIKE 'FT_CHILD%'
              AND object_name NOT LIKE 'FT_PERP%'
              AND object_name NOT LIKE '%_OLD'
              AND object_name NOT LIKE '%_OLD2'
              AND object_name NOT LIKE '%_LOAD_09'
              AND object_name NOT LIKE '%_LOAD_10'
              AND object_name NOT LIKE '%_201404'
              AND object_name NOT LIKE '%_20140616'
              AND object_name NOT LIKE 'DASHBOARD%'
              AND object_name NOT LIKE 'ACCESSHR%'
              AND object_name != 'PLAN_TABLE'
              AND object_name != 'TIME_DIM'
              AND object_name != 'PP_COURT_ORD_FACT'
              AND object_name != 'PP_POST_ADOPT_HIST_FACT'
              AND object_name != 'SVC_OUTMAT_HIST_DIM'
              AND object_name != 'CYD_SRVC_HIST_FACT'
              AND object_name != 'FAM_SERVICE_PLAN_DIM'
              AND object_name != 'INR_TR_APPRVD_CHILD_FACT'
              AND object_name != 'AUD_INV_PRINC_DIM'
            ORDER BY 3, 1, 2;
    BEGIN
        DBMS_OUTPUT.PUT_LINE(chr(10) || '----------- List of INVALID (active) objects.' || chr(10));
        
        FOR rec IN c_invalid_objects
        LOOP
            DBMS_OUTPUT.PUT_LINE(rec.object_type || ' : ' || rec.owner || '.' || rec.object_name || ' <----<<< Error?');
            
            -- Store result
            INSERT_VALIDATION_RESULT(
                p_owner => rec.owner,
                p_table_name => rec.object_name,
                p_validation_type => 'INVALID_OBJECT',
                p_time_load => NULL,
                p_count_value => 0,
                p_status => 'ERROR',
                p_severity => 'HIGH',
                p_message => 'Object type: ' || rec.object_type || ' is INVALID',
                p_email_address => NULL
            );
        END LOOP;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CHECK_INVALID_OBJECTS: ' || SQLERRM);
            RAISE;
    END CHECK_INVALID_OBJECTS;
    
    PROCEDURE CHECK_ARCH_TABLE_COMPARISONS IS
        CURSOR c_archived_sum_tables IS
            SELECT owner, table_name
            FROM all_tables
            WHERE owner IN ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
              AND (table_name LIKE '%SUM'
                   OR table_name LIKE '%SUM_SECURE'
                   OR table_name LIKE '%FACT'
                   OR table_name LIKE '%DIM'
                   OR table_name = 'DASHBOARD')
              AND table_name NOT LIKE 'ARCH%'
              AND table_name NOT LIKE 'GTT%'
              AND table_name NOT LIKE 'EXT%'
              AND table_name NOT LIKE 'QUEST%'
              AND table_name NOT LIKE 'TMP%'
              AND table_name NOT LIKE 'POP%SUM'
              AND table_name NOT LIKE 'FT%SUM'
              AND table_name NOT LIKE 'WK%'
              AND table_name NOT LIKE 'CSL_JC%'
              AND table_name NOT LIKE 'CSL_DAILY_KIN%'
              AND table_name NOT LIKE 'CSL_DAILY_SFI%'
              AND table_name NOT LIKE 'FN_SFI%'
              AND table_name NOT LIKE 'FT_CHILD%'
              AND table_name NOT LIKE 'FT_PERP%'
              AND table_name NOT LIKE 'DASHBOARD%'
              AND table_name NOT LIKE 'ACCESSHR%'
              AND table_name != 'TIME_DIM'
              AND table_name != 'PP_COURT_ORD_FACT'
              AND table_name != 'PP_POST_ADOPT_HIST_FACT'
              AND table_name != 'SVC_OUTMAT_HIST_DIM'
              AND table_name != 'CYD_SRVC_HIST_FACT'
              AND table_name != 'FAM_SERVICE_PLAN_DIM'
              AND table_name != 'INR_TR_APPRVD_CHILD_FACT'
              AND table_name != 'AUD_INV_PRINC_DIM'
              AND (owner, table_name) IN (
                  SELECT owner, SUBSTR(table_name, 6, LENGTH(table_name))
                  FROM all_tables
                  WHERE owner IN ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
                    AND table_name LIKE 'ARCH%'
              )
            ORDER BY 1, 2;
        
        v_base_tl NUMBER;
        v_current_tl NUMBER;
        v_sum_total NUMBER;
        v_arch_total NUMBER;
        v_month VARCHAR2(20);
        v_database_name VARCHAR2(50);
    BEGIN
        DBMS_OUTPUT.PUT_LINE(chr(10) || '----------- Compare table record counts to ARCH table record counts.');
        
        v_current_tl := GET_CURRENT_TIME_LOAD;
        v_base_tl := CALCULATE_BASE_TIME_LOAD(v_current_tl);
        
        -- Get month and database for special cases
        v_month := TRIM(TO_CHAR(SYSDATE, 'Month'));
        v_database_name := GET_DATABASE_NAME;
        
        FOR rec IN c_archived_sum_tables
        LOOP
            -- Adjust BaseTL for special cases
            IF (v_month = 'November' AND UPPER(v_database_name) LIKE '%WARE%') OR
               (v_month = 'October' AND UPPER(v_database_name) LIKE '%QAWH%') THEN
                v_base_tl := v_current_tl - 13;
            END IF;
            
            IF rec.table_name LIKE '%HIST_DIM' THEN
                v_base_tl := v_current_tl - 2;
            ELSIF rec.table_name = 'NYTD_FACT' OR rec.table_name LIKE '%HIST%' THEN
                v_base_tl := v_current_tl;
            END IF;
            
            DBMS_OUTPUT.PUT_LINE(chr(10) || rec.owner || '.' || rec.table_name);
            
            FOR i IN v_base_tl..v_current_tl
            LOOP
                BEGIN
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || rec.owner || '.' || rec.table_name || 
                                     ' WHERE id_time_load = ' || TO_CHAR(i)
                        INTO v_sum_total;
                    
                    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || rec.owner || '.ARCH_' || rec.table_name || 
                                     ' WHERE id_time_load = ' || TO_CHAR(i)
                        INTO v_arch_total;
                    
                    IF v_sum_total = v_arch_total AND v_sum_total > 0 THEN
                        DBMS_OUTPUT.PUT_LINE(TO_CHAR(i) || ' TABLE: ' || TO_CHAR(v_sum_total) || 
                                            ' = ARCH:' || TO_CHAR(v_arch_total));
                        
                        -- Store match result
                        INSERT_VALIDATION_RESULT(
                            p_owner => rec.owner,
                            p_table_name => rec.table_name,
                            p_validation_type => 'ARCH_COMPARE',
                            p_time_load => i,
                            p_count_value => v_sum_total,
                            p_status => 'PASS',
                            p_severity => 'LOW',
                            p_message => 'Table matches ARCH table',
                            p_compare_table => 'ARCH_' || rec.table_name,
                            p_compare_count => v_arch_total,
                            p_match_status => 'MATCH',
                            p_email_address => NULL
                        );
                    ELSIF v_sum_total = 0 AND rec.table_name NOT LIKE '%HIST%' THEN
                        DBMS_OUTPUT.PUT_LINE(TO_CHAR(i) || ' TABLE: ' || TO_CHAR(v_sum_total) || 
                                            ' != ARCH:' || TO_CHAR(v_arch_total) || ' <----<<< Error 0 Records');
                        
                        INSERT_VALIDATION_RESULT(
                            p_owner => rec.owner,
                            p_table_name => rec.table_name,
                            p_validation_type => 'ARCH_COMPARE',
                            p_time_load => i,
                            p_count_value => v_sum_total,
                            p_status => 'ERROR',
                            p_severity => 'HIGH',
                            p_message => 'Table has 0 records',
                            p_compare_table => 'ARCH_' || rec.table_name,
                            p_compare_count => v_arch_total,
                            p_match_status => 'NO_MATCH',
                            p_email_address => NULL
                        );
                    ELSE
                        IF (MOD(i, 12) = 0 AND rec.table_name NOT LIKE '%NYTD%') OR i = v_current_tl THEN
                            DBMS_OUTPUT.PUT_LINE(TO_CHAR(i) || ' TABLE: ' || TO_CHAR(v_sum_total) || 
                                                ' != ARCH:' || TO_CHAR(v_arch_total) || ' <----<<< Error?');
                            
                            INSERT_VALIDATION_RESULT(
                                p_owner => rec.owner,
                                p_table_name => rec.table_name,
                                p_validation_type => 'ARCH_COMPARE',
                                p_time_load => i,
                                p_count_value => v_sum_total,
                                p_status => 'WARNING',
                                p_severity => 'MEDIUM',
                                p_message => 'Table count does not match ARCH table',
                                p_compare_table => 'ARCH_' || rec.table_name,
                                p_compare_count => v_arch_total,
                                p_match_status => 'NO_MATCH',
                                p_email_address => NULL
                            );
                        END IF;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('ERROR comparing ' || rec.owner || '.' || rec.table_name || 
                                           ' at time load ' || TO_CHAR(i) || ': ' || SQLERRM);
                END;
            END LOOP;
        END LOOP;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CHECK_ARCH_TABLE_COMPARISONS: ' || SQLERRM);
            RAISE;
    END CHECK_ARCH_TABLE_COMPARISONS;
    
    PROCEDURE CHECK_DATA_VOLUME_CONSISTENCY IS
        CURSOR c_sum_tables IS
            SELECT owner, table_name
            FROM all_tables
            WHERE owner IN ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
              AND (table_name LIKE '%SUM'
                   OR table_name LIKE '%SUM_SECURE'
                   OR table_name LIKE '%FACT'
                   OR table_name LIKE 'SYMM%'
                   OR table_name LIKE 'TELSTR%'
                   OR table_name LIKE '%DIM'
                   OR table_name = 'DASHBOARD')
              AND table_name IN (
                  SELECT table_name 
                  FROM all_tab_columns 
                  WHERE column_name = 'ID_TIME_LOAD'
              )
              AND table_name NOT LIKE 'ARCH%'
              AND table_name NOT LIKE 'GTT%'
              AND table_name NOT LIKE 'EXT%'
              AND table_name NOT LIKE 'QUEST%'
              AND table_name NOT LIKE 'TMP%'
              AND table_name NOT LIKE 'POP%SUM'
              AND table_name NOT LIKE 'FT%SUM'
              AND table_name NOT LIKE '%HIST_SUM'
              AND table_name NOT LIKE 'WK%'
              AND table_name NOT LIKE 'DLY%'
              AND table_name NOT LIKE 'CSL_JC%'
              AND table_name NOT LIKE 'CSL_DAILY_KIN%'
              AND table_name NOT LIKE 'CSL_DAILY_SFI%'
              AND table_name NOT LIKE 'FN_SFI%'
              AND table_name NOT LIKE 'FT_CHILD%'
              AND table_name NOT LIKE 'FT_PERP%'
              AND table_name NOT LIKE 'DASHBOARD%'
              AND table_name NOT LIKE 'ACCESSHR%'
              AND table_name != 'TIME_DIM'
              AND table_name != 'PP_COURT_ORD_FACT'
              AND table_name != 'PP_POST_ADOPT_HIST_FACT'
              AND table_name != 'SVC_OUTMAT_HIST_DIM'
              AND table_name != 'CYD_SRVC_HIST_FACT'
              AND table_name != 'FAM_SERVICE_PLAN_DIM'
              AND table_name != 'INR_TR_APPRVD_CHILD_FACT'
              AND table_name != 'AUD_INV_PRINC_DIM'
            ORDER BY 1, 2;
        
        v_base_tl NUMBER;
        v_current_tl NUMBER;
        v_sum_total NUMBER;
        v_is_consistent BOOLEAN;
        v_month VARCHAR2(20);
        v_database_name VARCHAR2(50);
    BEGIN
        DBMS_OUTPUT.PUT_LINE(chr(10) || '----------- Check tables for consistent data volume.');
        DBMS_OUTPUT.PUT_LINE('----------- NOTE: HIST tables are NOT checked in this section.' || chr(10));
        
        v_current_tl := GET_CURRENT_TIME_LOAD;
        v_base_tl := CALCULATE_BASE_TIME_LOAD(v_current_tl);
        
        -- Get month and database for special cases
        v_month := TRIM(TO_CHAR(SYSDATE, 'Month'));
        v_database_name := GET_DATABASE_NAME;
        
        IF (v_month = 'November' AND UPPER(v_database_name) LIKE '%WARE%') OR
           (v_month = 'October' AND UPPER(v_database_name) LIKE '%QAWH%') THEN
            v_base_tl := v_current_tl - 13;
        END IF;
        
        FOR rec IN c_sum_tables
        LOOP
            IF rec.table_name NOT LIKE '%HIST%' THEN
                DBMS_OUTPUT.PUT_LINE(rec.owner || '.' || rec.table_name);
                
                FOR i IN v_base_tl..v_current_tl
                LOOP
                    BEGIN
                        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || rec.owner || '.' || rec.table_name || 
                                         ' WHERE id_time_load = ' || TO_CHAR(i)
                            INTO v_sum_total;
                        DBMS_OUTPUT.PUT_LINE(TO_CHAR(i) || ' : ' || TO_CHAR(v_sum_total));
                    EXCEPTION
                        WHEN OTHERS THEN
                            DBMS_OUTPUT.PUT_LINE('ERROR getting count for ' || rec.owner || '.' || rec.table_name || 
                                               ' at time load ' || TO_CHAR(i) || ': ' || SQLERRM);
                    END;
                END LOOP;
                
                -- Check consistency using MRS.ConsistentDataVolume
                BEGIN
                    v_is_consistent := MRS.ConsistentDataVolume(rec.owner, rec.table_name, v_current_tl, 0.10);
                    
                    IF v_is_consistent THEN
                        DBMS_OUTPUT.PUT_LINE('Data volume is consistent for timeloads ' || TO_CHAR(v_current_tl - 2) ||
                                           ' - ' || TO_CHAR(v_current_tl) || '.' || chr(10));
                        
                        INSERT_VALIDATION_RESULT(
                            p_owner => rec.owner,
                            p_table_name => rec.table_name,
                            p_validation_type => 'DATA_VOLUME_CONSISTENCY',
                            p_time_load => v_current_tl,
                            p_count_value => 0,
                            p_status => 'PASS',
                            p_severity => 'LOW',
                            p_message => 'Data volume is consistent',
                            p_email_address => NULL
                        );
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('Data volume is NOT consistent for timeloads ' || TO_CHAR(v_current_tl - 2) ||
                                           ' - ' || TO_CHAR(v_current_tl) || '. <----<<< Error?' || chr(10));
                        
                        INSERT_VALIDATION_RESULT(
                            p_owner => rec.owner,
                            p_table_name => rec.table_name,
                            p_validation_type => 'DATA_VOLUME_CONSISTENCY',
                            p_time_load => v_current_tl,
                            p_count_value => 0,
                            p_status => 'ERROR',
                            p_severity => 'HIGH',
                            p_message => 'Data volume is NOT consistent',
                            p_email_address => NULL
                        );
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('ERROR checking consistency for ' || rec.owner || '.' || rec.table_name || 
                                           ': ' || SQLERRM);
                END;
            END IF;
        END LOOP;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CHECK_DATA_VOLUME_CONSISTENCY: ' || SQLERRM);
            RAISE;
    END CHECK_DATA_VOLUME_CONSISTENCY;
    
    PROCEDURE GET_CURRENT_TIME_LOAD_COUNTS IS
        CURSOR c_time_load_tables IS
            SELECT owner, table_name 
            FROM all_tables 
            WHERE owner IN ('CAPS', 'CCL', 'PEI', 'SWI')
              AND table_name IN (
                  SELECT table_name 
                  FROM all_tab_columns 
                  WHERE column_name = 'ID_TIME_LOAD'
              )
              AND table_name NOT LIKE 'ARCH%'
              AND table_name NOT LIKE 'TMP%'
              AND table_name NOT LIKE 'TIME%'
              AND table_name NOT LIKE 'PASS%'
              AND table_name NOT LIKE 'SUM_LOG%'
              AND table_name NOT LIKE '%MASTER'
              AND table_name NOT LIKE '%3560'
              AND table_name NOT LIKE '%64'
              AND table_name NOT LIKE 'WK%'
              AND table_name NOT LIKE 'DLY%'
              AND table_name NOT LIKE 'CSL_JC%'
              AND table_name NOT LIKE 'CSL_DAILY_KIN%'
              AND table_name NOT LIKE 'CSL_DAILY_SFI%'
              AND table_name NOT LIKE 'FN_SFI%'
              AND table_name NOT LIKE 'FT_CHILD%'
              AND table_name NOT LIKE 'FT_PERP%'
              AND table_name NOT LIKE '%FD'
              AND table_name NOT LIKE '%RET'
              AND table_name NOT LIKE 'DASHBOARD%'
              AND table_name NOT LIKE 'ACCESSHR%'
              AND table_name != 'ACCESSHR_EVAL_CMPL_FACT'
              AND table_name != 'PP_COURT_ORD_FACT'
              AND table_name != 'PP_POST_ADOPT_HIST_FACT'
              AND table_name != 'SVC_OUTMAT_HIST_DIM'
              AND table_name != 'CYD_SRVC_HIST_FACT'
              AND table_name != 'FAM_SERVICE_PLAN_DIM'
              AND table_name != 'INR_TR_APPRVD_CHILD_FACT'
              AND table_name != 'AUD_INV_PRINC_DIM'
            ORDER BY 1, 2;
        
        v_current_tl NUMBER;
        v_sum_total NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE(chr(10) || '----------- List record count for the current time load in every tables.' || chr(10));
        
        v_current_tl := GET_CURRENT_TIME_LOAD;
        
        FOR rec IN c_time_load_tables
        LOOP
            BEGIN
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || rec.owner || '.' || rec.table_name || 
                                 ' WHERE id_time_load = ' || TO_CHAR(v_current_tl)
                    INTO v_sum_total;
                
                DBMS_OUTPUT.PUT_LINE(rec.owner || '.' || rec.table_name);
                
                IF v_sum_total > 0 THEN
                    DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_current_tl) || ' : ' || TO_CHAR(v_sum_total) || chr(10));
                    
                    INSERT_VALIDATION_RESULT(
                        p_owner => rec.owner,
                        p_table_name => rec.table_name,
                        p_validation_type => 'CURRENT_TL_COUNT',
                        p_time_load => v_current_tl,
                        p_count_value => v_sum_total,
                        p_status => 'PASS',
                        p_severity => 'LOW',
                        p_message => 'Current time load count',
                        p_email_address => NULL
                    );
                ELSE
                    DBMS_OUTPUT.PUT_LINE(TO_CHAR(v_current_tl) || ' : ' || TO_CHAR(v_sum_total) || 
                                        ' <----<<< Error 0 Records' || chr(10));
                    
                    INSERT_VALIDATION_RESULT(
                        p_owner => rec.owner,
                        p_table_name => rec.table_name,
                        p_validation_type => 'CURRENT_TL_COUNT',
                        p_time_load => v_current_tl,
                        p_count_value => v_sum_total,
                        p_status => 'ERROR',
                        p_severity => 'HIGH',
                        p_message => 'Current time load has 0 records',
                        p_email_address => NULL
                    );
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR getting count for ' || rec.owner || '.' || rec.table_name || 
                                       ': ' || SQLERRM);
            END;
        END LOOP;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in GET_CURRENT_TIME_LOAD_COUNTS: ' || SQLERRM);
            RAISE;
    END GET_CURRENT_TIME_LOAD_COUNTS;
    
    PROCEDURE CHECK_CCL_FACILITY_COMPARISON IS
        CURSOR c_ccl_diff IS
            SELECT id_time_load, COUNT(*) diff
            FROM ccl.facility_fact
            WHERE id_time_load >= 100261
            GROUP BY id_time_load
            MINUS
            SELECT id_time_load, COUNT(*) diff
            FROM ccl.ccl_facility_sum
            WHERE id_time_load >= 100261
            GROUP BY id_time_load
            ORDER BY 1;
        
        v_fact_cntr NUMBER := 0;
        v_sum_cntr NUMBER := 0;
        v_diff_cntr NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE(chr(10) || '----------- Compare CCL.FACILITY_FACT and CCL.CCL_FACILITY_SUM record counts.' || chr(10));
        
        BEGIN
            FOR rec IN c_ccl_diff
            LOOP
                SELECT COUNT(*)
                INTO v_fact_cntr
                FROM ccl.facility_fact
                WHERE id_time_load = rec.id_time_load;
                
                SELECT COUNT(*)
                INTO v_sum_cntr
                FROM ccl.ccl_facility_sum
                WHERE id_time_load = rec.id_time_load;
                
                DBMS_OUTPUT.PUT_LINE(TO_CHAR(rec.id_time_load) || ' : FACT=' || TO_CHAR(v_fact_cntr) || 
                                   ' : SUM=' || TO_CHAR(v_sum_cntr) || ' : Diff=' || 
                                   TO_CHAR(ABS(v_fact_cntr - v_sum_cntr)) || ' <----<<< Error!!');
                
                INSERT_VALIDATION_RESULT(
                    p_owner => 'CCL',
                    p_table_name => 'FACILITY_FACT',
                    p_validation_type => 'CCL_FACILITY_COMPARE',
                    p_time_load => rec.id_time_load,
                    p_count_value => v_fact_cntr,
                    p_status => 'ERROR',
                    p_severity => 'HIGH',
                    p_message => 'FACILITY_FACT count does not match CCL_FACILITY_SUM',
                    p_compare_table => 'CCL_FACILITY_SUM',
                    p_compare_count => v_sum_cntr,
                    p_match_status => 'NO_MATCH',
                    p_email_address => NULL
                );
                
                v_diff_cntr := v_diff_cntr + 1;
            END LOOP;
            
            IF v_diff_cntr = 0 THEN
                DBMS_OUTPUT.PUT_LINE('CCL.FACILITY_FACT record counts match CCL.CCL_FACILITY_SUM for all time loads.');
                
                INSERT_VALIDATION_RESULT(
                    p_owner => 'CCL',
                    p_table_name => 'FACILITY_FACT',
                    p_validation_type => 'CCL_FACILITY_COMPARE',
                    p_time_load => NULL,
                    p_count_value => 0,
                    p_status => 'PASS',
                    p_severity => 'LOW',
                    p_message => 'FACILITY_FACT matches CCL_FACILITY_SUM for all time loads',
                    p_compare_table => 'CCL_FACILITY_SUM',
                    p_match_status => 'MATCH',
                    p_email_address => NULL
                );
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('CCL.FACILITY_FACT record counts match CCL.CCL_FACILITY_SUM for all time loads.');
                
                INSERT_VALIDATION_RESULT(
                    p_owner => 'CCL',
                    p_table_name => 'FACILITY_FACT',
                    p_validation_type => 'CCL_FACILITY_COMPARE',
                    p_time_load => NULL,
                    p_count_value => 0,
                    p_status => 'PASS',
                    p_severity => 'LOW',
                    p_message => 'FACILITY_FACT matches CCL_FACILITY_SUM for all time loads',
                    p_compare_table => 'CCL_FACILITY_SUM',
                    p_match_status => 'MATCH',
                    p_email_address => NULL
                );
        END;
        
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in CHECK_CCL_FACILITY_COMPARISON: ' || SQLERRM);
            RAISE;
    END CHECK_CCL_FACILITY_COMPARISON;

    -- ========================================================================
    -- Calculation and Processing Procedures
    -- ========================================================================
    
    FUNCTION CALCULATE_PERCENTAGE_CHANGE(
        p_current_count IN NUMBER,
        p_prior_count   IN NUMBER
    ) RETURN NUMBER IS
    BEGIN
        -- TODO: Calculate ((current - prior) / prior) * 100
        -- Handle division by zero (return NULL if prior = 0)
        
        IF p_prior_count IS NULL OR p_prior_count = 0 THEN
            RETURN NULL;
        END IF;
        
        RETURN ROUND(((p_current_count - p_prior_count) / p_prior_count) * 100, 2);
    END CALCULATE_PERCENTAGE_CHANGE;
    
    FUNCTION IS_THRESHOLD_VIOLATION(
        p_pct_change   IN NUMBER,
        p_threshold_pct IN NUMBER
    ) RETURN BOOLEAN IS
    BEGIN
        -- TODO: Check if absolute percentage change exceeds threshold
        IF p_pct_change IS NULL THEN
            RETURN FALSE;
        END IF;
        
        RETURN ABS(p_pct_change) > p_threshold_pct;
    END IS_THRESHOLD_VIOLATION;
    
    PROCEDURE ASSIGN_STATUS_SEVERITY(
        p_pct_change    IN NUMBER,
        p_threshold_pct IN NUMBER,
        p_status       OUT VARCHAR2,
        p_severity     OUT VARCHAR2
    ) IS
        v_abs_pct_change NUMBER;
    BEGIN
        -- Assign status and severity based on absolute percentage change
        -- Status: PASS (<=5%), WARNING (>5% and <=10%), ERROR (>10%)
        -- Severity: HIGH (ERROR), MEDIUM (WARNING), LOW (PASS)
        
        IF p_pct_change IS NULL THEN
            p_status := 'PASS';
            p_severity := 'LOW';
            RETURN;
        END IF;
        
        v_abs_pct_change := ABS(p_pct_change);
        
        -- Determine status based on percentage change
        IF v_abs_pct_change <= 5 THEN
            p_status := 'PASS';
            p_severity := 'LOW';
        ELSIF v_abs_pct_change <= 10 THEN
            p_status := 'WARNING';
            p_severity := 'MEDIUM';
        ELSE
            p_status := 'ERROR';
            p_severity := 'HIGH';
        END IF;
        
        -- Override with threshold check: if exceeds threshold, mark as ERROR
        IF v_abs_pct_change > p_threshold_pct THEN
            p_status := 'ERROR';
            p_severity := 'HIGH';
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in ASSIGN_STATUS_SEVERITY: ' || SQLERRM);
            p_status := 'ERROR';
            p_severity := 'HIGH';
    END ASSIGN_STATUS_SEVERITY;

    -- ========================================================================
    -- Results Storage Procedures
    -- ========================================================================
    
    PROCEDURE INSERT_VALIDATION_RESULT(
        p_owner          IN VARCHAR2,
        p_table_name     IN VARCHAR2,
        p_validation_type IN VARCHAR2,
        p_time_load      IN NUMBER,
        p_column_name    IN VARCHAR2 DEFAULT NULL,
        p_group_by_value IN VARCHAR2 DEFAULT NULL,
        p_count_value    IN NUMBER,
        p_prior_count    IN NUMBER DEFAULT NULL,
        p_pct_change     IN NUMBER DEFAULT NULL,
        p_status         IN VARCHAR2,
        p_severity       IN VARCHAR2,
        p_message        IN VARCHAR2 DEFAULT NULL,
        p_email_address  IN VARCHAR2 DEFAULT NULL,
        p_compare_table  IN VARCHAR2 DEFAULT NULL,
        p_compare_count  IN NUMBER DEFAULT NULL,
        p_match_status   IN VARCHAR2 DEFAULT NULL,
        p_avg_12month_count IN NUMBER DEFAULT NULL,
        p_pct_change_12month IN NUMBER DEFAULT NULL
    ) IS
    BEGIN
        -- Insert into TBL_MDC_VALIDATION_RESULTS
        INSERT INTO TBL_MDC_VALIDATION_RESULTS (
            RESULT_ID, RUN_DATE, OWNER, TABLE_NAME, VALIDATION_TYPE,
            TIME_LOAD, COLUMN_NAME, GROUP_BY_VALUE, COUNT_VALUE, PRIOR_COUNT,
            PCT_CHANGE, STATUS, SEVERITY, MESSAGE, EMAIL_ADDRESS,
            COMPARE_TABLE, COMPARE_COUNT, MATCH_STATUS,
            AVG_12MONTH_COUNT, PCT_CHANGE_12MONTH
        ) VALUES (
            SEQ_MDC_RESULT_ID.NEXTVAL, SYSDATE, p_owner, p_table_name, p_validation_type,
            p_time_load, p_column_name, p_group_by_value, p_count_value, p_prior_count,
            p_pct_change, p_status, p_severity, p_message, p_email_address,
            p_compare_table, p_compare_count, p_match_status,
            p_avg_12month_count, p_pct_change_12month
        );
    END INSERT_VALIDATION_RESULT;

    -- ========================================================================
    -- Email Generation Procedures
    -- ========================================================================
    
    PROCEDURE SEND_VALIDATION_EMAILS(
        p_run_date IN DATE DEFAULT NULL
    ) IS
        v_run_date DATE;
        v_email_address VARCHAR2(255);
        v_html_content CLOB;
        v_subject VARCHAR2(500);
        v_proc_name VARCHAR2(100) := 'PKG_MDC_VALIDATION.SEND_VALIDATION_EMAILS';
        v_recipients VARCHAR2(4000);
        v_table_count NUMBER;
        v_error_count NUMBER := 0;
        v_success_count NUMBER := 0;
        
        -- Cursor to get distinct email addresses with results
        CURSOR c_emails IS
            SELECT DISTINCT email_address
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address IS NOT NULL
              AND TRUNC(run_date) = TRUNC(v_run_date)
            ORDER BY email_address;
    BEGIN
        -- Set run date
        IF p_run_date IS NULL THEN
            v_run_date := SYSDATE;
        ELSE
            v_run_date := p_run_date;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Sending Validation Results Emails');
        DBMS_OUTPUT.PUT_LINE('Run Date: ' || TO_CHAR(v_run_date, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Send email to each analyst
        FOR email_rec IN c_emails LOOP
            BEGIN
                v_email_address := email_rec.email_address;
                
                -- Generate HTML content
                GENERATE_EMAIL_CONTENT(
                    p_email_address => v_email_address,
                    p_run_date => v_run_date,
                    p_html_content => v_html_content
                );
                
                -- Check if there's content to send
                IF v_html_content IS NULL OR DBMS_LOB.GETLENGTH(v_html_content) = 0 THEN
                    DBMS_OUTPUT.PUT_LINE('  Skipping ' || v_email_address || ' - no results found');
                    CONTINUE;
                END IF;
                
                -- Get table count for this analyst
                SELECT COUNT(DISTINCT owner || '.' || table_name)
                INTO v_table_count
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = v_email_address
                  AND TRUNC(run_date) = TRUNC(v_run_date);
                
                -- Build subject line
                v_subject := 'MDC Validation Results - ' || TO_CHAR(v_run_date, 'YYYY-MM-DD') || 
                           ' - ' || v_table_count || ' table(s)';
                
                -- Get recipients (use Email_GetRecipients if available, otherwise use email_address)
                BEGIN
                    v_recipients := caps.mail_pkg.Email_GetRecipients('tech');
                    -- If the function returns NULL or empty, use the email_address directly
                    IF v_recipients IS NULL OR LENGTH(TRIM(v_recipients)) = 0 THEN
                        v_recipients := v_email_address;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Fallback to direct email address
                        v_recipients := v_email_address;
                END;
                
                -- Send email using caps.mail_pkg.send
                -- Note: caps.mail_pkg.send accepts p_body IN LONG
                -- Convert CLOB to LONG - try passing CLOB directly first, fall back to conversion if needed
                DECLARE
                    v_html_length NUMBER := DBMS_LOB.GETLENGTH(v_html_content);
                    v_html_body LONG;
                BEGIN
                    -- Try to pass CLOB directly - Oracle may handle the conversion automatically
                    -- If that fails, we'll convert manually
                    BEGIN
                        -- Attempt 1: Try to pass CLOB directly (if procedure accepts it)
                        -- This will fail if procedure signature requires LONG, but worth trying
                        BEGIN
                            caps.mail_pkg.send(
                                v_proc_name,
                                v_recipients,
                                NULL,
                                v_subject,
                                v_html_content  -- Try passing CLOB directly
                            );
                            DBMS_OUTPUT.PUT_LINE('  ✓ Email sent (CLOB passed directly) to ' || v_email_address || 
                                               ' (' || v_table_count || ' table(s), ' || TO_CHAR(v_html_length) || ' chars)');
                            RETURN;  -- Success, exit
                        EXCEPTION
                            WHEN OTHERS THEN
                                -- CLOB not accepted, need to convert to LONG
                                NULL;  -- Continue to conversion code below
                        END;
                    END;
                    
                    -- Attempt 2: Convert CLOB to LONG manually
                    -- For content <= 32KB, read directly
                    IF v_html_length <= 32767 THEN
                        v_html_body := DBMS_LOB.SUBSTR(v_html_content, v_html_length, 1);
                    ELSE
                        -- For larger content, we need to build LONG incrementally
                        -- But PL/SQL has limits on LONG concatenation
                        -- Strategy: Read in chunks and concatenate, but limit total size
                        -- to avoid buffer overflow
                        DECLARE
                            v_chunk_size NUMBER := 32767;
                            v_offset NUMBER := 1;
                            v_chunk VARCHAR2(32767);
                            v_max_size NUMBER := 100000;  -- Limit total size to ~100KB to avoid buffer issues
                        BEGIN
                            -- Read first chunk
                            v_chunk := DBMS_LOB.SUBSTR(v_html_content, v_chunk_size, v_offset);
                            v_html_body := v_chunk;
                            v_offset := v_offset + LENGTH(v_chunk);
                            
                            -- Continue reading chunks, but limit total size
                            WHILE v_offset <= v_html_length AND v_offset <= v_max_size LOOP
                                v_chunk := DBMS_LOB.SUBSTR(v_html_content, v_chunk_size, v_offset);
                                
                                IF v_chunk IS NOT NULL AND LENGTH(v_chunk) > 0 THEN
                                    BEGIN
                                        v_html_body := v_html_body || v_chunk;
                                        v_offset := v_offset + LENGTH(v_chunk);
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            -- Concatenation failed, stop here
                                            DBMS_OUTPUT.PUT_LINE('  ⚠ WARNING: Email content truncated at ' || 
                                                               TO_CHAR(v_offset - 1) || ' characters');
                                            EXIT;
                                    END;
                                ELSE
                                    EXIT;
                                END IF;
                            END LOOP;
                            
                            -- Warn if truncated
                            IF v_offset <= v_html_length THEN
                                DBMS_OUTPUT.PUT_LINE('  ⚠ WARNING: Email content truncated');
                                DBMS_OUTPUT.PUT_LINE('  Sent: ' || TO_CHAR(v_offset - 1) || ' of ' || 
                                                   TO_CHAR(v_html_length) || ' characters');
                            END IF;
                        END;
                    END IF;
                    
                    -- Send email with LONG content
                    caps.mail_pkg.send(
                        v_proc_name,
                        v_recipients,
                        NULL,
                        v_subject,
                        v_html_body
                    );
                    
                    DBMS_OUTPUT.PUT_LINE('  ✓ Email sent to ' || v_email_address || ' (' || v_table_count || ' table(s), ' || 
                                       TO_CHAR(v_html_length) || ' chars)');
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ✗ ERROR sending email: ' || SQLERRM);
                        DBMS_OUTPUT.PUT_LINE('  Content length: ' || TO_CHAR(v_html_length));
                        DBMS_OUTPUT.PUT_LINE('  SQLCODE: ' || SQLCODE);
                        RAISE;
                END;
                
                -- Free temporary CLOB
                IF DBMS_LOB.ISTEMPORARY(v_html_content) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_html_content);
                END IF;
                
                v_success_count := v_success_count + 1;
                DBMS_OUTPUT.PUT_LINE('  ✓ Email sent to ' || v_email_address || ' (' || v_table_count || ' table(s))');
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ✗ ERROR sending email to ' || v_email_address || ': ' || SQLERRM);
                    -- Free CLOB on error
                    IF v_html_content IS NOT NULL AND DBMS_LOB.ISTEMPORARY(v_html_content) = 1 THEN
                        DBMS_LOB.FREETEMPORARY(v_html_content);
                    END IF;
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Email Summary:');
        DBMS_OUTPUT.PUT_LINE('  Successfully sent: ' || v_success_count);
        IF v_error_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('  Errors: ' || v_error_count);
        END IF;
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in SEND_VALIDATION_EMAILS: ' || SQLERRM);
            RAISE;
    END SEND_VALIDATION_EMAILS;
    
    -- ========================================================================
    -- File Generation Procedure (Alternative to Email)
    -- ========================================================================
    
    PROCEDURE GENERATE_VALIDATION_FILES(
        p_directory IN VARCHAR2,
        p_run_date IN DATE DEFAULT NULL
    ) IS
        v_run_date DATE;
        v_email_address VARCHAR2(255);
        v_html_content CLOB;
        v_file_path VARCHAR2(1000);
        v_file_name VARCHAR2(500);
        v_file_handle UTL_FILE.FILE_TYPE;
        v_chunk_size NUMBER := 32767;
        v_offset NUMBER := 1;
        v_chunk VARCHAR2(32767);
        v_html_length NUMBER;
        v_table_count NUMBER;
        v_file_count NUMBER := 0;
        v_error_count NUMBER := 0;
        
        -- Cursor to get distinct email addresses with results
        CURSOR c_emails IS
            SELECT DISTINCT email_address
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address IS NOT NULL
              AND TRUNC(run_date) = TRUNC(v_run_date)
            ORDER BY email_address;
    BEGIN
        -- Set run date
        IF p_run_date IS NULL THEN
            v_run_date := SYSDATE;
        ELSE
            v_run_date := p_run_date;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Generating Validation Results HTML Files');
        DBMS_OUTPUT.PUT_LINE('Run Date: ' || TO_CHAR(v_run_date, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('Directory: ' || p_directory);
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Generate file for each analyst
        FOR email_rec IN c_emails LOOP
            BEGIN
                v_email_address := email_rec.email_address;
                
                -- Generate HTML content
                GENERATE_EMAIL_CONTENT(
                    p_email_address => v_email_address,
                    p_run_date => v_run_date,
                    p_html_content => v_html_content
                );
                
                -- Check if there's content to write
                IF v_html_content IS NULL OR DBMS_LOB.GETLENGTH(v_html_content) = 0 THEN
                    DBMS_OUTPUT.PUT_LINE('  Skipping ' || v_email_address || ' - no results found');
                    CONTINUE;
                END IF;
                
                v_html_length := DBMS_LOB.GETLENGTH(v_html_content);
                
                -- Get table count for this analyst
                SELECT COUNT(DISTINCT owner || '.' || table_name)
                INTO v_table_count
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = v_email_address
                  AND TRUNC(run_date) = TRUNC(v_run_date);
                
                -- Generate file name (sanitize email address for filename)
                v_file_name := REPLACE(REPLACE(REPLACE(v_email_address, '@', '_at_'), '.', '_'), '/', '_') || 
                              '_' || TO_CHAR(v_run_date, 'YYYYMMDD_HH24MISS') || '.html';
                v_file_path := p_directory || '/' || v_file_name;
                
                -- Open file for writing
                v_file_handle := UTL_FILE.FOPEN(
                    location => p_directory,
                    filename => v_file_name,
                    open_mode => 'W',
                    max_linesize => 32767
                );
                
                -- Write CLOB content to file in chunks
                v_offset := 1;
                WHILE v_offset <= v_html_length LOOP
                    v_chunk := DBMS_LOB.SUBSTR(v_html_content, v_chunk_size, v_offset);
                    
                    IF v_chunk IS NOT NULL THEN
                        UTL_FILE.PUT(v_file_handle, v_chunk);
                        v_offset := v_offset + LENGTH(v_chunk);
                    ELSE
                        EXIT;
                    END IF;
                END LOOP;
                
                -- Close file
                UTL_FILE.FCLOSE(v_file_handle);
                
                v_file_count := v_file_count + 1;
                DBMS_OUTPUT.PUT_LINE('  ✓ Generated file: ' || v_file_name || 
                                   ' (' || v_table_count || ' table(s), ' || TO_CHAR(v_html_length) || ' chars)');
                
                -- Free temporary CLOB
                IF DBMS_LOB.ISTEMPORARY(v_html_content) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_html_content);
                END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ✗ ERROR generating file for ' || v_email_address || ': ' || SQLERRM);
                    -- Close file if open
                    IF v_file_handle IS NOT NULL THEN
                        BEGIN
                            UTL_FILE.FCLOSE(v_file_handle);
                        EXCEPTION
                            WHEN OTHERS THEN NULL;
                        END;
                    END IF;
                    -- Free CLOB on error
                    IF v_html_content IS NOT NULL AND DBMS_LOB.ISTEMPORARY(v_html_content) = 1 THEN
                        DBMS_LOB.FREETEMPORARY(v_html_content);
                    END IF;
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('File Generation Summary:');
        DBMS_OUTPUT.PUT_LINE('  Files generated: ' || v_file_count);
        IF v_error_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('  Errors: ' || v_error_count);
        END IF;
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in GENERATE_VALIDATION_FILES: ' || SQLERRM);
            RAISE;
    END GENERATE_VALIDATION_FILES;
    
    -- ========================================================================
    -- Store Reports in Database Table (No Directory Permissions Needed)
    -- ========================================================================
    
    PROCEDURE STORE_VALIDATION_REPORTS(
        p_run_date IN DATE DEFAULT NULL
    ) IS
        v_run_date DATE;
        v_email_address VARCHAR2(255);
        v_html_content CLOB;
        v_table_count NUMBER;
        v_report_count NUMBER := 0;
        v_error_count NUMBER := 0;
        
        -- Cursor to get distinct email addresses with results
        CURSOR c_emails IS
            SELECT DISTINCT email_address
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address IS NOT NULL
              AND TRUNC(run_date) = TRUNC(v_run_date)
            ORDER BY email_address;
    BEGIN
        -- Set run date
        IF p_run_date IS NULL THEN
            v_run_date := SYSDATE;
        ELSE
            v_run_date := p_run_date;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Storing Validation Results HTML Reports in Database');
        DBMS_OUTPUT.PUT_LINE('Run Date: ' || TO_CHAR(v_run_date, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Generate and store report for each analyst
        FOR email_rec IN c_emails LOOP
            BEGIN
                v_email_address := email_rec.email_address;
                
                -- Generate HTML content
                GENERATE_EMAIL_CONTENT(
                    p_email_address => v_email_address,
                    p_run_date => v_run_date,
                    p_html_content => v_html_content
                );
                
                -- Check if there's content to store
                IF v_html_content IS NULL OR DBMS_LOB.GETLENGTH(v_html_content) = 0 THEN
                    DBMS_OUTPUT.PUT_LINE('  Skipping ' || v_email_address || ' - no results found');
                    CONTINUE;
                END IF;
                
                -- Get table count for this analyst
                SELECT COUNT(DISTINCT owner || '.' || table_name)
                INTO v_table_count
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = v_email_address
                  AND TRUNC(run_date) = TRUNC(v_run_date);
                
                -- Delete existing report for this email/date if it exists
                DELETE FROM TBL_MDC_VALIDATION_HTML_REPORTS
                WHERE email_address = v_email_address
                  AND TRUNC(run_date) = TRUNC(v_run_date);
                
                -- Insert new report
                INSERT INTO TBL_MDC_VALIDATION_HTML_REPORTS (
                    REPORT_ID,
                    EMAIL_ADDRESS,
                    RUN_DATE,
                    TABLE_COUNT,
                    HTML_CONTENT,
                    CREATED_DATE
                ) VALUES (
                    SEQ_MDC_REPORT_ID.NEXTVAL,
                    v_email_address,
                    v_run_date,
                    v_table_count,
                    v_html_content,
                    SYSDATE
                );
                
                COMMIT;
                
                v_report_count := v_report_count + 1;
                DBMS_OUTPUT.PUT_LINE('  ✓ Stored report for ' || v_email_address || 
                                   ' (' || v_table_count || ' table(s), ' || 
                                   TO_CHAR(DBMS_LOB.GETLENGTH(v_html_content)) || ' chars)');
                
                -- Free temporary CLOB
                IF DBMS_LOB.ISTEMPORARY(v_html_content) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_html_content);
                END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('  ✗ ERROR storing report for ' || v_email_address || ': ' || SQLERRM);
                    -- Free CLOB on error
                    IF v_html_content IS NOT NULL AND DBMS_LOB.ISTEMPORARY(v_html_content) = 1 THEN
                        DBMS_LOB.FREETEMPORARY(v_html_content);
                    END IF;
                    ROLLBACK;
            END;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('Report Storage Summary:');
        DBMS_OUTPUT.PUT_LINE('  Reports stored: ' || v_report_count);
        IF v_error_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('  Errors: ' || v_error_count);
        END IF;
        DBMS_OUTPUT.PUT_LINE('============================================================================');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('To view reports, query:');
        DBMS_OUTPUT.PUT_LINE('  SELECT email_address, run_date, table_count, html_content');
        DBMS_OUTPUT.PUT_LINE('  FROM TBL_MDC_VALIDATION_HTML_REPORTS');
        DBMS_OUTPUT.PUT_LINE('  WHERE TRUNC(run_date) = TRUNC(SYSDATE)');
        DBMS_OUTPUT.PUT_LINE('  ORDER BY email_address;');
        DBMS_OUTPUT.PUT_LINE('');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in STORE_VALIDATION_REPORTS: ' || SQLERRM);
            RAISE;
    END STORE_VALIDATION_REPORTS;
    
    PROCEDURE GENERATE_EMAIL_CONTENT(
        p_email_address IN VARCHAR2,
        p_run_date      IN DATE,
        p_html_content  OUT CLOB
    ) IS
        v_html CLOB;
        v_color VARCHAR2(20);
        v_bg_color VARCHAR2(20);
        v_total_results NUMBER := 0;
        v_error_count NUMBER := 0;
        v_warning_count NUMBER := 0;
        v_pass_count NUMBER := 0;
        v_table_count NUMBER := 0;
        v_db_name VARCHAR2(50);
        v_lf VARCHAR2(2) := CHR(13) || CHR(10);
        v_html_text VARCHAR2(32767);
        
        -- Helper function to get color based on status
        FUNCTION GET_COLOR_CODE(p_pct_change NUMBER, p_status VARCHAR2) RETURN VARCHAR2 IS
        BEGIN
            IF p_status = 'ERROR' THEN
                RETURN '#FFB6C1';  -- Light red
            ELSIF p_status = 'WARNING' THEN
                RETURN '#FFFFE0';  -- Light yellow
            ELSIF p_status = 'PASS' THEN
                RETURN '#90EE90';  -- Light green
            ELSE
                RETURN '#FFFFFF';  -- White
            END IF;
        END GET_COLOR_CODE;
        
        -- Helper procedure to append to CLOB
        PROCEDURE APPEND_HTML(p_text VARCHAR2) IS
        BEGIN
            IF LENGTH(p_text) <= 32767 THEN
                DBMS_LOB.WRITEAPPEND(v_html, LENGTH(p_text), p_text);
            ELSE
                -- Handle large text by chunking
                FOR i IN 1..CEIL(LENGTH(p_text) / 32767) LOOP
                    v_html_text := SUBSTR(p_text, (i-1)*32767 + 1, 32767);
                    DBMS_LOB.WRITEAPPEND(v_html, LENGTH(v_html_text), v_html_text);
                END LOOP;
            END IF;
        END APPEND_HTML;
        
    BEGIN
        -- Initialize CLOB
        DBMS_LOB.CREATETEMPORARY(v_html, TRUE);
        
        -- Get summary statistics
        SELECT COUNT(*),
               SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
               COUNT(DISTINCT owner || '.' || table_name)
        INTO v_total_results, v_error_count, v_warning_count, v_pass_count, v_table_count
        FROM TBL_MDC_VALIDATION_RESULTS
        WHERE email_address = p_email_address
          AND TRUNC(run_date) = TRUNC(p_run_date);
        
        -- If no results, return empty
        IF v_total_results = 0 THEN
            p_html_content := NULL;
            IF DBMS_LOB.ISTEMPORARY(v_html) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_html);
            END IF;
            RETURN;
        END IF;
        
        -- Get database name
        v_db_name := GET_DATABASE_NAME;
        
        -- Start HTML document
        APPEND_HTML('<!DOCTYPE html>' || v_lf);
        APPEND_HTML('<html>' || v_lf);
        APPEND_HTML('<head>' || v_lf);
        APPEND_HTML('<meta charset="UTF-8">' || v_lf);
        APPEND_HTML('<title>MDC Validation Results</title>' || v_lf);
        APPEND_HTML('<style>' || v_lf);
        APPEND_HTML('  body { font-family: Arial, sans-serif; margin: 20px; }' || v_lf);
        APPEND_HTML('  table { border-collapse: collapse; width: 100%; margin: 10px 0; }' || v_lf);
        APPEND_HTML('  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }' || v_lf);
        APPEND_HTML('  th { background-color: #f0f0f0; font-weight: bold; }' || v_lf);
        APPEND_HTML('  .header { background-color: #4CAF50; color: white; padding: 15px; }' || v_lf);
        APPEND_HTML('  .summary { background-color: #f9f9f9; padding: 10px; margin: 10px 0; }' || v_lf);
        APPEND_HTML('  .error { background-color: #FFB6C1; }' || v_lf);
        APPEND_HTML('  .warning { background-color: #FFFFE0; }' || v_lf);
        APPEND_HTML('  .pass { background-color: #90EE90; }' || v_lf);
        APPEND_HTML('</style>' || v_lf);
        APPEND_HTML('</head>' || v_lf);
        APPEND_HTML('<body>' || v_lf);
        
        -- Header Section
        APPEND_HTML('<div class="header">' || v_lf);
        APPEND_HTML('<h1>MDC Validation Results</h1>' || v_lf);
        APPEND_HTML('<p><strong>Run Date:</strong> ' || TO_CHAR(p_run_date, 'YYYY-MM-DD HH24:MI:SS') || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Analyst:</strong> ' || p_email_address || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Database:</strong> ' || v_db_name || '</p>' || v_lf);
        APPEND_HTML('</div>' || v_lf);
        
        -- Executive Summary
        APPEND_HTML('<div class="summary">' || v_lf);
        APPEND_HTML('<h2>Executive Summary</h2>' || v_lf);
        APPEND_HTML('<p><strong>Total Tables Validated:</strong> ' || v_table_count || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Total Validations:</strong> ' || v_total_results || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Errors:</strong> <span style="color: red;">' || v_error_count || '</span></p>' || v_lf);
        APPEND_HTML('<p><strong>Warnings:</strong> <span style="color: orange;">' || v_warning_count || '</span></p>' || v_lf);
        APPEND_HTML('<p><strong>Passed:</strong> <span style="color: green;">' || v_pass_count || '</span></p>' || v_lf);
        APPEND_HTML('</div>' || v_lf);
        
        -- High Priority Issues Section (ERROR status only)
        IF v_error_count > 0 THEN
            APPEND_HTML('<h2 style="color: red;">High Priority Issues (Errors)</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Validation Type</th><th>Time Load</th><th>% Change</th><th>Status</th><th>Message</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
                FOR error_rec IN (
                    SELECT owner, table_name, validation_type, time_load, 
                           pct_change, status, severity, message
                    FROM TBL_MDC_VALIDATION_RESULTS
                    WHERE email_address = p_email_address
                      AND TRUNC(run_date) = TRUNC(p_run_date)
                      AND status = 'ERROR'
                    ORDER BY severity, owner, table_name, validation_type, time_load
                ) LOOP
                v_bg_color := GET_COLOR_CODE(error_rec.pct_change, error_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || error_rec.owner || '.' || error_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || error_rec.validation_type || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(error_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(error_rec.pct_change, '999.99'), 'N/A') || '%</td>' || v_lf);
                APPEND_HTML('<td>' || error_rec.status || '</td>' || v_lf);
                APPEND_HTML('<td>' || SUBSTR(REPLACE(REPLACE(error_rec.message, '<', '&lt;'), '>', '&gt;'), 1, 200) || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
        END IF;
        
        -- Row Count Validations Section
        APPEND_HTML('<h2>Row Count Validations</h2>' || v_lf);
        APPEND_HTML('<table>' || v_lf);
        APPEND_HTML('<thead><tr><th>Table</th><th>Time Load</th><th>Count</th><th>Prior Count</th><th>% Change</th><th>Status</th><th>Group By</th></tr></thead>' || v_lf);
        APPEND_HTML('<tbody>' || v_lf);
        
        FOR row_rec IN (
            SELECT owner, table_name, time_load, count_value, prior_count,
                   pct_change, status, severity, group_by_value
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address = p_email_address
              AND TRUNC(run_date) = TRUNC(p_run_date)
              AND validation_type = 'ROW_COUNT'
            ORDER BY owner, table_name, time_load
        ) LOOP
            v_bg_color := GET_COLOR_CODE(row_rec.pct_change, row_rec.status);
            APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
            APPEND_HTML('<td>' || row_rec.owner || '.' || row_rec.table_name || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(TO_CHAR(row_rec.time_load), 'N/A') || '</td>' || v_lf);
            APPEND_HTML('<td>' || TO_CHAR(row_rec.count_value) || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(TO_CHAR(row_rec.prior_count), 'N/A') || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(TO_CHAR(row_rec.pct_change, '999.99'), 'N/A') || '%</td>' || v_lf);
            APPEND_HTML('<td>' || row_rec.status || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(row_rec.group_by_value, '-') || '</td>' || v_lf);
            APPEND_HTML('</tr>' || v_lf);
        END LOOP;
        
        APPEND_HTML('</tbody></table>' || v_lf);
        
        -- Column Count Validations Section (Pivot Format)
        -- Group by table, then display columns as rows with time loads as columns
        FOR table_rec IN (
            SELECT DISTINCT owner, table_name
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address = p_email_address
              AND TRUNC(run_date) = TRUNC(p_run_date)
              AND validation_type = 'COLUMN_COUNT'
            ORDER BY owner, table_name
        ) LOOP
            APPEND_HTML('<h2>Column Count Validations - ' || table_rec.owner || '.' || table_rec.table_name || '</h2>' || v_lf);
            
            -- Get distinct time loads for this table (to build column headers)
            DECLARE
                TYPE t_time_load_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                v_time_loads t_time_load_tab;
                v_tl_idx PLS_INTEGER := 0;
                v_max_status VARCHAR2(20);
            BEGIN
                -- Get all time loads for this table, ordered
                FOR tl_rec IN (
                    SELECT DISTINCT time_load
                    FROM TBL_MDC_VALIDATION_RESULTS
                    WHERE email_address = p_email_address
                      AND TRUNC(run_date) = TRUNC(p_run_date)
                      AND validation_type = 'COLUMN_COUNT'
                      AND owner = table_rec.owner
                      AND table_name = table_rec.table_name
                    ORDER BY time_load
                ) LOOP
                    v_tl_idx := v_tl_idx + 1;
                    v_time_loads(v_tl_idx) := tl_rec.time_load;
                END LOOP;
                
                -- Build table header with time load columns
                APPEND_HTML('<table style="font-size: 11px;">' || v_lf);
                APPEND_HTML('<thead><tr>' || v_lf);
                APPEND_HTML('<th style="position: sticky; left: 0; background-color: #f0f0f0; z-index: 10;">Column Name</th>' || v_lf);
                
                -- Add time load columns
                FOR i IN 1..v_time_loads.COUNT LOOP
                    APPEND_HTML('<th>TL ' || v_time_loads(i) || '</th>' || v_lf);
                END LOOP;
                
                APPEND_HTML('<th>% Change<br/>(Last)</th>' || v_lf);
                APPEND_HTML('<th>% Change<br/>(12-Month Avg)</th>' || v_lf);
                APPEND_HTML('<th>Status</th>' || v_lf);
                APPEND_HTML('</tr></thead>' || v_lf);
                APPEND_HTML('<tbody>' || v_lf);
                
                -- Get all columns for this table
                FOR col_rec IN (
                    SELECT DISTINCT column_name
                    FROM TBL_MDC_VALIDATION_RESULTS
                    WHERE email_address = p_email_address
                      AND TRUNC(run_date) = TRUNC(p_run_date)
                      AND validation_type = 'COLUMN_COUNT'
                      AND owner = table_rec.owner
                      AND table_name = table_rec.table_name
                    ORDER BY column_name
                ) LOOP
                    -- Get the latest status for this column (for row coloring)
                    SELECT MAX(status)
                    INTO v_max_status
                    FROM TBL_MDC_VALIDATION_RESULTS
                    WHERE email_address = p_email_address
                      AND TRUNC(run_date) = TRUNC(p_run_date)
                      AND validation_type = 'COLUMN_COUNT'
                      AND owner = table_rec.owner
                      AND table_name = table_rec.table_name
                      AND column_name = col_rec.column_name;
                    
                    v_bg_color := GET_COLOR_CODE(NULL, v_max_status);
                    APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                    APPEND_HTML('<td style="position: sticky; left: 0; background-color: ' || v_bg_color || '; font-weight: bold;">' || col_rec.column_name || '</td>' || v_lf);
                    
                    -- Add count values for each time load
                    FOR i IN 1..v_time_loads.COUNT LOOP
                        DECLARE
                            v_count NUMBER;
                        BEGIN
                            SELECT count_value
                            INTO v_count
                            FROM (
                                SELECT count_value
                                FROM TBL_MDC_VALIDATION_RESULTS
                                WHERE email_address = p_email_address
                                  AND TRUNC(run_date) = TRUNC(p_run_date)
                                  AND validation_type = 'COLUMN_COUNT'
                                  AND owner = table_rec.owner
                                  AND table_name = table_rec.table_name
                                  AND column_name = col_rec.column_name
                                  AND time_load = v_time_loads(i)
                                ORDER BY result_id DESC
                            )
                            WHERE ROWNUM = 1;
                            
                            APPEND_HTML('<td>' || TO_CHAR(v_count) || '</td>' || v_lf);
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                APPEND_HTML('<td>-</td>' || v_lf);
                        END;
                    END LOOP;
                    
                    -- Get latest % change values (from most recent time load)
                    DECLARE
                        v_pct_change NUMBER;
                        v_pct_change_12month NUMBER;
                        v_status VARCHAR2(20);
                    BEGIN
                        SELECT pct_change, pct_change_12month, status
                        INTO v_pct_change, v_pct_change_12month, v_status
                        FROM (
                            SELECT pct_change, pct_change_12month, status
                            FROM TBL_MDC_VALIDATION_RESULTS
                            WHERE email_address = p_email_address
                              AND TRUNC(run_date) = TRUNC(p_run_date)
                              AND validation_type = 'COLUMN_COUNT'
                              AND owner = table_rec.owner
                              AND table_name = table_rec.table_name
                              AND column_name = col_rec.column_name
                              AND time_load = (SELECT MAX(time_load) 
                                              FROM TBL_MDC_VALIDATION_RESULTS
                                              WHERE email_address = p_email_address
                                                AND TRUNC(run_date) = TRUNC(p_run_date)
                                                AND validation_type = 'COLUMN_COUNT'
                                                AND owner = table_rec.owner
                                                AND table_name = table_rec.table_name
                                                AND column_name = col_rec.column_name)
                            ORDER BY result_id DESC
                        )
                        WHERE ROWNUM = 1;
                        
                        APPEND_HTML('<td>' || NVL(TO_CHAR(v_pct_change, '999.99'), '-') || '%</td>' || v_lf);
                        APPEND_HTML('<td>' || NVL(TO_CHAR(v_pct_change_12month, '999.99'), '-') || '%</td>' || v_lf);
                        APPEND_HTML('<td>' || NVL(v_status, '-') || '</td>' || v_lf);
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            APPEND_HTML('<td>-</td>' || v_lf);
                            APPEND_HTML('<td>-</td>' || v_lf);
                            APPEND_HTML('<td>-</td>' || v_lf);
                    END;
                    
                    APPEND_HTML('</tr>' || v_lf);
                END LOOP;
                
                APPEND_HTML('</tbody></table>' || v_lf);
            END;
        END LOOP;
        
        -- Table Comparison Validations Section
        APPEND_HTML('<h2>Table Comparison Validations</h2>' || v_lf);
        APPEND_HTML('<table>' || v_lf);
        APPEND_HTML('<thead><tr><th>Table</th><th>Compare To</th><th>Time Load</th><th>Count</th><th>Compare Count</th><th>Match Status</th><th>Status</th></tr></thead>' || v_lf);
        APPEND_HTML('<tbody>' || v_lf);
        
        FOR comp_rec IN (
            SELECT owner, table_name, compare_table, time_load, count_value, compare_count,
                   match_status, status, severity
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address = p_email_address
              AND TRUNC(run_date) = TRUNC(p_run_date)
              AND validation_type = 'TABLE_COMPARE'
            ORDER BY owner, table_name, time_load
        ) LOOP
            v_bg_color := GET_COLOR_CODE(NULL, comp_rec.status);
            APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
            APPEND_HTML('<td>' || comp_rec.owner || '.' || comp_rec.table_name || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(comp_rec.compare_table, '-') || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(TO_CHAR(comp_rec.time_load), 'N/A') || '</td>' || v_lf);
            APPEND_HTML('<td>' || TO_CHAR(comp_rec.count_value) || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(TO_CHAR(comp_rec.compare_count), 'N/A') || '</td>' || v_lf);
            APPEND_HTML('<td>' || NVL(comp_rec.match_status, '-') || '</td>' || v_lf);
            APPEND_HTML('<td>' || comp_rec.status || '</td>' || v_lf);
            APPEND_HTML('</tr>' || v_lf);
        END LOOP;
        
        APPEND_HTML('</tbody></table>' || v_lf);
        
        -- Distinct Count Validations Section
        DECLARE
            v_has_distinct_count NUMBER := 0;
        BEGIN
            SELECT COUNT(*)
            INTO v_has_distinct_count
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address = p_email_address
              AND TRUNC(run_date) = TRUNC(p_run_date)
              AND validation_type = 'DISTINCT_COUNT';
            
            IF v_has_distinct_count > 0 THEN
            APPEND_HTML('<h2>Distinct Count Validations</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Column</th><th>Time Load</th><th>Distinct Count</th><th>Prior Count</th><th>% Change</th><th>Status</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
            FOR dist_rec IN (
                SELECT owner, table_name, column_name, time_load, count_value, prior_count,
                       pct_change, status, severity
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'DISTINCT_COUNT'
                ORDER BY owner, table_name, column_name, time_load
            ) LOOP
                v_bg_color := GET_COLOR_CODE(dist_rec.pct_change, dist_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || dist_rec.owner || '.' || dist_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(dist_rec.column_name, '-') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(dist_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || TO_CHAR(dist_rec.count_value) || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(dist_rec.prior_count), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(dist_rec.pct_change, '999.99'), 'N/A') || '%</td>' || v_lf);
                APPEND_HTML('<td>' || dist_rec.status || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
            END IF;
        END;
        
        -- Data Validation Section
        DECLARE
            v_has_data_validation NUMBER := 0;
        BEGIN
            SELECT COUNT(*)
            INTO v_has_data_validation
            FROM TBL_MDC_VALIDATION_RESULTS
            WHERE email_address = p_email_address
              AND TRUNC(run_date) = TRUNC(p_run_date)
              AND validation_type = 'DATA_VALIDATION';
            
            IF v_has_data_validation > 0 THEN
            APPEND_HTML('<h2>Data Validation Checks</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Time Load</th><th>Mismatch Count</th><th>Status</th><th>Message</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
            FOR data_rec IN (
                SELECT owner, table_name, time_load, count_value, status, severity, message
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'DATA_VALIDATION'
                ORDER BY owner, table_name, time_load
            ) LOOP
                v_bg_color := GET_COLOR_CODE(NULL, data_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || data_rec.owner || '.' || data_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(data_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || TO_CHAR(data_rec.count_value) || '</td>' || v_lf);
                APPEND_HTML('<td>' || data_rec.status || '</td>' || v_lf);
                APPEND_HTML('<td>' || SUBSTR(REPLACE(REPLACE(data_rec.message, '<', '&lt;'), '>', '&gt;'), 1, 200) || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
            END IF;
        END;
        
        -- Footer
        APPEND_HTML('<hr>' || v_lf);
        APPEND_HTML('<p><em>This is an automated email from the MDC Validation System.</em></p>' || v_lf);
        APPEND_HTML('<p><em>Generated on ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || '</em></p>' || v_lf);
        APPEND_HTML('</body>' || v_lf);
        APPEND_HTML('</html>' || v_lf);
        
        -- Return the HTML content
        p_html_content := v_html;
        
    EXCEPTION
        WHEN OTHERS THEN
            IF v_html IS NOT NULL AND DBMS_LOB.ISTEMPORARY(v_html) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_html);
            END IF;
            DBMS_OUTPUT.PUT_LINE('ERROR in GENERATE_EMAIL_CONTENT: ' || SQLERRM);
            p_html_content := NULL;
            RAISE;
    END GENERATE_EMAIL_CONTENT;
    
    PROCEDURE GENERATE_EMAIL_CONTENT_BY_TYPE(
        p_email_address    IN VARCHAR2,
        p_run_date         IN DATE,
        p_validation_type   IN VARCHAR2,
        p_part_number      IN NUMBER,
        p_total_parts       IN NUMBER,
        p_html_content      OUT CLOB
    ) IS
        v_html CLOB;
        v_bg_color VARCHAR2(20);
        v_db_name VARCHAR2(50);
        v_lf VARCHAR2(2) := CHR(13) || CHR(10);
        v_total_results NUMBER := 0;
        v_error_count NUMBER := 0;
        v_warning_count NUMBER := 0;
        v_pass_count NUMBER := 0;
        v_table_count NUMBER := 0;
        
        FUNCTION GET_COLOR_CODE(p_pct_change NUMBER, p_status VARCHAR2) RETURN VARCHAR2 IS
        BEGIN
            IF p_status = 'ERROR' THEN
                RETURN '#FFB6C1';
            ELSIF p_status = 'WARNING' THEN
                RETURN '#FFFFE0';
            ELSIF p_status = 'PASS' THEN
                RETURN '#90EE90';
            ELSE
                RETURN '#FFFFFF';
            END IF;
        END GET_COLOR_CODE;
        
        PROCEDURE APPEND_HTML(p_text VARCHAR2) IS
        BEGIN
            DBMS_LOB.WRITEAPPEND(v_html, LENGTH(p_text), p_text);
        END APPEND_HTML;
        
    BEGIN
        DBMS_LOB.CREATETEMPORARY(v_html, TRUE);
        
        -- Get summary statistics
        -- Special handling: if p_validation_type = 'ERROR', get all errors regardless of validation_type
        SELECT COUNT(*),
               SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END),
               SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
               COUNT(DISTINCT owner || '.' || table_name)
        INTO v_total_results, v_error_count, v_warning_count, v_pass_count, v_table_count
        FROM TBL_MDC_VALIDATION_RESULTS
        WHERE email_address = p_email_address
          AND TRUNC(run_date) = TRUNC(p_run_date)
          AND (
              (p_validation_type = 'ERROR' AND status = 'ERROR')
              OR (p_validation_type != 'ERROR' AND validation_type = p_validation_type)
          );
        
        IF v_total_results = 0 THEN
            p_html_content := NULL;
            IF DBMS_LOB.ISTEMPORARY(v_html) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_html);
            END IF;
            RETURN;
        END IF;
        
        v_db_name := GET_DATABASE_NAME;
        
        -- Start HTML document
        APPEND_HTML('<!DOCTYPE html>' || v_lf);
        APPEND_HTML('<html>' || v_lf);
        APPEND_HTML('<head>' || v_lf);
        APPEND_HTML('<meta charset="UTF-8">' || v_lf);
        APPEND_HTML('<title>MDC Validation Results</title>' || v_lf);
        APPEND_HTML('<style>' || v_lf);
        APPEND_HTML('  body { font-family: Arial, sans-serif; margin: 20px; }' || v_lf);
        APPEND_HTML('  table { border-collapse: collapse; width: 100%; margin: 10px 0; }' || v_lf);
        APPEND_HTML('  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }' || v_lf);
        APPEND_HTML('  th { background-color: #f0f0f0; font-weight: bold; }' || v_lf);
        APPEND_HTML('  .header { background-color: #4CAF50; color: white; padding: 15px; }' || v_lf);
        APPEND_HTML('  .summary { background-color: #f9f9f9; padding: 10px; margin: 10px 0; }' || v_lf);
        APPEND_HTML('</style>' || v_lf);
        APPEND_HTML('</head>' || v_lf);
        APPEND_HTML('<body>' || v_lf);
        
        -- Header Section
        APPEND_HTML('<div class="header">' || v_lf);
        APPEND_HTML('<h1>MDC Validation Results</h1>' || v_lf);
        APPEND_HTML('<p><strong>Run Date:</strong> ' || TO_CHAR(p_run_date, 'YYYY-MM-DD HH24:MI:SS') || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Analyst:</strong> ' || p_email_address || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Database:</strong> ' || v_db_name || '</p>' || v_lf);
        IF p_total_parts > 1 THEN
            APPEND_HTML('<p><strong>Part:</strong> ' || p_part_number || ' of ' || p_total_parts || '</p>' || v_lf);
        END IF;
        APPEND_HTML('</div>' || v_lf);
        
        -- Summary
        APPEND_HTML('<div class="summary">' || v_lf);
        APPEND_HTML('<h2>Summary' || CASE WHEN p_validation_type IS NOT NULL AND p_validation_type != 'ERROR' THEN ' - ' || p_validation_type WHEN p_validation_type = 'ERROR' THEN ' - Errors' ELSE '' END || '</h2>' || v_lf);
        APPEND_HTML('<p><strong>Tables in this part:</strong> ' || v_table_count || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Validations:</strong> ' || v_total_results || '</p>' || v_lf);
        APPEND_HTML('<p><strong>Errors:</strong> <span style="color: red;">' || v_error_count || '</span></p>' || v_lf);
        APPEND_HTML('<p><strong>Warnings:</strong> <span style="color: orange;">' || v_warning_count || '</span></p>' || v_lf);
        APPEND_HTML('<p><strong>Passed:</strong> <span style="color: green;">' || v_pass_count || '</span></p>' || v_lf);
        APPEND_HTML('</div>' || v_lf);
        
        -- Generate content based on validation type
        IF p_validation_type = 'ERROR' THEN
            -- High Priority Issues (all errors)
            IF v_error_count > 0 THEN
                APPEND_HTML('<h2 style="color: red;">High Priority Issues (Errors)</h2>' || v_lf);
                APPEND_HTML('<table>' || v_lf);
                APPEND_HTML('<thead><tr><th>Table</th><th>Validation Type</th><th>Time Load</th><th>% Change</th><th>Status</th><th>Message</th></tr></thead>' || v_lf);
                APPEND_HTML('<tbody>' || v_lf);
                
                FOR error_rec IN (
                    SELECT owner, table_name, validation_type, time_load, 
                           pct_change, status, severity, message
                    FROM TBL_MDC_VALIDATION_RESULTS
                    WHERE email_address = p_email_address
                      AND TRUNC(run_date) = TRUNC(p_run_date)
                      AND status = 'ERROR'
                    ORDER BY severity, owner, table_name, validation_type, time_load
                ) LOOP
                    v_bg_color := GET_COLOR_CODE(error_rec.pct_change, error_rec.status);
                    APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                    APPEND_HTML('<td>' || error_rec.owner || '.' || error_rec.table_name || '</td>' || v_lf);
                    APPEND_HTML('<td>' || error_rec.validation_type || '</td>' || v_lf);
                    APPEND_HTML('<td>' || NVL(TO_CHAR(error_rec.time_load), 'N/A') || '</td>' || v_lf);
                    APPEND_HTML('<td>' || NVL(TO_CHAR(error_rec.pct_change, '999.99'), 'N/A') || '%</td>' || v_lf);
                    APPEND_HTML('<td>' || error_rec.status || '</td>' || v_lf);
                    APPEND_HTML('<td>' || SUBSTR(REPLACE(REPLACE(NVL(error_rec.message, ''), '<', '&lt;'), '>', '&gt;'), 1, 200) || '</td>' || v_lf);
                    APPEND_HTML('</tr>' || v_lf);
                END LOOP;
                
                APPEND_HTML('</tbody></table>' || v_lf);
            END IF;
            
        ELSIF p_validation_type = 'ROW_COUNT' THEN
            APPEND_HTML('<h2>Row Count Validations</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Time Load</th><th>Count</th><th>Prior Count</th><th>% Change</th><th>Status</th><th>Group By</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
            FOR row_rec IN (
                SELECT owner, table_name, time_load, count_value, prior_count,
                       pct_change, status, severity, group_by_value
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'ROW_COUNT'
                ORDER BY owner, table_name, time_load
            ) LOOP
                v_bg_color := GET_COLOR_CODE(row_rec.pct_change, row_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || row_rec.owner || '.' || row_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(row_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || TO_CHAR(row_rec.count_value) || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(row_rec.prior_count), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(row_rec.pct_change, '999.99'), 'N/A') || '%</td>' || v_lf);
                APPEND_HTML('<td>' || row_rec.status || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(row_rec.group_by_value, '-') || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
            
        ELSIF p_validation_type = 'COLUMN_COUNT' THEN
            -- Column Count Validations Section (Pivot Format)
            -- Group by table, then display columns as rows with time loads as columns
            FOR table_rec IN (
                SELECT DISTINCT owner, table_name
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'COLUMN_COUNT'
                ORDER BY owner, table_name
            ) LOOP
                APPEND_HTML('<h2>Column Count Validations - ' || table_rec.owner || '.' || table_rec.table_name || '</h2>' || v_lf);
                
                -- Get distinct time loads for this table (to build column headers)
                DECLARE
                    TYPE t_time_load_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_time_loads t_time_load_tab;
                    v_tl_idx PLS_INTEGER := 0;
                    v_max_status VARCHAR2(20);
                BEGIN
                    -- Get all time loads for this table, ordered
                    FOR tl_rec IN (
                        SELECT DISTINCT time_load
                        FROM TBL_MDC_VALIDATION_RESULTS
                        WHERE email_address = p_email_address
                          AND TRUNC(run_date) = TRUNC(p_run_date)
                          AND validation_type = 'COLUMN_COUNT'
                          AND owner = table_rec.owner
                          AND table_name = table_rec.table_name
                        ORDER BY time_load
                    ) LOOP
                        v_tl_idx := v_tl_idx + 1;
                        v_time_loads(v_tl_idx) := tl_rec.time_load;
                    END LOOP;
                    
                    -- Build table header with time load columns
                    APPEND_HTML('<table style="font-size: 11px;">' || v_lf);
                    APPEND_HTML('<thead><tr>' || v_lf);
                    APPEND_HTML('<th style="position: sticky; left: 0; background-color: #f0f0f0; z-index: 10;">Column Name</th>' || v_lf);
                    
                    -- Add time load columns
                    FOR i IN 1..v_time_loads.COUNT LOOP
                        APPEND_HTML('<th>TL ' || v_time_loads(i) || '</th>' || v_lf);
                    END LOOP;
                    
                    APPEND_HTML('<th>% Change<br/>(Last)</th>' || v_lf);
                    APPEND_HTML('<th>% Change<br/>(12-Month Avg)</th>' || v_lf);
                    APPEND_HTML('<th>Status</th>' || v_lf);
                    APPEND_HTML('</tr></thead>' || v_lf);
                    APPEND_HTML('<tbody>' || v_lf);
                    
                    -- Get all columns for this table
                    FOR col_rec IN (
                        SELECT DISTINCT column_name
                        FROM TBL_MDC_VALIDATION_RESULTS
                        WHERE email_address = p_email_address
                          AND TRUNC(run_date) = TRUNC(p_run_date)
                          AND validation_type = 'COLUMN_COUNT'
                          AND owner = table_rec.owner
                          AND table_name = table_rec.table_name
                        ORDER BY column_name
                    ) LOOP
                        -- Get the latest status for this column (for row coloring)
                        SELECT MAX(status)
                        INTO v_max_status
                        FROM TBL_MDC_VALIDATION_RESULTS
                        WHERE email_address = p_email_address
                          AND TRUNC(run_date) = TRUNC(p_run_date)
                          AND validation_type = 'COLUMN_COUNT'
                          AND owner = table_rec.owner
                          AND table_name = table_rec.table_name
                          AND column_name = col_rec.column_name;
                        
                        v_bg_color := GET_COLOR_CODE(NULL, v_max_status);
                        APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                        APPEND_HTML('<td style="position: sticky; left: 0; background-color: ' || v_bg_color || '; font-weight: bold;">' || col_rec.column_name || '</td>' || v_lf);
                        
                        -- Add count values for each time load
                        FOR i IN 1..v_time_loads.COUNT LOOP
                            DECLARE
                                v_count NUMBER;
                            BEGIN
                                SELECT count_value
                                INTO v_count
                                FROM TBL_MDC_VALIDATION_RESULTS
                                WHERE email_address = p_email_address
                                  AND TRUNC(run_date) = TRUNC(p_run_date)
                                  AND validation_type = 'COLUMN_COUNT'
                                  AND owner = table_rec.owner
                                  AND table_name = table_rec.table_name
                                  AND column_name = col_rec.column_name
                                  AND time_load = v_time_loads(i);
                                
                                APPEND_HTML('<td>' || TO_CHAR(v_count) || '</td>' || v_lf);
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    APPEND_HTML('<td>-</td>' || v_lf);
                            END;
                        END LOOP;
                        
                        -- Get latest % change values (from most recent time load)
                        DECLARE
                            v_pct_change NUMBER;
                            v_pct_change_12month NUMBER;
                            v_status VARCHAR2(20);
                        BEGIN
                            SELECT pct_change, pct_change_12month, status
                            INTO v_pct_change, v_pct_change_12month, v_status
                            FROM TBL_MDC_VALIDATION_RESULTS
                            WHERE email_address = p_email_address
                              AND TRUNC(run_date) = TRUNC(p_run_date)
                              AND validation_type = 'COLUMN_COUNT'
                              AND owner = table_rec.owner
                              AND table_name = table_rec.table_name
                              AND column_name = col_rec.column_name
                              AND time_load = (SELECT MAX(time_load) 
                                              FROM TBL_MDC_VALIDATION_RESULTS
                                              WHERE email_address = p_email_address
                                                AND TRUNC(run_date) = TRUNC(p_run_date)
                                                AND validation_type = 'COLUMN_COUNT'
                                                AND owner = table_rec.owner
                                                AND table_name = table_rec.table_name
                                                AND column_name = col_rec.column_name);
                            
                            APPEND_HTML('<td>' || NVL(TO_CHAR(v_pct_change, '999.99'), '-') || '%</td>' || v_lf);
                            APPEND_HTML('<td>' || NVL(TO_CHAR(v_pct_change_12month, '999.99'), '-') || '%</td>' || v_lf);
                            APPEND_HTML('<td>' || NVL(v_status, '-') || '</td>' || v_lf);
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                APPEND_HTML('<td>-</td>' || v_lf);
                                APPEND_HTML('<td>-</td>' || v_lf);
                                APPEND_HTML('<td>-</td>' || v_lf);
                        END;
                        
                        APPEND_HTML('</tr>' || v_lf);
                    END LOOP;
                    
                    APPEND_HTML('</tbody></table>' || v_lf);
                END;
            END LOOP;
            
        ELSIF p_validation_type = 'TABLE_COMPARE' THEN
            APPEND_HTML('<h2>Table Comparison Validations</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Compare To</th><th>Time Load</th><th>Count</th><th>Compare Count</th><th>Match Status</th><th>Status</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
            FOR comp_rec IN (
                SELECT owner, table_name, compare_table, time_load, count_value, compare_count,
                       match_status, status, severity
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'TABLE_COMPARE'
                ORDER BY owner, table_name, time_load
            ) LOOP
                v_bg_color := GET_COLOR_CODE(NULL, comp_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || comp_rec.owner || '.' || comp_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(comp_rec.compare_table, '-') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(comp_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || TO_CHAR(comp_rec.count_value) || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(comp_rec.compare_count), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(comp_rec.match_status, '-') || '</td>' || v_lf);
                APPEND_HTML('<td>' || comp_rec.status || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
            
        ELSIF p_validation_type = 'DISTINCT_COUNT' THEN
            APPEND_HTML('<h2>Distinct Count Validations</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Column</th><th>Time Load</th><th>Distinct Count</th><th>Prior Count</th><th>% Change</th><th>Status</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
            FOR dist_rec IN (
                SELECT owner, table_name, column_name, time_load, count_value, prior_count,
                       pct_change, status, severity
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'DISTINCT_COUNT'
                ORDER BY owner, table_name, column_name, time_load
            ) LOOP
                v_bg_color := GET_COLOR_CODE(dist_rec.pct_change, dist_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || dist_rec.owner || '.' || dist_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(dist_rec.column_name, '-') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(dist_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || TO_CHAR(dist_rec.count_value) || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(dist_rec.prior_count), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(dist_rec.pct_change, '999.99'), 'N/A') || '%</td>' || v_lf);
                APPEND_HTML('<td>' || dist_rec.status || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
            
        ELSIF p_validation_type = 'DATA_VALIDATION' THEN
            APPEND_HTML('<h2>Data Validation Checks</h2>' || v_lf);
            APPEND_HTML('<table>' || v_lf);
            APPEND_HTML('<thead><tr><th>Table</th><th>Time Load</th><th>Mismatch Count</th><th>Status</th><th>Message</th></tr></thead>' || v_lf);
            APPEND_HTML('<tbody>' || v_lf);
            
            FOR data_rec IN (
                SELECT owner, table_name, time_load, count_value, status, severity, message
                FROM TBL_MDC_VALIDATION_RESULTS
                WHERE email_address = p_email_address
                  AND TRUNC(run_date) = TRUNC(p_run_date)
                  AND validation_type = 'DATA_VALIDATION'
                ORDER BY owner, table_name, time_load
            ) LOOP
                v_bg_color := GET_COLOR_CODE(NULL, data_rec.status);
                APPEND_HTML('<tr style="background-color: ' || v_bg_color || ';">' || v_lf);
                APPEND_HTML('<td>' || data_rec.owner || '.' || data_rec.table_name || '</td>' || v_lf);
                APPEND_HTML('<td>' || NVL(TO_CHAR(data_rec.time_load), 'N/A') || '</td>' || v_lf);
                APPEND_HTML('<td>' || TO_CHAR(data_rec.count_value) || '</td>' || v_lf);
                APPEND_HTML('<td>' || data_rec.status || '</td>' || v_lf);
                APPEND_HTML('<td>' || SUBSTR(REPLACE(REPLACE(NVL(data_rec.message, ''), '<', '&lt;'), '>', '&gt;'), 1, 200) || '</td>' || v_lf);
                APPEND_HTML('</tr>' || v_lf);
            END LOOP;
            
            APPEND_HTML('</tbody></table>' || v_lf);
        END IF;
        
        -- Footer
        APPEND_HTML('<hr>' || v_lf);
        APPEND_HTML('<p><em>This is an automated email from the MDC Validation System.</em></p>' || v_lf);
        IF p_total_parts > 1 THEN
            APPEND_HTML('<p><em>This is part ' || p_part_number || ' of ' || p_total_parts || ' emails for this validation run.</em></p>' || v_lf);
        END IF;
        APPEND_HTML('<p><em>Generated on ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || '</em></p>' || v_lf);
        APPEND_HTML('</body>' || v_lf);
        APPEND_HTML('</html>' || v_lf);
        
        p_html_content := v_html;
        
    EXCEPTION
        WHEN OTHERS THEN
            IF v_html IS NOT NULL AND DBMS_LOB.ISTEMPORARY(v_html) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_html);
            END IF;
            DBMS_OUTPUT.PUT_LINE('ERROR in GENERATE_EMAIL_CONTENT_BY_TYPE: ' || SQLERRM);
            p_html_content := NULL;
            RAISE;
    END GENERATE_EMAIL_CONTENT_BY_TYPE;
    
    -- ========================================================================
    -- Utility Procedures
    -- ========================================================================
    
    FUNCTION GET_DATABASE_NAME RETURN VARCHAR2 IS
        -- Hardcoded database name - change 'QA' to 'MAIN' for production
        -- Or set based on environment variable/config if available
        v_db_name VARCHAR2(50) := 'QA'; -- Change to 'MAIN' for production
    BEGIN
        -- You can override this by checking environment or config table if needed
        -- For now, hardcoded: 'QA' for QA environment, 'MAIN' for production
        RETURN v_db_name;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'UNKNOWN';
    END GET_DATABASE_NAME;
    
    FUNCTION VALIDATE_TABLE_EXISTS(
        p_owner      IN VARCHAR2,
        p_table_name IN VARCHAR2
    ) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM all_tables
        WHERE owner = p_owner
          AND table_name = p_table_name;
        
        RETURN v_count > 0;
    END VALIDATE_TABLE_EXISTS;
    
    FUNCTION GET_LOAD_PROCESS_COLUMN(
        p_owner      IN VARCHAR2,
        p_table_name IN VARCHAR2
    ) RETURN VARCHAR2 IS
    BEGIN
        -- Handle special cases for different load process column names
        IF p_table_name = 'INV_SUM' THEN
            RETURN 'dt_inv_load_process';
        ELSIF p_table_name = 'INV_AFC_FACT' THEN
            RETURN 'dt_inv_afc_load_process';
        ELSIF p_table_name = 'INV_APS_FACT' THEN
            RETURN 'dt_inv_aps_load_process';
        ELSIF p_table_name = 'INV_CPS_FACT' THEN
            RETURN 'dt_inv_cps_load_process';
        ELSIF p_table_name = 'INV_LIC_FACT' THEN
            RETURN 'dt_inv_lic_load_process';
        ELSIF p_table_name = 'PP_LEGAL_CUST_FACT' THEN
            RETURN 'dt_pp_lcust_load_process';
        ELSE
            -- Default: dt_load_process
            RETURN 'dt_load_process';
        END IF;
    END GET_LOAD_PROCESS_COLUMN;
    
    PROCEDURE CLEAR_RESULTS(
        p_run_date IN DATE DEFAULT NULL
    ) IS
    BEGIN
        IF p_run_date IS NULL THEN
            DELETE FROM TBL_MDC_VALIDATION_RESULTS;
        ELSE
            DELETE FROM TBL_MDC_VALIDATION_RESULTS
            WHERE TRUNC(RUN_DATE) = TRUNC(p_run_date);
        END IF;
        
        COMMIT;
    END CLEAR_RESULTS;

END PKG_MDC_VALIDATION;
/