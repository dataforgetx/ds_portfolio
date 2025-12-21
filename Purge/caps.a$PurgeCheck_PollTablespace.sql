------------------------------------------------------------------------------------------
-- Filename: caps.a$PurgeCheck_PollTablespace.sql
--
-- Purpose: Function to check if DBA has opened or closed a tablespace episode.
--          Used by automated purge fix procedures to poll for DBA approval.
--
------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION CAPS.a$PurgeCheck_PollTablespace(
    p_episode_id        IN NUMBER,
    p_expected_status   IN VARCHAR2  -- 'OPEN' or 'CLOSED'
) RETURN BOOLEAN IS
-----------------------------------------------------------------------------
-- Parameters:
--   p_episode_id       - Episode ID from ram.a$rectifydb_log_master
--   p_expected_status  - Expected status: 'OPEN' or 'CLOSED'
--
-- Returns:
--   TRUE if tablespace is in expected status, FALSE otherwise
--
-- Purpose:
--   Polls ram.a$rectifydb_log_master to check if DBA has opened or closed
--   the tablespace for the given episode.
--
-- Status Logic:
--   OPEN:  episode_ts_open IS NOT NULL (DBA has opened tablespace)
--   CLOSED: episode_ts_close IS NOT NULL AND episode_status = 'CLOSED'
-----------------------------------------------------------------------------

    v_ts_open       DATE;
    v_ts_close      DATE;
    v_status        VARCHAR2(20);
    v_episode_close DATE;
    
BEGIN
    
    -- Validate inputs
    IF p_episode_id IS NULL OR p_expected_status IS NULL THEN
        RETURN FALSE;
    END IF;
    
    IF p_expected_status NOT IN ('OPEN', 'CLOSED') THEN
        RETURN FALSE;
    END IF;
    
    -- Query episode status from ram.a$rectifydb_log_master
    BEGIN
        SELECT episode_ts_open,
               episode_ts_close,
               episode_status,
               episode_close
        INTO   v_ts_open,
               v_ts_close,
               v_status,
               v_episode_close
        FROM   ram.a$rectifydb_log_master
        WHERE  id = p_episode_id;
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Episode not found
            RETURN FALSE;
        WHEN OTHERS THEN
            -- Error querying - log and return FALSE
            post_a$log('PurgeCheck_PollTablespace', 
                'ERROR querying episode '||p_episode_id||': '||SQLERRM);
            RETURN FALSE;
    END;
    
    -- Check for OPEN status
    IF p_expected_status = 'OPEN' THEN
        -- Tablespace is open if episode_ts_open is not NULL
        -- This means DBA has opened it
        IF v_ts_open IS NOT NULL THEN
            RETURN TRUE;
        ELSE
            -- Still waiting for DBA to open
            RETURN FALSE;
        END IF;
    END IF;
    
    -- Check for CLOSED status
    IF p_expected_status = 'CLOSED' THEN
        -- Tablespace is closed if:
        -- 1. episode_ts_close is not NULL (DBA has closed it)
        -- 2. episode_status = 'CLOSED' (confirmed closed)
        -- 3. episode_close is not NULL (episode is fully closed)
        IF v_ts_close IS NOT NULL 
           AND v_status = 'CLOSED' 
           AND v_episode_close IS NOT NULL THEN
            RETURN TRUE;
        ELSE
            -- Still waiting for DBA to close
            RETURN FALSE;
        END IF;
    END IF;
    
    -- Should not reach here, but return FALSE as safe default
    RETURN FALSE;
    
END a$PurgeCheck_PollTablespace;
/

-- Add comment
COMMENT ON FUNCTION CAPS.a$PurgeCheck_PollTablespace IS 
    'Polls ram.a$rectifydb_log_master to check if DBA has opened or closed tablespace for given episode. Returns TRUE if in expected status.';

-- Grant execute permission (adjust as needed)
-- GRANT EXECUTE ON CAPS.a$PurgeCheck_PollTablespace TO <user>;

PROMPT Function CAPS.a$PurgeCheck_PollTablespace created successfully

