-- Function to notify about table changes
CREATE OR REPLACE FUNCTION notify_table_changes()
RETURNS TRIGGER AS $$
DECLARE
    data json;
    notification json;
BEGIN
    -- Handle different operations
    IF TG_OP = 'DELETE' THEN
        data = row_to_json(OLD);
    ELSE
        data = row_to_json(NEW);
    END IF;
    
    -- Build notification payload
    notification = json_build_object(
        'table', TG_TABLE_NAME,
        'operation', TG_OP,
        'data', data,
        'timestamp', extract(epoch from now())
    );
    
    -- Send notification
    PERFORM pg_notify('table_changes', notification::text);
    
    -- Return appropriate row
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER your_table_changes_trigger
    AFTER INSERT OR UPDATE OR DELETE
    ON Accounts
    FOR EACH ROW
    EXECUTE FUNCTION notify_table_changes();

DO $$
DECLARE
    table_name text;
BEGIN
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename NOT LIKE 'pg_%'
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS %I_changes_trigger ON %I;
            CREATE TRIGGER %I_changes_trigger
                AFTER INSERT OR UPDATE OR DELETE
                ON %I
                FOR EACH ROW
                EXECUTE FUNCTION notify_table_changes();',
            table_name, table_name, table_name, table_name);
    END LOOP;
END $$;
