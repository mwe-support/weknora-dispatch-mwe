DO $$ BEGIN RAISE EXCEPTION 'Artifact references protect live outputs. Use a lifecycle-compatible binary rollback; retain this schema.'; END $$;
