DO $$ BEGIN RAISE EXCEPTION 'Restore a verified backup to roll back processing storage accounting; destructive down migration refused'; END $$;
