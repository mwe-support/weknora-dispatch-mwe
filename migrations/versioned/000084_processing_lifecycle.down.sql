DO $$ BEGIN RAISE EXCEPTION 'Restore a verified backup to roll back graph contribution tracking; destructive down migration refused'; END $$;
