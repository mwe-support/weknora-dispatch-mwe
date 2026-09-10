-- Deliberately irreversible while processing protocol 2 history exists.
-- Roll back the application to a protocol-2-compatible image; preserve the ledger.
DO $$ BEGIN RAISE EXCEPTION 'processing lifecycle history must be retained; use a protocol-2-compatible rollback'; END $$;
