-- Deliberately irreversible while processing protocol 2 history exists.
-- Roll back the application to a protocol-2-compatible image; preserve the ledger.
CREATE TEMP TABLE processing_rollback_guard (value INTEGER);
CREATE TEMP TRIGGER processing_rollback_refused BEFORE INSERT ON processing_rollback_guard
BEGIN SELECT RAISE(ABORT, 'processing lifecycle history must be retained'); END;
INSERT INTO processing_rollback_guard VALUES (0);
