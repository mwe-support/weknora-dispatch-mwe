DROP TABLE IF EXISTS processing_wiki_writes;
ALTER TABLE wiki_pages DROP COLUMN IF EXISTS mutation_revision;
