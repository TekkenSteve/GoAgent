-- Historical: this is the template's first migration (a translation history
-- table from the project this repository was generated from). It is kept, with
-- its version unchanged, because a database that already applied it can only
-- migrate down through the same file list. It is not part of this project's
-- schema; nothing references it.
CREATE TABLE IF NOT EXISTS history(
    id serial PRIMARY KEY,
    source VARCHAR(255),
    destination VARCHAR(255),
    original VARCHAR(255),
    translation VARCHAR(255)
);
