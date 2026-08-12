CREATE TABLE IF NOT EXISTS propcache_v2 (
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    value BLOB,
    type INTEGER NOT NULL,
    interface_major INTEGER NOT NULL,
    ownership INTEGER NOT NULL,
    state INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    updated_at_nanos INTEGER NOT NULL DEFAULT 0,
    updated_at_counter INTEGER NOT NULL,
    UNIQUE (updated_at, updated_at_nanos, updated_at_counter),
    PRIMARY KEY (interface, path)
) STRICT;
INSERT INTO propcache_v2 (
    interface,
    path,
    value,
    type,
    interface_major,
    ownership,
    updated_at,
    updated_at_counter
)
SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership,
    unixepoch(),
    row_number() OVER (ORDER BY interface, path) - 1 AS updated_at_counter
FROM propcache;
DROP TABLE propcache;
ALTER TABLE propcache_v2 RENAME TO propcache;
