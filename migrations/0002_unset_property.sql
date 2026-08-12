CREATE TABLE IF NOT EXISTS propcache_v2 (
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    value BLOB,
    type INTEGER NOT NULL,
    interface_major INTEGER NOT NULL,
    ownership INTEGER NOT NULL,
    PRIMARY KEY (interface, path)
);

INSERT OR REPLACE INTO propcache_v2 (
    interface, path, value, type, interface_major, ownership
)
SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership
FROM propcache;

DROP TABLE propcache;

ALTER TABLE propcache_v2 RENAME TO propcache;
