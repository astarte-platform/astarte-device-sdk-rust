CREATE TABLE IF NOT EXISTS propcache (
    interface TEXT NOT NULl,
    path TEXT NOT NULL,
    value BLOB NOT NULL,
    interface_major INTEGER NOT NULL,
    PRIMARY KEY (interface, path)
)
