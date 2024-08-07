CREATE TABLE IF NOT EXISTS propcache (
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    value BLOB NOT NULL,
    type INTEGER NOT NULL,
    interface_major INTEGER NOT NULL,
    -- Ownership of the interface
    -- 0: Server owned
    -- 1: Device owned
    ownership INTEGER NOT NULL,
    PRIMARY KEY (interface, path)
)
