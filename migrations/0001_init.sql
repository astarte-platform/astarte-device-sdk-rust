CREATE TABLE IF NOT EXISTS propcache (
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    value BLOB NOT NULL,
    type INTEGER NOT NULL,
    interface_major INTEGER NOT NULL,
    ownership INTEGER NOT NULL,
    PRIMARY KEY (interface, path)
);

CREATE TABLE IF NOT EXISTS retention_mapping (
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    major_version INTEGER NOT NULL,
    reliability INTEGER NOT NULL,
    expiry_sec INTEGER,
    PRIMARY KEY (interface, path)
);

CREATE TABLE IF NOT EXISTS retention_publish (
    t_millis BLOB NOT NULL,
    counter INTEGER NOT NULL,
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    expiry_t_secs BLOB,
    sent BOOLEAN NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (t_millis, counter),
    FOREIGN KEY (interface, path) REFERENCES retention_mapping (interface, path)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);
