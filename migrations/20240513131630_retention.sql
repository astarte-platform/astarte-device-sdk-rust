-- Interface stored retention
CREATE TABLE IF NOT EXISTS retention_mapping (
    -- Interface path where the data was published on.
    path TEXT NOT NULL PRIMARY KEY,
    -- Version of the interface the data was published on.
    major_version INTEGER NOT NULL,
    -- Quality of service
    reliability INTEGER NOT NULL,
    -- Seconds after the entry will expire
    expiry_sec INTEGER
);


-- Interface stored retention
CREATE TABLE IF NOT EXISTS retention_publish (
    -- Timestamp as u128 milliseconds since the Unix epoch, used for packet order
    t_millis BLOB NOT NULL,
    --- Counter for same milliseconds packets
    counter INTEGER NOT NULL,
    --- path of the packet
    path TEXT NOT NULL,
    -- Timestamp as u128 milliseconds since the Unix epoch, when the publish expires
    -- (t_millis + expiry_sec).
    expiry_t_millis BLOB,
    -- Payload for the packet
    payload BLOB NOT NULL,
    -- Primary key for packet uniqueness and ordering the table ordering
    PRIMARY KEY (t_millis, counter),
    -- References to the retention information
    FOREIGN KEY (path) REFERENCES retention_mapping (
        path
    ) ON UPDATE CASCADE ON DELETE CASCADE
);
