-- Interface stored retention
CREATE TABLE IF NOT EXISTS retention_mapping (
    -- Interface name
    interface TEXT NOT NULL,
    -- Interface path where the data was published on.
    path TEXT NOT NULL,
    -- Version of the interface the data was published on.
    major_version INTEGER NOT NULL,
    -- Quality of service
    reliability INTEGER NOT NULL,
    -- Seconds after the entry will expire
    expiry_sec INTEGER,
    PRIMARY KEY (interface, path)
);


-- Payload for the stored interfaces
CREATE TABLE IF NOT EXISTS retention_publish (
    -- Timestamp as u128 milliseconds since the Unix epoch, used for packet order
    t_millis BLOB NOT NULL,
    --- Counter for same milliseconds packets
    counter INTEGER NOT NULL,
    -- Interface name
    interface TEXT NOT NULL,
    --- interface path
    path TEXT NOT NULL,
    -- Timestamp as u64 milliseconds since the Unix epoch, when the publish expires
    -- (t_millis as secs + expiry_sec).
    expiry_t_secs BLOB,
    --  Whether the publish was sent or stored when offline.
    sent BOOLEAN NOT NULL,
    -- Payload for the packet
    payload BLOB NOT NULL,
    -- Primary key for packet uniqueness and ordering the table ordering
    PRIMARY KEY (t_millis, counter),
    -- References to the retention information
    FOREIGN KEY (interface, path) REFERENCES retention_mapping (interface, path)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);
