<!--
This file is part of Astarte.

Copyright 2026 SECO Mind Srl

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

# SQLite

The SDK requires a modern SQLite version, with:

- `RETURNING` support present since version 3.35.0 (2021-03-12)[^1].

[^1]: https://sqlite.org/lang_returning.html

You can use the vendored version of SQLite with the `vendored` feature.

## Database Schema

### Properties

```sql
-- Properties
CREATE TABLE IF NOT EXISTS propcache (
    interface TEXT NOT NULL,
    path TEXT NOT NULL,
    -- Nullable for unset
    value BLOB,
    type INTEGER NOT NULL,
    interface_major INTEGER NOT NULL,
    -- Ownership of the interface
    -- 0: Device owned
    -- 1: Server owned
    ownership INTEGER NOT NULL,
    -- State of the property when sending
    -- 0: Changed
    -- 1: Completed
    state INTEGER NOT NULL DEFAULT 0,
    -- Version of the change to keep track and update the state
    epoch INTEGER NOT NULL DEFAULT 0,
    -- Timestamp of when the property was updated
    updated_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Sub nanosec of the timestamp
    updated_at_nanos INTEGER NOT NULL DEFAULT 0,
    -- Counter to guarantee ordering and uniqueness
    updated_at_counter INTEGER NOT NULL,
    UNIQUE (updated_at, updated_at_nanos, updated_at_counter),
    PRIMARY KEY (interface, path)
);

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

CREATE TABLE IF NOT EXISTS introspection (
    "name" TEXT NOT NULL PRIMARY KEY,
    "major" INTEGER NOT NULL,
    "minor" INTEGER NOT NULL
)
```
