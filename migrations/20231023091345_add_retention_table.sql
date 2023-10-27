-- Add migration script here
create table retentionmessage
(
    id      INTEGER PRIMARY KEY,
    expiry  INTEGER DEFAULT 0 NOT NULL ,
    payload BLOB              NOT NULL ,
    reliability     INTEGER DEFAULT 0 NOT NULL ,
    topic   TEXT              NOT NULL
);
-- Add migration script here
