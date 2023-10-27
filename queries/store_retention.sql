INSERT OR
REPLACE INTO retentionmessage (id,
                               expiry,
                               payload,
                               reliability,
                               topic)
VALUES (?,?, ?, ?, ?)
