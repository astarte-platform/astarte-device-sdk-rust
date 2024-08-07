UPDATE retention_publish
SET
    sent = ?
WHERE
    t_millis = ? AND counter = ?;
