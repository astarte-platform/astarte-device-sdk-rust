DELETE FROM retention_publish
WHERE
    t_millis = ?
    AND counter = ?;
