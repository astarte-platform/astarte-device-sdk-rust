SELECT
    t_millis,
    counter,
    interface,
    path,
    expiry_t_secs,
    sent,
    payload
FROM retention_publish
WHERE
    t_millis = ?
    AND counter = ?;
