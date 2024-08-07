SELECT
    t_millis,
    counter AS "counter: u32",
    interface,
    path,
    expiry_t_millis,
    sent,
    payload
FROM retention_publish
WHERE t_millis = ? AND counter = ?;
