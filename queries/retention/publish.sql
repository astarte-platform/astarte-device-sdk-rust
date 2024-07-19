SELECT
    t_millis,
    counter AS "counter: u32",
    path,
    expiry_t_millis,
    payload
FROM retention_publish
WHERE t_millis = ? AND counter = ?;
