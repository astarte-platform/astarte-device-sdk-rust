SELECT
    retention_publish.t_millis,
    retention_publish.counter,
    retention_publish.interface,
    retention_publish.path,
    retention_publish.sent,
    retention_publish.payload,
    retention_mapping.reliability,
    retention_mapping.major_version,
    retention_mapping.expiry_sec
FROM retention_publish
INNER JOIN retention_mapping USING (interface, path)
WHERE
    retention_publish.sent = FALSE
    AND (
        retention_publish.expiry_t_secs IS NULL
        OR retention_publish.expiry_t_secs >= ?
    )
ORDER BY t_millis ASC, counter ASC
LIMIT ?;
