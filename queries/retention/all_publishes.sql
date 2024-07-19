SELECT
    retention_publish.t_millis,
    retention_publish.counter AS "counter: u32",
    retention_publish.path,
    retention_publish.payload,
    retention_mapping.reliability AS "reliability: u8",
    retention_mapping.major_version AS "major_version: i32",
    retention_mapping.expiry_sec
FROM retention_publish
INNER JOIN retention_mapping USING (path)
ORDER BY t_millis ASC, counter ASC
