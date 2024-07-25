SELECT
    interface,
    path,
    major_version AS "major_version: i32",
    reliability AS "qos: u8",
    expiry_sec
FROM retention_mapping
WHERE interface = ? AND path = ?
