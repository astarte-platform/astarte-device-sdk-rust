SELECT
    interface,
    path,
    major_version,
    reliability,
    expiry_sec
FROM retention_mapping
WHERE
    interface = ?
    AND path = ?
