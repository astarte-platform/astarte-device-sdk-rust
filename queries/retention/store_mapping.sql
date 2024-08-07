INSERT OR REPLACE INTO retention_mapping (
    interface,
    path,
    major_version,
    reliability,
    expiry_sec
) VALUES (?, ?, ?, ?, ?)
