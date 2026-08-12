SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership,
    epoch,
    updated_at,
    updated_at_nanos,
    updated_at_counter
FROM propcache
WHERE
    interface = ?1
    AND interface_major = ?2
    AND value IS NOT NULL
    AND (
        ?3 IS NULL
        OR (updated_at, updated_at_nanos, updated_at_counter) > (?3, ?4, ?5)
    )
ORDER BY updated_at, updated_at_nanos, updated_at_counter, interface, path
LIMIT ?6;
