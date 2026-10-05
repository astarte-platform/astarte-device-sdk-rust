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
    ownership = ?1
    AND state = ?2
    AND (
        ?3 IS NULL
        OR (updated_at, updated_at_nanos, updated_at_counter) > (?3, ?4, ?5)
    )
ORDER BY updated_at, updated_at_nanos, updated_at_counter, interface, path
LIMIT ?6;
