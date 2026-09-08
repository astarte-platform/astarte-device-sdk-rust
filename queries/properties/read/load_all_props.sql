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
    value IS NOT NULL
    AND (
        ?1 IS NULL
        OR (updated_at, updated_at_nanos, updated_at_counter) > (?1, ?2, ?3)
    )
ORDER BY updated_at, updated_at_nanos, updated_at_counter, interface, path
LIMIT ?4;
