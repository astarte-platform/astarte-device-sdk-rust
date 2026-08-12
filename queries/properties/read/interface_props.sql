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
    AND value IS NOT NULL
    AND (
        ?2 IS NULL
        OR (updated_at, updated_at_nanos, updated_at_counter) > (?2, ?3, ?4)
    )
ORDER BY updated_at, updated_at_nanos, updated_at_counter, interface, path
LIMIT ?5;
