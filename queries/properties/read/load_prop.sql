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
    interface = ?
    AND path = ?
    AND value IS NOT NULL;
