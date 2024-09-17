SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership
FROM propcache
WHERE
    interface = ?
    AND value IS NOT NULL;
