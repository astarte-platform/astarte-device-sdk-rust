SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership
FROM propcache
WHERE
    value IS NOT NULL;
