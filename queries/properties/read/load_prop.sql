SELECT
    value,
    type,
    interface_major
FROM propcache
WHERE
    interface = ?
    AND path = ?
    AND value IS NOT NULL;
