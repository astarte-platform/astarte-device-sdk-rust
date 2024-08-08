SELECT
    value,
    type,
    interface_major
FROM propcache
WHERE
    interface = ?
    AND path = ?;
