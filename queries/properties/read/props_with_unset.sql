SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership
FROM propcache
WHERE
    ownership = ?
    AND state = ?
ORDER BY interface, path
LIMIT ? OFFSET ?;
