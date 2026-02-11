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
ORDER BY interface, path
LIMIT ? OFFSET ?;
