DELETE FROM propcache
WHERE
    interface = ?
    AND path = ?
    AND interface_major = ?;
