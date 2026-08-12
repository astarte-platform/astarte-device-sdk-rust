DELETE FROM propcache
WHERE
    ownership = 0
    AND interface = ?
    AND path = ?
    AND epoch = ?;
