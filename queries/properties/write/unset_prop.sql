UPDATE propcache
SET value = NULL
WHERE
    interface = ?
    AND path = ?;
