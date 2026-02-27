UPDATE propcache
SET state = ?
WHERE
    interface = ?
    AND path = ?;
