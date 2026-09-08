UPDATE propcache
SET
    state = ?1
WHERE
    ownership = 0
    AND interface = ?2
    AND path = ?3
    AND epoch = ?4;
