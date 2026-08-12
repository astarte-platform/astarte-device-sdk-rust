UPDATE propcache
SET
    state = ?1,
    epoch = ?2
WHERE
    ownership = 0
    AND interface = ?3
    AND path = ?4
    AND epoch = ?5;
