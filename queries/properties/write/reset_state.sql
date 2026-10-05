UPDATE propcache
SET
    state = ?1,
    epoch = 0
WHERE
    ownership = ?2;
