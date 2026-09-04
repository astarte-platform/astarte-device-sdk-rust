UPDATE propcache
SET
    value = NULL,
    state = ?1,
    epoch = propcache.epoch + 1,
    updated_at = ?2,
    updated_at_nanos = ?3,
    updated_at_counter = ?4
WHERE
    interface = ?5
    AND path = ?6
    AND (value IS NOT NULL OR interface_major != ?7)
RETURNING epoch;
