INSERT INTO propcache (
    interface,
    path,
    value,
    type,
    interface_major,
    ownership,
    state,
    updated_at,
    updated_at_nanos,
    updated_at_counter
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
ON CONFLICT (interface, path) DO UPDATE SET
    epoch = propcache.epoch + 1,
    value = excluded.value,
    state = 0,
    interface_major = ?5,
    updated_at = ?8,
    updated_at_nanos = ?9,
    updated_at_counter = ?10
WHERE
propcache.value IS NOT excluded.value
OR propcache.interface_major != excluded.interface_major
RETURNING epoch;
