INSERT OR FAIL INTO retention_publish (
    t_millis,
    counter,
    path,
    expiry_t_millis,
    payload
) VALUES (?, ?, ?, ?, ?);
