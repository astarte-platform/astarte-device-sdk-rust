DELETE FROM retention_publish
WHERE expiry_t_millis < ?;
