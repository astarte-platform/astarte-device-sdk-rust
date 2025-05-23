DELETE FROM retention_publish
WHERE (t_millis, counter) IN (
    SELECT t_millis, counter
    FROM retention_publish
    LIMIT ?
);
