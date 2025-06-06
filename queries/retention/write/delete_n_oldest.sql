DELETE FROM retention_publish
WHERE (t_millis, counter) IN (
    SELECT t_millis, counter
    FROM retention_publish
    ORDER BY t_millis ASC, counter ASC
    LIMIT ?
);
