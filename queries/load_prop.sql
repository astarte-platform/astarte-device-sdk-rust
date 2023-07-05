SELECT
    value,
    interface_major AS "interface_major: i32"
FROM propcache WHERE interface = ? AND path = ?
