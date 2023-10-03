SELECT
    interface,
    path,
    value,
    type AS 'stored_type: u8',
    interface_major AS 'interface_major: i32'
FROM propcache
