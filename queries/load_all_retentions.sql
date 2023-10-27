SELECT  id,
        expiry ,
        payload,
        reliability AS 'reliability: u8',
        topic
FROM retentionmessage
