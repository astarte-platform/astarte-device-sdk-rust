SELECT NOT EXISTS (SELECT 1 from retentionmessage) AS 'is_empty!: bool'
