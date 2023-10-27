DELETE
FROM retentionmessage
where ROWID IN (SELECT ROWID FROM retentionmessage LIMIT 1);
