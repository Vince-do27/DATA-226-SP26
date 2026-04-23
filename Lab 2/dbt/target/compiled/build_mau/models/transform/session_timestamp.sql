SELECT
    sessionId,
    ts
FROM USER_DB_BOA.RAW.session_timestamp
WHERE sessionId IS NOT NULL