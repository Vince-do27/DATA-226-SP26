SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_BOA.RAW.user_session_channel
WHERE sessionId IS NOT NULL