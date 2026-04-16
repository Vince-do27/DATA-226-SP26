
  create or replace   view USER_DB_BOA.analytics.user_session_channel
  
  
  
  
  as (
    SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_BOA.RAW.user_session_channel
WHERE sessionId IS NOT NULL
  );

