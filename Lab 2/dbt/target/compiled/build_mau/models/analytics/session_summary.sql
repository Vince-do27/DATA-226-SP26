with __dbt__cte__user_session_channel as (
SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_BOA.RAW.user_session_channel
WHERE sessionId IS NOT NULL
),  __dbt__cte__session_timestamp as (
SELECT
    sessionId,
    ts
FROM USER_DB_BOA.RAW.session_timestamp
WHERE sessionId IS NOT NULL
) SELECT u.*, s.ts
FROM __dbt__cte__user_session_channel u 
JOIN __dbt__cte__session_timestamp s ON u.sessionId = s.sessionId