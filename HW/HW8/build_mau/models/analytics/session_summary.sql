
SELECT u.*, s.ts
FROM {{ ref("user_session_channel") }} u 
JOIN {{ ref("session_timestamp") }} s ON u.sessionId = s.sessionId
