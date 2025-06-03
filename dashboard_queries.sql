-- 1. Total Tests Started

-- Metric: Count of distinct test attempts that were started.


SELECT
    DATE(start_timestamp) AS attempt_date,
    COUNT(DISTINCT attempt_id) AS tests_started
FROM
    `varta.idfy_assignment.fact_test_attempts`
WHERE
    start_timestamp IS NOT NULL
    AND DATE(start_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
GROUP BY
    attempt_date
ORDER BY
    attempt_date;


-- 2. Total Tests Completed

-- Metric: Count of distinct test attempts that were completed.



SELECT
    DATE(end_timestamp) AS completion_date,
    COUNT(DISTINCT attempt_id) AS tests_completed
FROM
    `varta.idfy_assignment.fact_test_attempts`
WHERE
    status = 'COMPLETED'
    AND DATE(end_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
GROUP BY
    completion_date
ORDER BY
    completion_date;


--3) Test not completed and reason for it

SELECT
    status,
    COUNT(DISTINCT attempt_id) AS count_of_tests
FROM
    `varta.idfy_assignment.fact_test_attempts`
WHERE
    status != 'COMPLETED'
    AND DATE(start_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
GROUP BY
    status
ORDER BY
    count_of_tests DESC;  



--4) Session and question level stats

SELECT
    COUNT(DISTINCT session_id) AS total_sessions,
    AVG(session_duration_seconds) AS average_session_duration_seconds,
    SUM(CASE WHEN ended_due_to_ttl THEN 1 ELSE 0 END) AS sessions_ended_by_ttl
FROM
    `varta.idfy_assignment.fact_sessions`
WHERE
    session_start_timestamp IS NOT NULL
    AND DATE(session_start_timestamp) BETWEEN '2025-06-01' AND '2025-06-30';


-- Most Time-Consuming Questions
SELECT
    fqs.question_id,
    dq.question_text,
    AVG(fqs.time_on_question_ms) AS average_time_on_question_ms
FROM
    `varta.idfy_assignment.fact_question_submissions` AS fqs
JOIN
    `varta.idfy_assignment.dim_questions` AS dq
ON
    fqs.question_id = dq.question_id AND fqs.test_id = dq.test_id
WHERE
    fqs.time_on_question_ms IS NOT NULL
    AND DATE(fqs.submission_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
    -- AND fqs.test_id = 'testA' -- Uncomment and specify if filtering for a single test
GROUP BY
    fqs.question_id, dq.question_text
ORDER BY
    average_time_on_question_ms DESC
LIMIT 10;

-- Mostly Answered Correctly Questions
SELECT
    fqs.question_id,
    dq.question_text,
    COUNT(fqs.submission_id) AS total_submissions,
    SUM(CASE WHEN fqs.is_correct THEN 1 ELSE 0 END) AS correct_submissions,
    SAFE_DIVIDE(SUM(CASE WHEN fqs.is_correct THEN 1 ELSE 0 END), COUNT(fqs.submission_id)) AS correct_rate
FROM
    `varta.idfy_assignment.fact_question_submissions` AS fqs
JOIN
    `varta.idfy_assignment.dim_questions` AS dq
ON
    fqs.question_id = dq.question_id AND fqs.test_id = dq.test_id
WHERE
    fqs.is_correct IS NOT NULL
    AND DATE(fqs.submission_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
    -- AND fqs.test_id = 'testA' -- Uncomment and specify if filtering for a single test
GROUP BY
    fqs.question_id, dq.question_text
ORDER BY
    correct_rate DESC
LIMIT 10;


-- Mostly Revisited Questions
SELECT
    fqs.question_id,
    dq.question_text,
    COUNT(fqs.submission_id) AS total_revisits
FROM
    `varta.idfy_assignment.fact_question_submissions` AS fqs
JOIN
    `varta.idfy_assignment.dim_questions` AS dq
ON
    fqs.question_id = dq.question_id AND fqs.test_id = dq.test_id
WHERE
    fqs.is_resubmission = TRUE
    AND DATE(fqs.submission_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
    
GROUP BY
    fqs.question_id, dq.question_text
ORDER BY
    total_revisits DESC
LIMIT 10;

-- Mostly Answered Wrong Questions
SELECT
    fqs.question_id,
    dq.question_text,
    COUNT(fqs.submission_id) AS total_submissions,
    SUM(CASE WHEN NOT fqs.is_correct THEN 1 ELSE 0 END) AS incorrect_submissions,
    SAFE_DIVIDE(SUM(CASE WHEN NOT fqs.is_correct THEN 1 ELSE 0 END), COUNT(fqs.submission_id)) AS incorrect_rate
FROM
    `varta.idfy_assignment.fact_question_submissions` AS fqs
JOIN
    `varta.idfy_assignment.dim_questions` AS dq
ON
    fqs.question_id = dq.question_id AND fqs.test_id = dq.test_id
WHERE
    fqs.is_correct IS NOT NULL
    AND DATE(fqs.submission_timestamp) BETWEEN '2025-06-01' AND '2025-06-30'
    
GROUP BY
    fqs.question_id, dq.question_text
ORDER BY
    incorrect_rate DESC
LIMIT 10;


-- Funnel View 


SELECT
    fqs.submission_id,
    fqs.submission_timestamp,
    fqs.question_id,
    dq.page_order,
    dq.question_text, 
    fqs.selected_option,
    fqs.is_correct,
    fqs.is_resubmission,
    fqs.time_on_question_ms,
    fqs.time_since_last_question_ms,
    fqs.time_since_test_start_ms
FROM
    `varta.idfy_assignment.fact_question_submissions` AS fqs
JOIN
    `varta.idfy_assignment.dim_questions` AS dq
ON
    fqs.question_id = dq.question_id AND fqs.test_id = dq.test_id
WHERE
    fqs.user_id = 'user1' 
    AND fqs.test_id = 'testA' 
ORDER BY
    fqs.submission_timestamp;


-- Session and question level stats

SELECT
    COUNT(DISTINCT session_id) AS total_sessions,
    AVG(session_duration_seconds) AS average_session_duration_seconds,
    SUM(CASE WHEN ended_due_to_ttl THEN 1 ELSE 0 END) AS sessions_ended_by_ttl
FROM
    `varta.idfy_assignment.fact_sessions`
WHERE
    session_start_timestamp IS NOT NULL
    AND DATE(session_start_timestamp) BETWEEN '2025-06-01' AND '2025-06-30';

