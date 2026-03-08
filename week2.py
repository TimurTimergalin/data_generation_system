from clickhouse_connect import create_client
from scipy.stats import chisquare
from itertools import chain
from ab_exec.ab_results import *
import numpy as np


client = create_client(host='5.129.200.238', username='team_11', password='team_pass_11')


def onboarding_length_retention(group, days):
    query = f"""
WITH
data as (
    SELECT
        uniqExact(user_id) AS cohort_size,
        uniqExactIf(user_id, days_since_install >= {days}) AS returned
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'onboarding_length') = '{group}'
    GROUP BY cohort_date
)
SELECT returned / cohort_size AS retention FROM data
""".strip()
    res = client.query(query)
    return list(chain.from_iterable(res.result_rows))


def onboarding_length_tutorial_completion_rate(group):
    query = f"""
WITH
completed AS(
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE 
        JSONExtractString(ab_tests, 'onboarding_length') = '{group}' AND
        event_name = 'tutorial_step' AND 
        NOT JSONExtractBool(event_properties, 'is_skipped') AND
        JSONExtractString(event_properties, 'step_id') = 'tut_complete'
)
SELECT
    if(user_id IN completed, 1, 0)
FROM game_analytics.events
WHERE JSONExtractString(ab_tests, 'onboarding_length') = '{group}'
""".strip()
    res = client.query(query)
    return list(chain.from_iterable(res.result_rows))


def onboarding_length_d1_sessions(group):
    query = f"""
SELECT count(*)
FROM game_analytics.events
WHERE JSONExtractString(ab_tests, 'onboarding_length') = '{group}' AND
      event_name = 'session_start' AND
      days_since_install = 0
GROUP BY user_id
""".strip()
    res = client.query(query)
    return list(chain.from_iterable(res.result_rows))


def onboarding_length_tutorial_duration(group):
    query = f"""
WITH
target_users AS (
    SELECT DISTINCT
        user_id
    FROM game_analytics.events
    WHERE
        JSONExtractString(ab_tests, 'onboarding_length') = '{group}' AND
        event_name = 'tutorial_step' AND
        NOT JSONExtractBool(event_properties, 'is_skipped') AND
        JSONExtractString(event_properties, 'step_id') = 'tut_complete'
)
SELECT 
    date_diff('second', min(e.event_timestamp), max(e.event_timestamp))
FROM game_analytics.events e
INNER JOIN target_users t on e.user_id = t.user_id
WHERE e.event_name = 'tutorial_step' AND JSONExtractString(event_properties, 'step_id') IN ['tut_welcome', 'tut_complete'] 
GROUP BY e.user_id

""".strip()
    res = client.query(query)
    return list(chain.from_iterable(res.result_rows))


if __name__ == '__main__':
    metrics = ["D1 retention", "D7 retention", "Tutorial completion rate", "Sessions on day 1", "Tutorial length"]

    sample_names = ["control", "extended"]
    samples = [
        [
            Measure(np.asarray(onboarding_length_retention(group, 1)), 'mean'),
            Measure(np.asarray(onboarding_length_retention(group, 7)), 'mean'),
            Measure(np.asarray(onboarding_length_tutorial_completion_rate(group)), 'conversion'),
            Measure(np.asarray(onboarding_length_d1_sessions(group)), 'mean'),
            Measure(np.asarray(onboarding_length_tutorial_duration(group)), 'mean')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
