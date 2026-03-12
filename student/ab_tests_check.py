from clickhouse_connect import create_client
from scipy.stats import chisquare
from itertools import chain
from ab_exec.ab_results import *
import numpy as np
from functools import wraps


client = create_client(host='5.129.200.238', username='team_11', password='team_pass_11')


def query(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        res = client.query(func(*args, **kwargs))
        return np.asarray(list(chain.from_iterable(res.result_rows)))
    return new_func


@query
def onboarding_length_retention(group, days):
    return f"""
WITH
returned AS (
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE days_since_install >= {days} AND JSONExtractString(ab_tests, 'onboarding_length') = '{group}'
),
all_users AS (
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'onboarding_length') = '{group}'
)
SELECT if(user_id in returned, 1, 0) FROM all_users

""".strip()


@query
def onboarding_length_tutorial_completion_rate(group):
    return f"""
WITH
completed AS(
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE 
        JSONExtractString(ab_tests, 'onboarding_length') = '{group}' AND
        event_name = 'tutorial_step' AND 
        NOT JSONExtractBool(event_properties, 'is_skipped') AND
        JSONExtractString(event_properties, 'step_id') = 'tut_complete'
),
all_users AS (
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'onboarding_length') = '{group}'
)
SELECT
    if(user_id IN completed, 1, 0)
FROM all_users
""".strip()


@query
def onboarding_length_d1_sessions(group):
    return f"""
SELECT count(*)
FROM game_analytics.events
WHERE JSONExtractString(ab_tests, 'onboarding_length') = '{group}' AND
      event_name = 'session_start' AND
      days_since_install = 0
GROUP BY user_id
""".strip()


@query
def onboarding_length_tutorial_duration(group):
    return f"""
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


@query
def starter_pack_price_purchase_conversion(group):
    return f"""
WITH
purchasing_users AS (
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE event_name = 'iap_purchase'
      AND JSONExtractString(event_properties, 'product_name') = 'Starter Pack'
      AND JSONExtractString(ab_tests, 'starter_pack_price') = '{group}'
),
all_users AS (
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'starter_pack_price') = '{group}'
)
SELECT if(user_id in purchasing_users, 1, 0)
FROM all_users
""".strip()


@query
def starter_pack_price_arppu(group):
    return f"""
SELECT
    sum(JSONExtractFloat(event_properties, 'price_usd'))
FROM game_analytics.events
WHERE JSONExtractString(ab_tests, 'starter_pack_price') = '{group}' AND
      event_name = 'iap_purchase'
GROUP BY user_id
""".strip()


@query
def starter_pack_price_arpu(group):
    return f"""
WITH
revenue AS (
    SELECT
        user_id,
        sum(JSONExtractFloat(event_properties, 'price_usd')) as revenue
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'starter_pack_price') = '{group}' AND
        event_name = 'iap_purchase'
    GROUP BY user_id
),
all_users AS (
    SELECT DISTINCT user_id
    FROM game_analytics.events
    WHERE JSONExtractString(ab_tests, 'starter_pack_price') = '{group}'
)
SELECT
    if(r.revenue = NULL, 0.0, r.revenue)
FROM revenue r
RIGHT JOIN all_users a ON r.user_id = a.user_id
""".strip()


@query
def starter_pack_price_multiple_purchase_rate(group):
    return f"""
SELECT if(count(event_name) > 1, 1, 0) AS purchases
FROM game_analytics.events
WHERE event_name = 'iap_purchase'
  AND JSONExtractString(event_properties, 'product_name') = 'Starter Pack'
  AND JSONExtractString(ab_tests, 'starter_pack_price') = '{group}'
GROUP BY user_id 
""".strip()

@query
def custom_tutorial_streamline_retention(group, days):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_tutorial_streamline') = '{group}'
),
returned AS (
    SELECT DISTINCT user_id
    FROM filtered_events
    WHERE days_since_install >= {days}
),
all_users AS (
    SELECT DISTINCT user_id
    FROM filtered_events
)
SELECT if(user_id in returned, 1, 0) FROM all_users
""".strip()

@query
def custom_aggressive_starter_purchase_conversion(group):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_aggressive_starter') = '{group}'
),
purchasing_users AS (
    SELECT DISTINCT user_id
    FROM filtered_events
    WHERE event_name = 'iap_purchase'
      AND JSONExtractString(event_properties, 'product_name') = 'Starter Pack'
),
all_users AS (
    SELECT DISTINCT user_id
    FROM filtered_events
)
SELECT if(user_id in purchasing_users, 1, 0)
FROM all_users
""".strip()

@query
def custom_generous_ads_ads(group):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_generous_ads') = '{group}'
)
SELECT count(*)
FROM filtered_events
WHERE event_name = 'ad_completed'
GROUP BY user_id
""".strip()

@query
def custom_generous_ads_ads(group):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_generous_ads') = '{group}'
)
SELECT count(*)
FROM filtered_events
WHERE event_name = 'ad_completed'
GROUP BY user_id
""".strip()

@query
def custom_generous_ads_sessions(group):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_generous_ads') = '{group}'
)
SELECT count(*)
FROM filtered_events
WHERE event_name = 'session_start'
GROUP BY user_id
""".strip()

@query
def custom_generous_ads_purchase_conversion(group):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_generous_ads') = '{group}'
),
purchasing_users AS (
    SELECT DISTINCT user_id
    FROM filtered_events
    WHERE event_name = 'iap_purchase'
      AND JSONExtractString(event_properties, 'product_name') = 'Starter Pack'
),
all_users AS (
    SELECT DISTINCT user_id
    FROM filtered_events
)
SELECT if(user_id in purchasing_users, 1, 0)
FROM all_users
""".strip()

@query
def custom_early_gacha_hype_retention(group, days):
    return f"""
WITH
filtered_events AS (
    SELECT *
    FROM team_11.events
    WHERE run_id = 'onboarding_test' AND JSONExtractString(ab_tests, 'custom_early_gacha_hype') = '{group}'
),
returned AS (
    SELECT DISTINCT user_id
    FROM filtered_events
    WHERE days_since_install >= {days}
),
all_users AS (
    SELECT DISTINCT user_id
    FROM filtered_events
)
SELECT if(user_id in returned, 1, 0) FROM all_users
""".strip()

if __name__ == '__main__':
    # Week 2
    # Part A
    print("Week 2")
    print("Part A")
    print("Onboarding length")
    metrics = ["D1 retention", "D7 retention", "Tutorial completion rate", "Sessions on day 1", "Tutorial length"]
    sample_names = ["control", "short", "extended"]
    samples = [
        [
            Measure(onboarding_length_retention(group, 1), 'conversion'),
            Measure(onboarding_length_retention(group, 7), 'conversion'),
            Measure(onboarding_length_tutorial_completion_rate(group), 'conversion'),
            Measure(onboarding_length_d1_sessions(group), 'mean'),
            Measure(onboarding_length_tutorial_duration(group), 'mean')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
    print("================================================")

    print("Starter pack price")
    metrics = ['Purchase conversion', 'ARPPU', 'ARPU', 'Multiple Purchase Rate']
    sample_names = ['control', 'lower', 'higher']
    samples = [
        [
            Measure(starter_pack_price_purchase_conversion(group), 'conversion'),
            Measure(starter_pack_price_arppu(group), 'mean'),
            Measure(starter_pack_price_arpu(group), 'mean'),
            Measure(starter_pack_price_multiple_purchase_rate(group), 'conversion')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
    print("================================================")

    # Part C
    print("Part C")
    print("Custom tutorial streamline")
    metrics = ["D1 retention", "D7 retention"]
    sample_names = ['control', 'treatment']
    samples = [
        [
            Measure(custom_tutorial_streamline_retention(group, 1), 'conversion'),
            Measure(custom_tutorial_streamline_retention(group, 7), 'conversion')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
    print("================================================")

    print("Custom aggressive starter")
    metrics = ["Purchase conversion"]
    sample_names = ['control', 'treatment']
    samples = [
        [
            Measure(custom_aggressive_starter_purchase_conversion(group), 'conversion'),
            Measure(custom_aggressive_starter_purchase_conversion(group), 'conversion')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
    print("================================================")

    print("Custom early gacha hype retention")
    metrics = ["D1 retention", "D7 retention"]
    sample_names = ['control', 'treatment']
    samples = [
        [
            Measure(custom_early_gacha_hype_retention(group, 1), 'conversion'),
            Measure(custom_early_gacha_hype_retention(group, 7), 'conversion')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
    print("================================================")

    # Week 3
    # Part A
    print("Week 3")
    print("Part A")
    print("Onboarding length")
    metrics = ["D1 retention", "D7 retention", "Tutorial completion rate", "Sessions on day 1", "Tutorial length"]
    sample_names = ["control", "short", "extended"]
    samples = [
        [
            Measure(onboarding_length_retention(group, 1), 'conversion'),
            Measure(onboarding_length_retention(group, 7), 'conversion'),
            Measure(onboarding_length_tutorial_completion_rate(group), 'conversion'),
            Measure(onboarding_length_d1_sessions(group), 'mean'),
            Measure(onboarding_length_tutorial_duration(group), 'mean')
        ]
        for group in sample_names
    ]
    perform_tests(metrics, sample_names, samples, 0.05, 0.2)
    print("================================================")
