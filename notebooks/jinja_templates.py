# ============================================================
# jinja_templates.py
# StreamPulse - Dynamic SQL using Jinja2 Templates
# Makes SQL modular, reusable and dynamic
# ============================================================

from jinja2 import Template
from pyspark.sql import SparkSession

# ── Template 1: Aggregation Template ────────────────────────
AGGREGATION_TEMPLATE = """
SELECT
    {{ group_by_col }},
    COUNT(event_id)                    AS total_views,
    ROUND(AVG(completion_pct), 2)      AS avg_completion,
    ROUND(AVG(engagement_score), 2)    AS avg_engagement,
    ROUND(SUM(revenue_per_view), 2)    AS total_revenue,
    SUM(is_completed)                  AS completed_views,
    SUM(is_dropped)                    AS dropped_views,
    ROUND(
        SUM(is_completed) * 100.0 / COUNT(*), 2
    )                                  AS completion_rate_pct
FROM {{ table_name }}
{% if filter_condition %}
WHERE {{ filter_condition }}
{% endif %}
GROUP BY {{ group_by_col }}
ORDER BY total_views DESC
{% if limit %}
LIMIT {{ limit }}
{% endif %}
"""

# ── Template 2: Content Performance Template ─────────────────
CONTENT_PERFORMANCE_TEMPLATE = """
SELECT
    c.content_title,
    c.genre,
    c.language,
    c.content_type,
    COUNT(f.event_id)                  AS total_views,
    ROUND(AVG(f.completion_pct), 2)    AS avg_completion,
    ROUND(AVG(f.engagement_score), 2)  AS avg_engagement,
    ROUND(SUM(f.revenue_per_view), 2)  AS total_revenue,
    SUM(f.is_completed)                AS completed_views,
    SUM(f.is_dropped)                  AS dropped_views
FROM {{ fact_table }} f
JOIN {{ dim_content_table }} c
    ON f.content_id = c.content_id
{% if genre_filter %}
WHERE c.genre = '{{ genre_filter }}'
{% endif %}
GROUP BY
    c.content_title,
    c.genre,
    c.language,
    c.content_type
ORDER BY {{ order_by }} DESC
{% if limit %}
LIMIT {{ limit }}
{% endif %}
"""

# ── Template 3: Time Analysis Template ───────────────────────
TIME_ANALYSIS_TEMPLATE = """
SELECT
    {{ time_col }},
    COUNT(event_id)                    AS total_views,
    ROUND(AVG(completion_pct), 2)      AS avg_completion,
    ROUND(AVG(engagement_score), 2)    AS avg_engagement,
    SUM(is_prime_time)                 AS prime_time_views,
    SUM(is_weekend)                    AS weekend_views
FROM {{ table_name }}
{% if start_date and end_date %}
WHERE event_timestamp
    BETWEEN '{{ start_date }}'
    AND '{{ end_date }}'
{% endif %}
GROUP BY {{ time_col }}
ORDER BY {{ time_col }}
"""

# ── Template 4: Location Analysis Template ───────────────────
LOCATION_TEMPLATE = """
SELECT
    l.city,
    l.state,
    l.region,
    COUNT(f.event_id)                  AS total_views,
    ROUND(AVG(f.completion_pct), 2)    AS avg_completion,
    ROUND(AVG(f.engagement_score), 2)  AS avg_engagement,
    ROUND(SUM(f.revenue_per_view), 2)  AS total_revenue,
    COUNT(DISTINCT f.user_id)          AS unique_users
FROM {{ fact_table }} f
JOIN {{ dim_location_table }} l
    ON f.city = l.city
{% if region_filter %}
WHERE l.region = '{{ region_filter }}'
{% endif %}
GROUP BY
    l.city,
    l.state,
    l.region
ORDER BY total_views DESC
{% if limit %}
LIMIT {{ limit }}
{% endif %}
"""

# ── Metadata Config ──────────────────────────────────────────
# All reports defined in one place
# Add new report here without changing any other code

REPORT_CONFIGS = [
    {
        "name":             "views_by_device",
        "template":         AGGREGATION_TEMPLATE,
        "group_by_col":     "device_type",
        "table_name":       "streampulse_gold.fact_watch_events",
        "filter_condition": None,
        "limit":            10
    },
    {
        "name":             "views_by_subscription",
        "template":         AGGREGATION_TEMPLATE,
        "group_by_col":     "subscription_plan",
        "table_name":       "streampulse_gold.fact_watch_events",
        "filter_condition": "is_completed = 1",
        "limit":            None
    },
    {
        "name":             "views_by_time_of_day",
        "template":         AGGREGATION_TEMPLATE,
        "group_by_col":     "time_of_day",
        "table_name":       "streampulse_gold.fact_watch_events",
        "filter_condition": None,
        "limit":            None
    },
    {
        "name":             "views_by_completion_category",
        "template":         AGGREGATION_TEMPLATE,
        "group_by_col":     "completion_category",
        "table_name":       "streampulse_gold.fact_watch_events",
        "filter_condition": None,
        "limit":            None
    },
    {
        "name":             "thriller_content_performance",
        "template":         CONTENT_PERFORMANCE_TEMPLATE,
        "fact_table":       "streampulse_gold.fact_watch_events",
        "dim_content_table":"streampulse_gold.dim_content",
        "genre_filter":     "THRILLER",
        "order_by":         "total_views",
        "limit":            5
    },
    {
        "name":             "all_content_performance",
        "template":         CONTENT_PERFORMANCE_TEMPLATE,
        "fact_table":       "streampulse_gold.fact_watch_events",
        "dim_content_table":"streampulse_gold.dim_content",
        "genre_filter":     None,
        "order_by":         "total_revenue",
        "limit":            10
    },
    {
        "name":             "views_by_day_of_week",
        "template":         TIME_ANALYSIS_TEMPLATE,
        "time_col":         "event_day",
        "table_name":       "streampulse_gold.fact_watch_events",
        "start_date":       None,
        "end_date":         None
    },
    {
        "name":             "west_region_performance",
        "template":         LOCATION_TEMPLATE,
        "fact_table":       "streampulse_gold.fact_watch_events",
        "dim_location_table":"streampulse_gold.dim_locations",
        "region_filter":    "WEST",
        "limit":            5
    },
    {
        "name":             "all_location_performance",
        "template":         LOCATION_TEMPLATE,
        "fact_table":       "streampulse_gold.fact_watch_events",
        "dim_location_table":"streampulse_gold.dim_locations",
        "region_filter":    None,
        "limit":            8
    },
]


# ── Run All Reports ──────────────────────────────────────────
def run_all_reports(spark):
    """
    Runs all reports defined in REPORT_CONFIGS.
    Each report is generated dynamically from template.
    Adding a new report = adding one dict to REPORT_CONFIGS.
    No other code changes needed.
    """
    results = {}

    for config in REPORT_CONFIGS:
        print(f"\n{'='*55}")
        print(f"📊 Running: {config['name']}")
        print(f"{'='*55}")

        try:
            # Render SQL from template
            sql = Template(
                config["template"]
            ).render(**config)

            print(f"Generated SQL:")
            print(sql)

            # Execute SQL
            df = spark.sql(sql)
            df.show(truncate=False)

            results[config["name"]] = df
            print(f"✅ {config['name']} complete!")

        except Exception as e:
            print(f"❌ {config['name']} failed: {str(e)}")

    print(f"\n{'='*55}")
    print(f"✅ All {len(REPORT_CONFIGS)} reports complete!")
    print(f"{'='*55}")

    return results


# ── Run if called directly ───────────────────────────────────
if __name__ == "__main__":
    # This runs in Databricks
    # spark is already available in Databricks
    results = run_all_reports(spark)