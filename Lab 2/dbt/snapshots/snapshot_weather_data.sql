{% snapshot snapshot_weather_data %}

{{
    config(
        target_schema='snapshot',
        unique_key="city || '-' || weather_date",
        strategy='check',
        check_cols=['precipitation_sum', 'weather_code'],
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('stg_weather_data') }}
{% endsnapshot %}