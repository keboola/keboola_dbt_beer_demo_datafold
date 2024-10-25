{{ config(
    materialized='table',
    unique_key='order_line',
    persist_docs={"relation": true, "columns": true}
) }}

-- We'll fake some data, since this is an example repository
-- we want to make sure that we continue to generate data up
-- to today

WITH generated_order_lines AS (
    {% for day_ago in range(5) %}
        {% for order_number in range(2) %}
            -- Each order has between 1 and 5 order_lines
            {% for order_line in range(3) %}
                SELECT CONCAT(
                            {{ date_format() }}(
                                {{ bigquery__yyymmdd() }},
                                DATE_ADD(DATE(current_timestamp), INTERVAL -1 * {{ day_ago }} DAY)
                            ),
                            '{{ order_number }}'
                       )                                   AS order_no,
                       CONCAT(
                           {{ date_format() }}(
                                {{ bigquery__yyymmdd() }},
                                DATE_ADD(DATE(current_timestamp), INTERVAL -1 * {{ day_ago }} DAY)
                           ),
                           '{{ order_number }}{{ order_line }}'
                       ) AS order_line,
                       (
                            -- Deterministically select a random beer
                            SELECT MOD(
                                CAST(FLOOR(100*RAND()) AS INT64),
                                (
                                    SELECT MAX(SAFE_CAST(beer_id AS INT64)) FROM {{ ref('beers') }}
                                )
                            )
                       )                                                          AS beer_id,
                       CAST(FLOOR(100*RAND()) AS INT64) AS quantity,
                       CAST(FLOOR(100*RAND()) AS INT64) AS price,
                       DATE_ADD(DATE(current_timestamp), INTERVAL -1 * {{ day_ago }} DAY)        AS created_at,
                       current_timestamp                                          AS changed_at

                {% if not loop.last %}
                  UNION ALL
                {% endif %}
            {% endfor %}
            {% if not loop.last %}
            UNION ALL
            {% endif %}
      {% endfor %}

      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
)

SELECT *
FROM generated_order_lines

{% if is_incremental() %}
    WHERE created_at::date > (SELECT MAX(created_at)::date FROM {{ this }})
{% endif %}
