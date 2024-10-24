{{ config(
    materialized='table',
    unique_key='order_no',
    persist_docs={"relation": true, "columns": true}
) }}

-- We'll fake some data, since this is an example repository
-- we want to make sure that we continue to generate data up
-- to today

WITH generated_orders AS (
    {% for day_ago in range(30) %}
        {% for order_number in range(10) %}
          SELECT
              GENERATE_UUID() AS order_no,
             {{ bigquery__randint(123456, 654321) }} AS customer_id,
             {% if order_number is divisibleby 13 %}
                'PENDING' AS status,
             {% else %}
                'DELIVERED' AS status,
             {% endif %}
             DATE_ADD(DATE(CURRENT_DATE), INTERVAL -1 * {{ day_ago }} DAY) AS created_at,
             current_timestamp AS changed_at

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
FROM generated_orders

{% if is_incremental() %}
    WHERE created_at::date > (SELECT MAX(created_at)::date FROM {{ this }})
{% endif %}
