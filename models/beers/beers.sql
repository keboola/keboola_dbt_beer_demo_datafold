
{{ config(
    materialized="table"
) }}

SELECT
  "id"            AS beer_id,
  TRIM("name")    AS beer_name,
  "style"         AS beer_style,
  "abv"           AS abv,
  "ibu"           AS ibu,
  CASE
       WHEN SAFE_CAST("ibu" AS INT) <= 50 THEN 'Malty'
       WHEN SAFE_CAST("ibu" AS INT) > 50 THEN 'Hoppy'
  END AS bitterness,
  "brewery_id"    AS brewery_id,
  "ounces"        AS ounces
FROM
  {{ source('dbt_beer_seed', 'beers') }}
