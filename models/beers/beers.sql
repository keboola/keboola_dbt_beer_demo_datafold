
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
       WHEN "ibu" <= 50 THEN 'Malty'
       WHEN "ibu" > 50 THEN 'Hoppy'
   END AS bitterness,
  "brewery_id"    AS brewery_id,
  "ounces"        AS ounces
FROM
  {{ source('in.c-dbt_beer_seed', 'seed_beers') }}
