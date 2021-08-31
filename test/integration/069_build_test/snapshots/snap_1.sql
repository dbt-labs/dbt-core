{% snapshot snap_1 %}

{{
    config(
      target_database=database,
      target_schema=schema,
      unique_key='iso3',

      strategy='timestamp',
      updated_at='snap_1_updated_at',
    )
}}

SELECT 
  iso3, 
  "name", 
  iso2, 
  iso_numeric, 
  cow_alpha, 
  cow_numeric, 
  fao_code, 
  un_code, 
  wb_code, 
  imf_code, 
  fips, 
  geonames_name, 
  geonames_id, 
  r_name, 
  aiddata_name, 
  aiddata_code, 
  oecd_name, 
  oecd_code, 
  historical_name, 
  historical_iso3, 
  historical_iso2, 
  historical_iso_numeric,
  current_timestamp as snap_1_updated_at from {{ ref('model_1') }}

{% endsnapshot %}