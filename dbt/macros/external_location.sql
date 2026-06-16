{#
  Builds the on-disk Parquet path for an external model under DATA_DIR, e.g.
    external_location('silver', 'quickbooks/customers')
    -> $DATA_DIR/silver/quickbooks/customers.parquet
  DATA_DIR is required (a local dir in dev, the SMB share in prod). `make dbt-run`
  exports it from .env; set it in the environment when running dbt directly.
#}
{% macro external_location(layer, name) %}
    {% set root = env_var('DATA_DIR') %}
    {{ return(root ~ '/' ~ layer ~ '/' ~ name ~ '.parquet') }}
{% endmacro %}
