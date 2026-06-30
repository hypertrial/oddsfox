{% macro canonical_team_match_key(column_expr) -%}
coalesce(
    (
        select aliases.canonical_match_key
        from {{ ref("seed_team_canonical_aliases") }} as aliases
        where aliases.variant_match_key = {{ name_match_key(column_expr) }}
        limit 1
    ),
    {{ name_match_key(column_expr) }}
)
{%- endmacro %}
