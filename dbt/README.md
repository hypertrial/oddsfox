# OddsFox dbt Project

This dbt project models the WC2026 Polymarket raw and ops tables in DuckDB.

See the operator docs for warehouse details:

- [Warehouse](../docs/warehouse.md)
- [Operations](../docs/operations.md)

Modeled layers:

- `polymarket_staging`
- `polymarket_intermediate`
- `polymarket_marts`
- `polymarket_observability`

Run locally:

```bash
dbt parse --project-dir dbt --profiles-dir dbt/profiles
dbt build --full-refresh --project-dir dbt --profiles-dir dbt/profiles
```
