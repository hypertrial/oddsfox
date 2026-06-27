# Sync one day of active markets

```bash
oddsfox init --out ./lake
oddsfox sync markets --active --out ./lake --limit 500
oddsfox duckdb --out ./lake --db ./lake/catalog.duckdb
```

Query in DuckDB:

```sql
SELECT question, volume_24h FROM bronze_markets ORDER BY volume_24h DESC LIMIT 10;
```
