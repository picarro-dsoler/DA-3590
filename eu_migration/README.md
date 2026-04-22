# EU Migration Bundle

This folder contains copied ETL scripts for the Toscana and Italgas/Gas2Go flow plus a copied Italgas G2G database ingester. The original files under `src/` and `italgas_g2g_leak_data_ingester/` were not modified.

## Local path changes in these copies

- CSV output now goes to `eu-migration/utility_data/`.
- The copied dashboard ETL no longer references `/eng-app-anders/g2g_report_data/`.
- The copied Gas2Go reader now looks for the SQLite database at `eu-migration/italgas_g2g_leak_data_ingester/database/italgas_g2g.db`.

## Included files

- `update_energy_service_dashboard.py`
- `picarro_investigation_app_data.py`
- `import_leak_investigation.py`
- `energy_service_modules.py`
- `smartsheet_data_access.py`
- `investigation_decoder.ini`
- `italgas_g2g_leak_data_ingester/`
