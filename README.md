# NYC Childcare Centers Inspections ETL

## Para ejecutar...

1. Editar nombre del archivo `settings.ini.example` a `settings.ini`
2. Agregar credenciales
3. `PYTHONPATH='.' luigi --module nyc_ccci_etl.luigi_tasks.load_task LoadTask --year=2020 --month=2 --day=6  --local-scheduler`