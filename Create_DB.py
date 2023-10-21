import duckdb

con = duckdb.connect(database='matriculaciones.duckdb')

load_query = """
    CREATE OR REPLACE TABLE matriculaciones AS
    SELECT * FROM 'df_streamlit_mapeado.csv'
"""
con.execute(load_query)

delete_column = "ALTER TABLE matriculaciones DROP COLUMN00"
con.execute(delete_column)

add_date_col = "ALTER TABLE matriculaciones ADD COLUMN FECHA DATE"
con.execute(add_date_col)

update_date_col = "UPDATE matriculaciones SET FECHA = last_day(FECHA_MATRICULA)"
con.execute(update_date_col)

rename_column_query = """
    ALTER TABLE matriculaciones
    RENAME COLUMN "CATEGORÍA_VEHÍCULO_ELÉCTRICO" TO CATEGORIA_VEHICULO_ELECTRICO
"""
con.execute(rename_column_query)

con.close()