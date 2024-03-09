# Databricks notebook source
# MAGIC %md
# MAGIC ## Validando la conneccion con el punto de monjate

# COMMAND ----------

#Validar el punto de coneccion
dbutils.fs.ls('mnt/bronze/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('mnt/silver/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformacion de un campo

# COMMAND ----------

# # estabelzco la ruta con el punto de montaje del archivo(tabla) que quiero consumir
# input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'
# # paso la ruta anterior a una funcion que me permite leer ese archivo que esta en formato parquet
# df=spark.read.format('parquet').load(input_path)
# # muestro la tabla
# display(df)


# COMMAND ----------

# # Importación de funciones y tipos de PySpark
# from pyspark.sql.functions import from_utc_timestamp, date_format
# from pyspark.sql.types import TimestampType

# # Por medio de la funcion withColumn() modificamos una columna en espesifico
# # por medio de la funcion date_format() convierte una columna a un formato de fecha espesificado
# # por meido de la funicon from_utc_timestamp() convierte la columna de timestamp al formato UTC (tiempo coordinado universal).
# df = df.withColumn(
#     "ModifiedDate",
#     date_format(
#         from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"),
#         "yyyy-MM-dd"
#     )
# )

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformacion para todas las tablas

# COMMAND ----------

# Creo un vector(lista) vacio
table_name = []

#lleno ese vector con con los nombre de todas las tablas
for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split('/')[0])

# display(table_name)

# COMMAND ----------

# table_name

# COMMAND ----------

# Importación de funciones y tipos de PySpark
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in table_name:
    path = '/mnt/bronze/SalesLT/'+i+'/'+i+'.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns
    # print(column)
    # print('-----------------------------')

    for col in column:
        if 'Date' in col or 'date' in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
            # display(df)

    output_path = '/mnt/silver/SalesLT/'+i+'/'
    df.write.format('delta').mode("overwrite").save(output_path)



# COMMAND ----------

for i in table_name:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS bronze.{i}
    USING parquet
    OPTIONS ('path' 'abfss://bronze@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/{i}.parquet')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC
