# Databricks notebook source
# input_path='/mnt/silver/SalesLT/Address/'
# df = spark.read.format('delta').load(input_path)
# display(df)

# COMMAND ----------

# dbutils.fs.ls('mnt/silver/SalesLT')

# COMMAND ----------

# dbutils.fs.ls('mnt/gold/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear virtualmente las tablas en databricks

# COMMAND ----------

# Creo un vector(lista) vacio
table_name_silver_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
    table_name_silver_virtual.append(i.name.split('/')[0])

# COMMAND ----------

for i in table_name_silver_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS silver.{i}
    USING delta
    OPTIONS ('path' 'abfss://silver@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ajustar los nombres de las columnas  

# COMMAND ----------

# Creo un vector(lista) vacio
table_name = []

#lleno ese vector con con los nombre de todas las tablas
for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
    table_name.append(i.name.split('/')[0])


# COMMAND ----------


# table_name

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# ciclo para obtener todas las tablas en formato delta de la capa silver
for table in table_name:
    
    path= '/mnt/silver/SalesLT/'+ table
    df=spark.read.format('delta').load(path)

    #obtener las columnas de cada tablas
    column_names = df.columns

    #ciclo para iterar por cada columa de la tabla
    for old_column in column_names:
        #Atraves de una lista de comprension se esta evaluando si cada letra es mayuscula y si su predecesora lo es para asiganar un "_" como separador de cada palabra
        new_col_name = "".join(["_" + char if char.isupper() and not old_column[i - 1].isupper() else char for i, char in enumerate(old_column)]).lstrip("_")

        # se cambia el nombre de la columna vieja por el nuevo
        df = df.withColumnRenamed(old_column, new_col_name)
    
    #Aplicar las tranformaciones a las tablas fisicas
    output_path='/mnt/gold/SalestLT/'+table+'/' 
    df.write.format('delta').mode('overwrite').save(output_path)



# COMMAND ----------

# input_path='/mnt/gold/SalesLT/Address/'
# df = spark.read.format('delta').load(input_path)
# display(df)

# COMMAND ----------

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

# COMMAND ----------

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)
