# Databricks notebook source
# MAGIC %md
# MAGIC ## Validando la conneccion con el punto de monjate

# COMMAND ----------

#Validar el punto de coneccion
dbutils.fs.ls('mnt/bronze/SalesLT/Address')

# COMMAND ----------

dbutils.fs.ls('mnt/silver/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replicamos virtualmente todas las tablas de la capa bronze del datalake en el catalogo bronze de databricks

# COMMAND ----------

# Creo un vector(lista) vacio
table_name = []

#lleno ese vector con con los nombre de todas las tablas
for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split('/')[0])

# display(table_name)

# COMMAND ----------

for i in table_name:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS bronze.{i}
    USING parquet
    OPTIONS ('path' 'abfss://bronze@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/{i}.parquet')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformacion para todas las tablas
# MAGIC ##### - Se cambia el formato de fecha de todos las campos de fecha en todas las tablas 
# MAGIC ##### - Se cambia el nombre de cada campo y de cada tabla con una estrutura como: Palabra1_Palabra2 o Palabra1
# MAGIC ##### - Se agrega el nombre de la respectiva tabla a los campos que puedan tener nombres ambiguos con otras tablas

# COMMAND ----------

# Importación de funciones y tipos de PySpark
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

# Cilo para iterar por cada tabla
for table in table_name:
    path = '/mnt/bronze/SalesLT/'+table+'/'+table+'.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    # Cilo para itrar por cada columna
    for col in column:
        # validacion si el campo tiene nombre date (haciendo referencia a q todos los campos de fecha tiene un date en du nombre)
        if 'Date' in col or 'date' in col:
            #cambio de tipo fecha de timestamp a un formato yyyy-MM-dd
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
    
    #ciclo para iterar por cada columa de la tabla
    for old_column in column:
        #Atraves de una lista de comprension se esta evaluando si cada letra es mayuscula y si su predecesora lo es para asiganar un "_" como separador de cada palabra
        new_col_name = "".join(["_" + char if char.isupper() and not old_column[i - 1].isupper() else char for i, char in enumerate(old_column)]).lstrip("_")

        # con este condicinal modifico el nombre de las columnas como Name y ModifiedDate que se repiten varias tablas, agegando un "_nombre tabla" al seguido de su previo nombre, esto con el fin de diferenciar los campos en las posteriores dimensiones 
        if (new_col_name == 'Name') or ('Modified_Date' in new_col_name or 'date' in new_col_name) :
        # if (new_col_name == 'Name') or ('Date' in new_col_name or 'date' in new_col_name) :    
            new_col_name_2 = new_col_name+'_'+table 
        else: new_col_name_2=new_col_name

        # se cambia el nombre de la columna vieja por el nuevo
        df = df.withColumnRenamed(old_column, new_col_name_2)

    # #Aplicar las tranformaciones a las tablas fisicas
    output_path='/mnt/silver/SalesLT/'+table+'/' 
    df.write.format('delta').mode('overwrite').save(output_path)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Replicamos virtualmente todas las tablas de la capa silver del datalake en el catalogo silver de databricks

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
