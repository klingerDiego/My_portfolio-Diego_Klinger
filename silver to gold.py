# Databricks notebook source
# MAGIC %md
# MAGIC # Se crear la dim_producto a partir de las demas tablas
# MAGIC ##### - Se pivotena los campos de descripcion por regiones para pasar de una relacion de uno a muchos entre la tabla de product y productmodelproductdescription a una uno a uno, de esta forma sera mas facil de integrar en la dim_producto

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with culter_description as (
# MAGIC
# MAGIC select Product_Model_ID, max(he) he_description, max(th) th_description, max(fr) fr_description, max(ar) ar_description, max(zh_cht) zh_cht_description, max(en) en_description
# MAGIC from (
# MAGIC select 
# MAGIC Product_Model_ID,
# MAGIC Culture,
# MAGIC Description,
# MAGIC case when trim(culture) = 'he' then Description end as he,
# MAGIC case when trim(culture) = 'th' then Description end as th,
# MAGIC case when trim(culture) = 'fr' then Description end as fr,
# MAGIC case when trim(culture) = 'ar' then Description end as ar,
# MAGIC case when trim(culture) = 'zh-cht' then Description end as zh_cht,
# MAGIC case when trim(culture) = 'en' then Description end as en
# MAGIC from(
# MAGIC   select pmd.Product_Model_ID,Culture, pd.Description
# MAGIC   from silver.productmodelproductdescription pmd
# MAGIC   inner join silver.productdescription pd on pmd.Product_Description_ID = pd.Product_Description_ID 
# MAGIC ))
# MAGIC group by Product_Model_ID
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC monotonically_increasing_id() as Pk_product,
# MAGIC p.Name_Product,
# MAGIC p.Product_Number,
# MAGIC p.color,
# MAGIC p.Standard_Cost,
# MAGIC p.List_Price,
# MAGIC p.Size,
# MAGIC p.Weight,
# MAGIC p.Sell_Start_Date,
# MAGIC p.Sell_End_Date,
# MAGIC p.Discontinued_Date, 
# MAGIC pc.Name_ProductCategory,
# MAGIC pm.Name_ProductModel,
# MAGIC cd.he_description, cd.th_description, cd.fr_description, cd.ar_description, cd.zh_cht_description, cd.en_description
# MAGIC from silver.product p
# MAGIC inner join silver.productcategory pc on p.Product_Category_ID = pc.Product_Category_ID
# MAGIC left join silver.productmodel pm on p.Product_Model_ID = pm.Product_Model_ID
# MAGIC left join culter_description cd on pm.Product_Model_ID = cd.Product_Model_ID
# MAGIC group by
# MAGIC p.Name_Product,
# MAGIC p.Product_Number,
# MAGIC p.color,
# MAGIC p.Standard_Cost,
# MAGIC p.List_Price,
# MAGIC p.Size,
# MAGIC p.Weight,
# MAGIC p.Sell_Start_Date,
# MAGIC p.Sell_End_Date,
# MAGIC p.Discontinued_Date, 
# MAGIC pc.Name_ProductCategory,
# MAGIC pm.Name_ProductModel,
# MAGIC cd.he_description, cd.th_description, cd.fr_description, cd.ar_description, cd.zh_cht_description, cd.en_description
# MAGIC

# COMMAND ----------


join_table = spark.sql('''

with culter_description as (

select Product_Model_ID, max(he) he_description, max(th) th_description, max(fr) fr_description, max(ar) ar_description, max(zh_cht) zh_cht_description, max(en) en_description
from (
select 
Product_Model_ID,
Culture,
Description,
case when trim(culture) = 'he' then Description end as he,
case when trim(culture) = 'th' then Description end as th,
case when trim(culture) = 'fr' then Description end as fr,
case when trim(culture) = 'ar' then Description end as ar,
case when trim(culture) = 'zh-cht' then Description end as zh_cht,
case when trim(culture) = 'en' then Description end as en
from(
  select pmd.Product_Model_ID,Culture, pd.Description
  from silver.productmodelproductdescription pmd
  inner join silver.productdescription pd on pmd.Product_Description_ID = pd.Product_Description_ID 
))
group by Product_Model_ID

)

select 
monotonically_increasing_id() as Pk_Product,
p.Name_Product,
p.Product_Number,
p.color,
p.Standard_Cost,
p.List_Price,
p.Size,
p.Weight,
p.Sell_Start_Date,
p.Sell_End_Date,
p.Discontinued_Date, 
pc.Name_ProductCategory,
pm.Name_ProductModel,
cd.he_description, 
cd.th_description, 
cd.fr_description, 
cd.ar_description, 
cd.zh_cht_description, 
cd.en_description

from silver.product p
inner join silver.productcategory pc on p.Product_Category_ID = pc.Product_Category_ID
left join silver.productmodel pm on p.Product_Model_ID = pm.Product_Model_ID
left join culter_description cd on pm.Product_Model_ID = cd.Product_Model_ID
group by
p.Name_Product,
p.Product_Number,
p.Color,
p.Standard_Cost,
p.List_Price,
p.Size,
p.Weight,
p.Sell_Start_Date,
p.Sell_End_Date,
p.Discontinued_Date, 
pc.Name_ProductCategory,
pm.Name_ProductModel,
cd.he_description, cd.th_description, cd.fr_description, cd.ar_description, cd.zh_cht_description, cd.en_description

''') 


output_path='/mnt/gold/SalesLT/dim_product'+'/' 
# join_table.write.format('delta').mode('overwrite').save(output_path)
join_table.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(output_path)



# COMMAND ----------


# Este bloque de codigo crea un copia virtual de mis tablas en el datalake de azure para pegarlas en el catalogo de databricks y poderlas consumir mas facil con SQL

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Se crea la dim_customer
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  
# MAGIC   monotonically_increasing_id() as Pk_customer,
# MAGIC   Name_Style,
# MAGIC   Title,
# MAGIC   First_Name,
# MAGIC   Middle_Name,
# MAGIC   Last_Name,
# MAGIC   Suffix,
# MAGIC   Company_Name,
# MAGIC   Sales_Person,
# MAGIC   Email_Address,
# MAGIC   Phone,
# MAGIC   Password_Hash,
# MAGIC   Password_Hash
# MAGIC from silver.customer 
# MAGIC group by   
# MAGIC   Name_Style,
# MAGIC   Title,
# MAGIC   First_Name,
# MAGIC   Middle_Name,
# MAGIC   Last_Name,
# MAGIC   Suffix,
# MAGIC   Company_Name,
# MAGIC   Sales_Person,
# MAGIC   Email_Address,
# MAGIC   Phone,
# MAGIC   Password_Hash,
# MAGIC   Password_Hash
# MAGIC

# COMMAND ----------

join_table = spark.sql(''' 

select  
  monotonically_increasing_id() as Pk_Customer,
  Name_Style,
  Title,
  First_Name,
  Middle_Name,
  Last_Name,
  Suffix,
  Company_Name,
  Sales_Person,
  Email_Address,
  Phone,
  Password_Hash

from silver.customer 
group by   
  Name_Style,
  Title,
  First_Name,
  Middle_Name,
  Last_Name,
  Suffix,
  Company_Name,
  Sales_Person,
  Email_Address,
  Phone,
  Password_Hash 

''')



output_path='/mnt/gold/SalesLT/dim_customer'+'/' 
# join_table.write.format('delta').mode('overwrite').save(output_path)
join_table.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(output_path)


# COMMAND ----------

# Este bloque de codigo crea un copia virtual de mis tablas en el datalake de azure para pegarlas en el catalogo de databricks y poderlas consumir mas facil con SQL 

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Se crea la dim_address

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC monotonically_increasing_id() as Pk_Address,
# MAGIC address_Line1,
# MAGIC address_Line2,
# MAGIC city,
# MAGIC state_Province,
# MAGIC country_Region,
# MAGIC postal_Code
# MAGIC from silver.address
# MAGIC group by
# MAGIC address_Line1,
# MAGIC address_Line2,
# MAGIC city,
# MAGIC state_Province,
# MAGIC country_Region,
# MAGIC postal_Code 

# COMMAND ----------

join_table = spark.sql(''' 

select
Monotonically_increasing_id() as Pk_Address,
Address_Line1,
Address_Line2,
City,
State_Province,
Country_Region,
Postal_Code

from silver.address
group by
address_Line1,
address_Line2,
city,
state_Province,
country_Region,
postal_Code 

''')

output_path='/mnt/gold/SalesLT/dim_address'+'/' 
join_table.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(output_path)
# join_table.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC #Se crea la dim_tiempo

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW FechasTemp AS
# MAGIC SELECT DATE_ADD('2008-01-01', n) AS Fecha
# MAGIC FROM (
# MAGIC     SELECT posexplode(sequence(to_date('2008-01-01'), current_date(), interval 1 day)) AS (n, _) -- Generamos una secuencia de fechas hasta la fecha actual
# MAGIC ) t;
# MAGIC
# MAGIC -- Creamos la dimensión de tiempo a partir de la tabla temporal
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW DimTiempo AS
# MAGIC SELECT
# MAGIC     monotonically_increasing_id() AS pk_tiempo,
# MAGIC     fecha,
# MAGIC     year(Fecha) AS ano,
# MAGIC     month(Fecha) AS mes,
# MAGIC     day(Fecha) AS dia,
# MAGIC     CASE month(Fecha)
# MAGIC         WHEN 1 THEN 'Enero'
# MAGIC         WHEN 2 THEN 'Febrero'
# MAGIC         WHEN 3 THEN 'Marzo'
# MAGIC         WHEN 4 THEN 'Abril'
# MAGIC         WHEN 5 THEN 'Mayo'
# MAGIC         WHEN 6 THEN 'Junio'
# MAGIC         WHEN 7 THEN 'Julio'
# MAGIC         WHEN 8 THEN 'Agosto'
# MAGIC         WHEN 9 THEN 'Septiembre'
# MAGIC         WHEN 10 THEN 'Octubre'
# MAGIC         WHEN 11 THEN 'Noviembre'
# MAGIC         WHEN 12 THEN 'Diciembre'
# MAGIC     END AS nombre_mes,
# MAGIC     CASE date_format(Fecha, 'E')
# MAGIC         WHEN 'Mon' THEN 'Lunes'
# MAGIC         WHEN 'Tue' THEN 'Martes'
# MAGIC         WHEN 'Wed' THEN 'Miércoles'
# MAGIC         WHEN 'Thu' THEN 'Jueves'
# MAGIC         WHEN 'Fri' THEN 'Viernes'
# MAGIC         WHEN 'Sat' THEN 'Sábado'
# MAGIC         WHEN 'Sun' THEN 'Domingo'
# MAGIC     END AS nombre_dia_semana
# MAGIC FROM
# MAGIC     FechasTemp;
# MAGIC

# COMMAND ----------

# Create a temporary view with a single column 'Fecha' that contains all the needed dates
spark.sql('''
CREATE OR REPLACE TEMPORARY VIEW FechasTemp AS
SELECT DATE_ADD('2008-01-01', n) AS Fecha
FROM (
    SELECT posexplode(sequence(to_date('2008-01-01'), current_date(), interval 1 day)) AS (n, _)
) t
''')

# Create the time dimension from the temporary view
spark.sql('''
SELECT
    monotonically_increasing_id() AS Pk_Tiempo,
    Fecha,
    year(Fecha) AS Ano,
    month(Fecha) AS Mes,
    day(Fecha) AS Dia,
    CASE month(Fecha)
        WHEN 1 THEN 'Enero'
        WHEN 2 THEN 'Febrero'
        WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Mayo'
        WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
    END AS Nombre_Mes,
    CASE date_format(Fecha, 'E')
        WHEN 'Mon' THEN 'Lunes'
        WHEN 'Tue' THEN 'Martes'
        WHEN 'Wed' THEN 'Miércoles'
        WHEN 'Thu' THEN 'Jueves'
        WHEN 'Fri' THEN 'Viernes'
        WHEN 'Sat' THEN 'Sábado'
        WHEN 'Sun' THEN 'Domingo'
    END AS Nombre_Dia_Semana

FROM
    FechasTemp
''').write.format('delta').mode('overwrite').option("mergeSchema", "true").save('/mnt/gold/SalesLT/dim_tiempo/')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS FechasTemp;

# COMMAND ----------


# Este bloque de codigo crea un copia virtual de mis tablas en el datalake de azure para pegarlas en el catalogo de databricks y poderlas consumir mas facil con SQL

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Se crea la fact_order

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC monotonically_increasing_id() as pk_order,
# MAGIC md5(CAST(h.Sales_Order_ID AS BINARY)) key_sales_detail,
# MAGIC h.revision_number,
# MAGIC h.status,
# MAGIC h.online_order_flag,
# MAGIC h.sales_order_number,
# MAGIC h.account_number,
# MAGIC h.credit_card_approval_code,
# MAGIC h.sub_total,
# MAGIC h.tax_amt,
# MAGIC h.freight,
# MAGIC h.total_Due,
# MAGIC dc.PK_Customer as fk_customer,
# MAGIC da.Pk_address as fk_customer_address,
# MAGIC da2.Pk_address as fk_bill_address,
# MAGIC da3.Pk_address as fk_ship_address,
# MAGIC dt.pk_tiempo as fk_ship_date,
# MAGIC dt2.pk_tiempo as fk_order_date,
# MAGIC dt3.pk_tiempo as fk_due_date
# MAGIC
# MAGIC from silver.salesorderheader h
# MAGIC inner join silver.customer c                on h.Customer_ID = c.Customer_ID
# MAGIC inner join silver.customerAddress ca        on c.Customer_ID = ca.Customer_ID
# MAGIC inner join silver.address a                 on ca.Address_ID = a.Address_ID
# MAGIC inner join silver.address a2                on h.Bill_To_Address_ID = a2.Address_ID
# MAGIC inner join silver.address a3                on h.Ship_To_Address_ID = a3.Address_ID
# MAGIC left join gold.dim_customer dc              on dc.Email_Address = c.Email_Address
# MAGIC left join gold.dim_address da               on concat(da.Address_Line1, coalesce(da.Address_Line2,''), da.city , da.Country_Region) = concat(a.Address_Line1, coalesce(a.Address_Line2,''), a.city , a.Country_Region)
# MAGIC left join gold.dim_address da2              on concat(da2.Address_Line1, coalesce(da2.Address_Line2,''), da2.city , da2.Country_Region) = concat(a2.Address_Line1, coalesce(a2.Address_Line2,''), a2.city , a2.Country_Region)
# MAGIC left join gold.dim_address da3              on concat(da3.Address_Line1, coalesce(da3.Address_Line2,''), da3.city , da3.Country_Region) = concat(a3.Address_Line1, coalesce(a3.Address_Line2,''), a3.city , a3.Country_Region)
# MAGIC left join gold.dim_tiempo dt                on h.Ship_Date = dt.fecha
# MAGIC left join gold.dim_tiempo dt2               on h.Order_Date = dt2.fecha
# MAGIC left join gold.dim_tiempo dt3               on h.Due_Date = dt3.fecha
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- from silver.salesorderdetail d
# MAGIC -- inner join silver.product p on d.Product_ID = p.Product_ID
# MAGIC -- inner join silver.productmodel pm on p.Product_Model_ID = pm.Product_Model_ID
# MAGIC -- inner join silver.productmodelproductdescription pmd on pm.Product_Model_ID = pmd.Product_Model_ID
# MAGIC -- inner join silver.productdescription pd on pmd.Product_Description_ID = pd.Product_Description_ID
# MAGIC -- inner join silver.productcategory pc on p.Product_Category_ID = pc.Product_Category_ID
# MAGIC
# MAGIC --3246

# COMMAND ----------

join_table = spark.sql(''' 

select 
monotonically_increasing_id() as Pk_Order,
md5(CAST(h.Sales_Order_ID AS BINARY)) Key_Order_Detail,
h.Revision_Number,
h.Status,
h.Online_Order_Flag,
h.Sales_Order_Number,
h.Account_Number,
h.Credit_Card_Approval_Code,
h.Sub_Total,
h.Tax_Amt,
h.Freight,
h.Total_Due,
dc.PK_Customer as Fk_Customer,
da.Pk_Address as Fk_Customer_Address,
da2.Pk_address as Fk_Bill_Address,
da3.Pk_address as Fk_ship_Address,
dt.pk_tiempo as Fk_Ship_Date,
dt2.pk_tiempo as Fk_Order_Date,
dt3.pk_tiempo as Fk_Due_Date

from silver.salesorderheader h
inner join silver.customer c                on h.Customer_ID = c.Customer_ID
inner join silver.customerAddress ca        on c.Customer_ID = ca.Customer_ID
inner join silver.address a                 on ca.Address_ID = a.Address_ID
inner join silver.address a2                on h.Bill_To_Address_ID = a2.Address_ID
inner join silver.address a3                on h.Ship_To_Address_ID = a3.Address_ID
left join gold.dim_customer dc              on dc.Email_Address = c.Email_Address
left join gold.dim_address da               on concat(da.Address_Line1, coalesce(da.Address_Line2,''), da.city , da.Country_Region) = concat(a.Address_Line1, coalesce(a.Address_Line2,''), a.city , a.Country_Region)
left join gold.dim_address da2              on concat(da2.Address_Line1, coalesce(da2.Address_Line2,''), da2.city , da2.Country_Region) = concat(a2.Address_Line1, coalesce(a2.Address_Line2,''), a2.city , a2.Country_Region)
left join gold.dim_address da3              on concat(da3.Address_Line1, coalesce(da3.Address_Line2,''), da3.city , da3.Country_Region) = concat(a3.Address_Line1, coalesce(a3.Address_Line2,''), a3.city , a3.Country_Region)
left join gold.dim_tiempo dt                on h.Ship_Date = dt.fecha
left join gold.dim_tiempo dt2               on h.Order_Date = dt2.fecha
left join gold.dim_tiempo dt3               on h.Due_Date = dt3.fecha

''')

output_path='/mnt/gold/SalesLT/fact_orden'+'/' 
join_table.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(output_path)
# join_table.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

# Este bloque de codigo crea un copia virtual de mis tablas en el datalake de azure para pegarlas en el catalogo de databricks y poderlas consumir mas facil con SQL

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Se crea la Fact_Order_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   md5(cast(Sales_order_ID as BINARY)) as Key_order,
# MAGIC   Order_Qty,
# MAGIC   Unit_Price,
# MAGIC   Unit_Price_Discount,
# MAGIC   Line_Total,
# MAGIC   dp.Pk_product as Fk_Product
# MAGIC
# MAGIC from silver.salesorderdetail od
# MAGIC left join silver.product p         on od.Product_ID =  p.Product_ID
# MAGIC left join gold.dim_product dp      on concat(p.Name_Product,p.product_Number) = concat(dp.Name_Product,dp.product_Number)
# MAGIC

# COMMAND ----------

join_table = spark.sql(''' 

select
  md5(cast(Sales_order_ID as BINARY)) as Key_order,
  Order_Qty,
  Unit_Price,
  Unit_Price_Discount,
  Line_Total,
  dp.Pk_product as Fk_Product

from silver.salesorderdetail od
left join silver.product p         on od.Product_ID =  p.Product_ID
left join gold.dim_product dp      on concat(p.Name_Product,p.product_Number) = concat(dp.Name_Product,dp.product_Number)

''')

output_path='/mnt/gold/SalesLT/fact_order_detail'+'/' 
join_table.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(output_path)

# COMMAND ----------

# Este bloque de codigo crea un copia virtual de mis tablas en el datalake de azure para pegarlas en el catalogo de databricks y poderlas consumir mas facil con SQL

# Creo un vector(lista) vacio
table_name_gold_virtual = []

#lleno ese vector con con los nombre de todas las tablas silver
for i in dbutils.fs.ls('mnt/gold/SalesLT/'):
    table_name_gold_virtual.append(i.name.split('/')[0])

for i in table_name_gold_virtual:
    spark.sql(f""" 
    CREATE TABLE IF NOT EXISTS gold.{i}
    USING delta
    OPTIONS ('path' 'abfss://gold@datalakegen2projectdgkd.dfs.core.windows.net/SalesLT/{i}/')
    """)
