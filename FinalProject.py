# Databricks notebook source
# MAGIC %md ![image.png](attachment:image.png)
# MAGIC # <center>AdventureWorks Analytics Dashboard Project</center>
# MAGIC 
# MAGIC Useful link: 
# MAGIC 
# MAGIC https://stackoverflow.com/questions/29226210/what-is-the-spark-dataframe-method-topandas-actually-doing

# COMMAND ----------

# MAGIC %md ## Create a Spark Cluster to run your notebook on.

# COMMAND ----------

# MAGIC %md
# MAGIC ## You should already have uploaded the data into Databricks as tables.

# COMMAND ----------

# MAGIC %md ## Create the Dashboard Widgets...
# MAGIC See link for documentation: https://docs.databricks.com/user-guide/notebooks/widgets.html

# COMMAND ----------

# DBTITLE 1,Delete all widgets...
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Use DESCRIBE to get the list of column names...
# MAGIC %sql describe  factinternetsales

# COMMAND ----------

# MAGIC %md # Create Widgets...

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1) Create the following Notbeook Widgets...
# MAGIC 
# MAGIC - Drop Down List named FiscalYear that is loaded dynamically from the unique FiscalYear values in factinternetsales.  (You will need to join to dimdate) Default to "2014"
# MAGIC - Drop Down List named Country that is loaded dynmically from the unique list of EnglishCountryRegionName values in dimgeography. Default to "United Sates".
# MAGIC - Drop Down list named Category that is loaded dynamically from the unqiue ProductCategory values in dimproductcategory. Default to 'Bikes'

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE WIDGET DROPDOWN FiscalYear DEFAULT "2013" CHOICES 
# MAGIC select distinct FiscalYear 
# MAGIC from factinternetsales f
# MAGIC inner join dimdate d 
# MAGIC on  f.orderdatekey = d.datekey 
# MAGIC order by fiscalyear

# COMMAND ----------

# MAGIC 
# MAGIC %sql 
# MAGIC CREATE WIDGET DROPDOWN Country DEFAULT "United States" CHOICES 
# MAGIC select distinct EnglishCountryRegionName
# MAGIC from dimgeography

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE WIDGET DROPDOWN Category DEFAULT "Bikes" CHOICES 
# MAGIC select distinct EnglishProductCategoryName
# MAGIC from dimproductcategory

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) For product analysis, it will be a lot easier if we can get the Product information together, i.e. we want to have the product category, product subcategory, and product information together.  Lets create a SQL view that gives us these columns with the keys so we can easily do queries with them.  Note:  This is a good example of denormalizing data to make it easier to query.
# MAGIC 
# MAGIC #### Create a SQL view called *vproductinfo* that contains the following columns:
# MAGIC 
# MAGIC - ProductCategoryKey from dimproductcategory
# MAGIC - ProductSubcategoryKey from dimproductsubcategory
# MAGIC - ProductKey from dimproduct
# MAGIC - EnglishProductCategoryName renamed as Category from dimproductcategory
# MAGIC - EnglishProductSubcategoryName renamed as Subcategory from improductsubcategory
# MAGIC - ModelName as Model from dimproduct

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vproductinfo
# MAGIC   AS
# MAGIC Select DPC.ProductCategoryKey, DPSC.productsubcategorykey, DP.ProductKey, dpc.EnglishProductCategoryName as Category,DPSC.EnglishProductSubcategoryName as Subcategory, dp.ModelName as Model
# MAGIC from dimproductcategory DPC
# MAGIC inner join dimproductsubcategory DPSC on DPC.ProductCategoryKey = DPSC.ProductCategoryKey 
# MAGIC inner join dimproduct DP on DP.ProductSubcategoryKey = DPSC.productsubcategorykey

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vproductinfo

# COMMAND ----------

# MAGIC %md ## 3 A)  Create a SQL view named vsalesinfo that contains the followig data.
# MAGIC - SalesAmount from factinternetsales
# MAGIC - All the columns from vproductinfo (the view we created earlier)
# MAGIC - from dimcustomer, get EnglishEductionLevel renamed as Education
# MAGIC - from dimcustomer, get Gender
# MAGIC - from dimcustomer, get YearlyIncome as Salary
# MAGIC - from dimcustomer, if the customer has any children (NumberChildrenAtHome > 0), set to 'Y', else set to 'N' and call this column 'HasChildren'
# MAGIC - from dimcustomer, get HomeOwnerFlag as HomeOwner
# MAGIC - from dimcustomer, AgeBand as 
# MAGIC ``` CASE  WHEN age < 18 then 'Minor'
# MAGIC                        WHEN age between 19 and 29 then 'Young'
# MAGIC                        WHEN age between 30 and 39 then 'Middle'
# MAGIC                        WHEN age between 40 and 49 then 'Late Middle'
# MAGIC                        WHEN age > 50 then 'Golden'
# MAGIC                        ELSE 'Other' END as AgeBand"
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC describe dimcustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vdimcustomer as
# MAGIC select *, int((DATEDIFF(CURRENT_DATE, birthdate))/365) as age from dimcustomer;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW vsalesinfo as
# MAGIC 
# MAGIC Select FIS.SalesAmount, VPI.* , VC.EnglishEducAtion as Education, VC.Gender, VC.YearlyIncome as Salary, 
# MAGIC 
# MAGIC CASE WHEN NumberChildrenAtHome > 0 then  'y'
# MAGIC      else 'n' END AS HasChildren , 
# MAGIC 
# MAGIC VC.HouseOwnerFlag as HomeOwner,
# MAGIC               
# MAGIC CASE WHEN VC.age < 18 then 'Minor'
# MAGIC      WHEN VC.age between 19 and 29 then 'Young'
# MAGIC      WHEN VC.age between 30 and 39 then 'Middle'
# MAGIC      WHEN VC.age between 40 and 49 then 'Late Middle'
# MAGIC      WHEN VC.age > 50 then 'Golden'
# MAGIC      ELSE 'Other' END as AgeBand
# MAGIC                  
# MAGIC                  
# MAGIC from vdimcustomer VC
# MAGIC inner join factinternetsales FIS 
# MAGIC on (FIS.CustomerKey = VC.CustomerKey)
# MAGIC inner join vproductinfo VPI
# MAGIC on (FIS.ProductKey= VPI.ProductKey)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe vsalesinfo

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vsalesinfo

# COMMAND ----------

# MAGIC %md ## 3 B) Use SQL to dipslay the total sales amount by Education and then change the visual to a pie chart.

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(salesamount), education 
# MAGIC from vsalesinfo
# MAGIC group by education

# COMMAND ----------

# MAGIC %md ## 4) Convert the vsalesinfo view above into a PySpark dataframe. Use the display() function to show the data.

# COMMAND ----------

vsalesinfo_df = sqlContext.sql("SELECT * FROM vsalesinfo")
display(vsalesinfo_df)

# COMMAND ----------

# MAGIC %md ## 5) Use the PySpark dataframe you created in quetion 4 to return total sales by Gender and AgeBand and then display the data as the visual defined below.
# MAGIC 
# MAGIC ### Hint:  Example: display(diamonds_df.groupBy("color","size").sum("price").orderBy("color","size"))
# MAGIC 
# MAGIC 
# MAGIC - Adjust the Databricks visual output to show the results as:
# MAGIC - 2 Pie Charts 
# MAGIC - Slices = total sales by AgeBand
# MAGIC - Split by Gender, i.e. One pie per Gender.

# COMMAND ----------

display(vsalesinfo_df.groupBy("gender", "ageband").sum("salesamount").orderBy("gender","ageband"))

# COMMAND ----------

# MAGIC %md ## 6) Convert the PySpark dataframe from question 5 into a local pandas dataframe and display the few rows using the  head() method.

# COMMAND ----------


vsalesinfo_dfP = vsalesinfo_df.toPandas()
vsalesinfo_dfP.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Create a SQL view named vsalesbyproduct that gets the following information.
# MAGIC - from factinternetsales, get SalesAmount
# MAGIC - from dimdate, get FiscalYear 
# MAGIC - from dimdate, get FiscalQuarter
# MAGIC - from vproductinfo, get Category
# MAGIC - from vproductinfo, get Model

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW vsalesbyproduct
# MAGIC     AS
# MAGIC Select FIS.SalesAmount, DD.FiscalYear, DD.FiscalQuarter, VPI.Category, VPI.Model
# MAGIC from dimdate DD
# MAGIC inner join factinternetsales FIS
# MAGIC on (FIS.OrderDateKey= DD.DateKey)
# MAGIC inner join vproductinfo VPI 
# MAGIC on (FIS.ProductKey = VPI.ProductKey)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vsalesbyproduct

# COMMAND ----------

# MAGIC %md ## 8) Using SQL or Python query that data from the view created in step 7, i.e. vsalesbyproduct, filtering the Category on the widget Category value.  Then change the visual display to a bar chart by FiscalYear, by Model.  The model should display as a separate bar within the FiscalYear.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vsalesbyproduct

# COMMAND ----------

WFiscalYear = dbutils.widgets.get("FiscalYear") 
WCategory = dbutils.widgets.get("Category") 
query = "select * from vsalesbyproduct WHERE Category = '" + WCategory + "' and FiscalYear ='" + WFiscalYear + "'" 
vsalesbyproduct_df = spark.sql(query)
display(vsalesbyproduct_df)



# COMMAND ----------

# MAGIC %md ## 9) We want to get a visual of total sales amound by FiscalYear/FiscalQuarter together.  However, the Databricks visual does not support multiple columns to be used like this so we need to merge the FiscalYear and FiscalQuarter into one column so it looks like '2013-1', '2013-2'...etc.  
# MAGIC 
# MAGIC ### You will need to convert the FiscalYear and FiscalQuarter into strings using the SQL CAST function as shown below.
# MAGIC 
# MAGIC select CAST(FiscalYear as varchar(4))||"-"|| CAST(FiscalQuarter as varchar(10)) FiscalYrQtr 
# MAGIC 
# MAGIC You need to complete the above SQL statement below  to get the SalesAmount and Category and add a filter that filters Category to the widget dropdown list. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select RAND(5) as PlaceHolder, CAST(FiscalYear as varchar(4))||"-"|| CAST(FiscalQuarter as varchar(10)) FiscalYrQtr 
# MAGIC from dimdate           d 
# MAGIC GROUP BY FiscalYear, FiscalQuarter 
# MAGIC ORDER BY FiscalYear, FiscalQuarter

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- FIRST MAKE THIS SELECT QUERY, THEN TURN THE QUERY INTO A VIEW, THEN CREATE A DATAFRAME FROM THE VIEW AND USE AN EMBEDDED SQL STATEMENT TO ADD THE WIDET FEATURE ON THE DASHBOARD -- SEE THE BELOW CELLS!
# MAGIC select sum(fis.salesamount) as TotalSales, DPC.EnglishProductCategoryName, CAST(FiscalYear as varchar(4))||"-"|| CAST(FiscalQuarter as varchar(10)) FiscalYrQtr 
# MAGIC from factinternetsales FIS
# MAGIC inner join dimproduct DP
# MAGIC on(DP.productkey = FIS.productkey)
# MAGIC inner join dimproductsubcategory DPSC
# MAGIC on(DPSC.productsubcategorykey = DP.productsubcategorykey)
# MAGIC inner join dimproductcategory DPC
# MAGIC on(DPC.productcategorykey = DPSC.productcategorykey)
# MAGIC inner join dimdate DD
# MAGIC on(DD.DateKey = FIS.orderdatekey)
# MAGIC group by dpc.EnglishProductCategoryName, FiscalYrQTR
# MAGIC ORDER BY FiscalYrQtr

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC --create a view based on the query, all of the joins were needed because the one of the tables was missing a very imporant foreign key 
# MAGIC --dimproduct, thats why all the inner joins were needed
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW vTSFYQTR
# MAGIC     AS
# MAGIC select sum(FIS.SalesAmount) as TotalSales, DPC.EnglishProductCategoryName, CAST(FiscalYear as varchar(4))||"-"|| CAST(FiscalQuarter as varchar(10)) FiscalYrQtr 
# MAGIC from factinternetsales FIS 
# MAGIC inner join dimproduct DP 
# MAGIC on(DP.productkey = FIS.productkey) 
# MAGIC inner join dimproductsubcategory DPSC 
# MAGIC on(DPSC.productsubcategorykey = DP.productsubcategorykey) 
# MAGIC inner join dimproductcategory DPC 
# MAGIC on(DPC.productcategorykey = DPSC.productcategorykey) 
# MAGIC inner join dimdate DD 
# MAGIC on(DD.DateKey = FIS.orderdatekey) 
# MAGIC group by dpc.EnglishProductCategoryName, FiscalYrQTR

# COMMAND ----------

#transform the view into a pyspark df and add the widget feautre to the dataframe
WCategory = dbutils.widgets.get("Category") 
Aquery = "select * from vTSFYQTR WHERE EnglishProductCategoryName = '" + WCategory + "' ORDER BY FiscalYRQTR"
vSBP_df = spark.sql(Aquery)
display(vSBP_df)

# COMMAND ----------

# MAGIC %md ## 10) Pin all the visualizations to a new Dashboard named 'AW Exec' and confirm the dashboard functions.  
# MAGIC ## When a widget changes value, the related visualization should be updatred.

# COMMAND ----------

# I seriously cannot believe that I managed to do all of this on my own, it finally clicked!
#Thank you for all of the help this semester, there is no way that i could have completed this without meeting with you and working through some key issues together
#i genuienly did not realize how powerful this software is until today, its amazing
