# Databricks notebook source
# Install necessary packages
!pip install kaggle
!pip install azure-identity
!pip install azure-keyvault-secrets
!pip install azure-storage-blob



# COMMAND ----------

# Import required modules
import os
import json
import zipfile
import subprocess
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, avg

# COMMAND ----------

# Configure logging for debug purposes
logging.basicConfig(filename='/RS_Transform_Notebook', level=logging.DEBUG, format='%(asctime)s %(message)s')

# COMMAND ----------

storage_account_name = dbutils.secrets.get(scope="Blob2brick_Secret_Scope", key="RetailSalesStorageAccountName")
storage_account_key = dbutils.secrets.get(scope="Blob2brick_Secret_Scope", key="RetailSalesStorageaccountkey")
container_name = dbutils.secrets.get(scope="Blob2brick_Secret_Scope", key="RetailCintainer")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)

# COMMAND ----------

dbutils.fs.unmount("/mnt/RetailSalesMount")

# COMMAND ----------

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = "/mnt/RetailSalesMount",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/RetailSalesMount')

# COMMAND ----------

SP_password = dbutils.secrets.get(scope="Blob2brick_Secret_Scope", key="RetailSalesSPsecret")
spark.conf.set("fs.azure.account.auth.type.rawretaindata.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rawretaindata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rawretaindata.dfs.core.windows.net", "e8cf5941-7604-4d6b-b10a-3dbe63128a90")
spark.conf.set("fs.azure.account.oauth2.client.secret.rawretaindata.dfs.core.windows.net", SP_password)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rawretaindata.dfs.core.windows.net", "https://login.microsoftonline.com/e75b8380-abd5-4a92-9bd9-41dffa92d385/oauth2/token")

# COMMAND ----------

df_SP = spark.read.csv("abfs://rawdatastaging@rawretaindata.dfs.core.windows.net/Amazon Sale Report.csv")

# COMMAND ----------

display(df_SP)

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Define the paths to the CSV files
mount_point = "/mnt/RetailSalesMount"
sale_report_path = f"{mount_point}/Sale Report.csv"
amazon_sale_report_path = f"{mount_point}/Amazon Sale Report.csv"
international_sale_report_path = f"{mount_point}/International sale Report.csv"

# Load the CSV files into DataFrames
df_sale_report = spark.read.csv(sale_report_path, header=True, inferSchema=True)
df_amazon_sale_report = spark.read.csv(amazon_sale_report_path, header=True, inferSchema=True)
df_international_sale_report = spark.read.csv(international_sale_report_path, header=True, inferSchema=True)

# Show the first few rows of each DataFrame to verify the content
df_sale_report.show()
df_amazon_sale_report.show()
df_international_sale_report.show()


# COMMAND ----------

display(df_sale_report)

# COMMAND ----------

display(df_amazon_sale_report)

# COMMAND ----------

display(df_international_sale_report)

# COMMAND ----------

#Data Cleaning and Preprocessing of Sale Report dataset

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Cleaning and Preprocessing of Sale Report dataset

# COMMAND ----------

from pyspark.sql.functions import col

# Check for null values with corrected column names
df_sale_report.select([col(f"`{c}`").isNull().alias(c) for c in df_sale_report.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Count null values for each column, ensuring column names with spaces are handled correctly
null_counts = df_sale_report.select(
    [(col(f"`{c}`").isNull().cast("int")).alias(c) for c in df_sale_report.columns]
).agg(
    *[sum(col(f"`{c}`")).alias(c) for c in df_sale_report.columns]
)

display(null_counts)


# COMMAND ----------

#Removing spaces in column name
df_sale_report = df_sale_report.toDF(*(c.replace(' ', '_').replace('.', '') for c in df_sale_report.columns))

# COMMAND ----------

#Dropping null vales
df_sale_report=df_sale_report.dropna()

# COMMAND ----------

# Display distinct values to find inconsistencies
for col_name in df_sale_report.columns:
    df_sale_report.select(col_name).distinct().show()


# COMMAND ----------

# Remove complete duplicate rows
df_sale_report = df_sale_report.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Standardize formats
df_sale_report = df_sale_report.withColumn('Category', trim(col('Category')))
df_sale_report = df_sale_report.withColumn('Size', trim(col('Size')))
df_sale_report = df_sale_report.withColumn('Color', trim(col('Color')))

# COMMAND ----------

# Validate numerical values in Stock column
df_sale_report = df_sale_report.filter(col('Stock') >= 0)

# COMMAND ----------

# Display the cleaned dataframe
display(df_sale_report)

# COMMAND ----------

#Stock Analysis
from pyspark.sql import functions as F

# Aggregate the data by Category and SKU_Code
df_aggregated = df_sale_report.groupBy('Category', 'Design_No','Color','Size').agg(
    F.sum('Stock').alias('Total_Stock')
)

# Display the aggregated data
display(df_aggregated)


# COMMAND ----------

# Sort by Category for better visualization
df_sorted = df_aggregated.orderBy('Category','Design_No','Color','Size')

# Display the aggregated and sorted data
display(df_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning, Preprocessing and transformation of International Sale Report

# COMMAND ----------

# Display the first few rows of the DataFrame
display(df_international_sale_report)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, count, when, isnull, lit, create_map
# Check for null values
null_counts = df_international_sale_report.select([count(when(isnull(c), c)).alias(c) for c in df_international_sale_report.columns])
display(null_counts)

# COMMAND ----------

# Filter rows where SKU is null
null_sku_df = df_international_sale_report.filter(col("SKU").isNull())

# Check if Style and Size are also null in those rows
null_sku_style_size_df = null_sku_df.select("Style", "Size").filter(col("Style").isNull() & col("Size").isNull())

# Count the rows where SKU, Style, and Size are all null
null_sku_style_size_count = null_sku_style_size_df.count()

# Display the result
print(f"Number of rows where SKU, Style, and Size are all null: {null_sku_style_size_count}")


# COMMAND ----------

# Drop rows where Style is null
df_ISR_cleaned = df_international_sale_report.filter(col("Style").isNotNull())

# Display the cleaned DataFrame
display(df_ISR_cleaned)


# COMMAND ----------

from pyspark.sql.functions import col, to_date, count, when, isnull, lit, create_map
# Check for null values
null_counts = df_ISR_cleaned.select([count(when(isnull(c), c)).alias(c) for c in df_ISR_cleaned.columns])
display(null_counts)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col

# Fill SKU with Style + Size
df_ISR_cleaned = df_ISR_cleaned.withColumn("SKU", concat_ws("-", col("Style"), col("Size")))

# Display the updated DataFrame
display(df_ISR_cleaned)


# COMMAND ----------

# Rename Style column to Design_No
df_ISR_cleaned = df_ISR_cleaned.withColumnRenamed("Style", "Design_No")

# COMMAND ----------

# MAGIC %md
# MAGIC ## There is no category column in International Sales Report but we can add if by joining Sales Report and international report
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

# Create a distinct DataFrame from df_sale_report with Design_No and Category
df_design_category = df_sale_report.select("Design_No", "Category").distinct()

# Perform a join operation to add the Category column to df_ISR
df_ISR = df_ISR_cleaned.join(df_design_category, on="Design_No", how="left")

# COMMAND ----------

display(df_ISR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Cleaning, Preprocessing and Transformation of Amazon Sales Report

# COMMAND ----------

# Display the first few rows of the DataFrame
display(df_amazon_sale_report)

# COMMAND ----------



# COMMAND ----------

# Step 1: Check for null values and handle them
null_counts_amazon = df_amazon_sale_report.select([count(when(isnull(c), c)).alias(c) for c in df_amazon_sale_report.columns])
display(null_counts_amazon)

# COMMAND ----------

# Clear intermediate results to manage notebook size
spark.catalog.clearCache()

# COMMAND ----------

# Rename columns to remove spaces and special characters
df_amazon_sale_report = df_amazon_sale_report.toDF(
    *[c.replace(' ', '_').replace('.', '').replace('(', '').replace(')', '') for c in df_amazon_sale_report.columns]
)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, trim, concat_ws, count, when, isnull, sum as _sum, mean, lit, expr, first
from pyspark.sql.window import Window

# Since 'Courier Status' and 'Currency' are categorical values Fill null values with most occurring value
most_occurring_courier_status = df_amazon_sale_report.groupBy("Courier_Status").count().orderBy("count", ascending=False).first()[0]
most_occurring_currency = df_amazon_sale_report.groupBy("Currency").count().orderBy("count", ascending=False).first()[0]

df_amazon_sale_report = df_amazon_sale_report.fillna({'Courier_Status': most_occurring_courier_status, 'Currency': most_occurring_currency})

# COMMAND ----------

# Fill null values in 'Amount' with the mean value
mean_amount = df_amazon_sale_report.agg(mean(col('Amount'))).first()[0]
df_amazon_sale_report = df_amazon_sale_report.fillna({'Amount': mean_amount})

# Drop rows with null values in 'Address'
df_amazon_sale_report = df_amazon_sale_report.dropna(subset=['ship-city'])

# Fill null values in 'Promotion' with 'No Promo'
df_amazon_sale_report = df_amazon_sale_report.fillna({'Promotion-ids': 'No_Promo'})

# Fill null values in 'Fulfilled_By' with 'Regular_Ship'
df_amazon_sale_report = df_amazon_sale_report.fillna({'fulfilled-by': 'Regular_Ship'})

# Drop the 'Unnamed_22' column
df_amazon_sale_report = df_amazon_sale_report.drop('Unnamed:_22')

# COMMAND ----------

# Drop the 'Unnamed_22' column
df_amazon_sale_report = df_amazon_sale_report.drop('Unnamed:_22')

# COMMAND ----------

# Standardize date formats
df_amazon_sale_report = df_amazon_sale_report.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))

# Ensure numerical columns are in the correct format
numerical_columns_amazon = ["Qty", "ship-postal-code", "Amount","index"]
for col_name in numerical_columns_amazon:
    df_amazon_sale_report = df_amazon_sale_report.withColumn(col_name, col(col_name).cast("double"))

# COMMAND ----------

# Rename specific columns to match other datasets
df_amazon_sale_report = df_amazon_sale_report.withColumnRenamed("Style", "Design_No")

# COMMAND ----------

df_amazon_sale_report.printSchema()

# COMMAND ----------

# List all secrets within the specified secret scope
secrets = dbutils.secrets.list("Blob2brick_Secret_Scope")

# Display the secrets
for secret in secrets:
    print(secret.key)


# COMMAND ----------

#SnowflakeUname SnowflakePwd

# COMMAND ----------

snowflake_user_name = dbutils.secrets.get(scope="Blob2brick_Secret_Scope", key="SnowflakeUname")
snowflake_pwd=dbutils.secrets.get(scope="Blob2brick_Secret_Scope", key="SnowflakePwd")

# COMMAND ----------

# Snowflake connection options
options = {
  "sfURL" : "https://jwtixta-pq56526.snowflakecomputing.com",
  "sfUser" : snowflake_user_name ,
  "sfPassword" : snowflake_pwd,
  "sfDatabase" : "RETAILSALESDB",
  "sfSchema" : "CLEANEDDATA",
  "sfWarehouse" : "RETAILSALES_WH",
  "dbtable": "SALE_REPORT"
}




# COMMAND ----------

df_sorted.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "SALE_REPORT") \
  .mode("overwrite") \
  .save()


# COMMAND ----------

df_ISR.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "INTERNATIONAL_SALE_REPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_amazon_sale_report.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AMAZON_SALE_REPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
