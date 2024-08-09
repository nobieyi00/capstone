# Databricks notebook source
# MAGIC %md
# MAGIC Read in the dataframe from the json file. Please note that the Json file could have incremental and full load data with varying schema.
# MAGIC In our approach, we could have a full load file with no incremental file. hence it is possible that the metadata field like Action Type would not be available in the final dataframe
# MAGIC
# MAGIC We can also have a situation where we have a full load and incremental load file present. in that case the ActionType values for the records from the full load file will be null. 
# MAGIC
# MAGIC The business rule is to default the Action Type to Insert if null and add an Action Type field if it is missing.

# COMMAND ----------

from pyspark.sql.functions import *
import os
from pyspark.sql import DataFrame

# COMMAND ----------

landingpath = "abfss://landing@spring2024adls.dfs.core.windows.net/input/"
refinedpath =  "abfss://refined@spring2024adls.dfs.core.windows.net/Goldzone/"
governancepath = "abfss://governance@spring2024adls.dfs.core.windows.net/"

# COMMAND ----------

# Generate widget for variable string
dbutils.widgets.text("tablename", "Product")
tablename = dbutils.widgets.get("tablename")

# COMMAND ----------

print(tablename)

# COMMAND ----------

# Check if incoming files exist, if none exist exit notebook
from pyspark.sql.functions import lit

folder_path = f"abfss://landing@spring2024adls.dfs.core.windows.net/input/{tablename}"
file_list = dbutils.fs.ls(folder_path)

if not file_list:
    dbutils.notebook.exit("No incoming files found in the specified folder path.")

# COMMAND ----------

df = spark.read.option ('multiline',"true").option('inferSchema', True).json(f"{governancepath}PrimaryKey_config.json")
display(df)

# COMMAND ----------

filtered_df = df.filter(df['TableName'] == tablename).select('PrimaryKey')
primary_key_value = filtered_df.collect()[0]['PrimaryKey']
display(filtered_df)

# COMMAND ----------

print(primary_key_value)

# COMMAND ----------

df = spark.read.option('multiline', True).option('inferSchema', True).json(f"abfss://landing@spring2024adls.dfs.core.windows.net/input/{tablename}")

display(df)

# COMMAND ----------

if 'ActionType' in df.columns:
    #if actiontype field exist default null values to insert
    df = df.withColumn('ActionType', coalesce(col('ActionType'), lit('INSERT')))
else:
    #if actiontype  field does not exist create a new field and default it to insert
    df = df.withColumn('ActionType', lit('INSERT'))

df.display()

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Create a window specification to partition by addressid and order by modifieddate in descending order
windowSpec = Window.partitionBy(primary_key_value).orderBy(df["modifieddate"].desc())

# Add a row number column to identify the latest record for each addressid
df = df.withColumn("row_number", row_number().over(windowSpec))

# Filter out the duplicate records by keeping only the rows with row_number = 1
df = df.filter(df["row_number"] == 1)

# Drop the row_number column
df = df.drop("row_number")

df.display()

# COMMAND ----------

def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------


merge_condition = ''
for col in primary_key_value:
    merge_condition =  merge_condition + f" and target.{col} = source.{col}"
print(merge_condition[4:])



# COMMAND ----------



file_path = refinedpath + tablename

if file_exists(file_path):
    print("Delta file exists")
    from delta.tables import *
    target_df = DeltaTable.forPath(spark, refinedpath + tablename)
    
    (target_df.alias("target")
    .merge(df.alias("source"), merge_condition[4:])
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )



else:
    print("Delta file does not exist")
    df.write.format("delta").mode("overwrite").save(refinedpath + tablename)

# COMMAND ----------

#confirm load was successful
df = spark.read.format("delta").load(refinedpath + tablename)
display(df)

# COMMAND ----------

#archive incoming files
source_path = f"abfss://landing@spring2024adls.dfs.core.windows.net/input/{tablename}"
archive_path = f"abfss://landing@spring2024adls.dfs.core.windows.net/archive/{tablename}"

files = dbutils.fs.ls(source_path)

for file in files:
    dbutils.fs.mv(file.path, archive_path + "/" + file.name)
