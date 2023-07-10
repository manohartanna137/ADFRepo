# Databricks notebook source
from pyspark.sql.functions import explode_outer,col
from pyspark.sql.types import StructType,StructField,LongType,ArrayType,StringType

# COMMAND ----------

def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

mounting_layer_json('wasbs://adfjson@manstoragedemo.blob.core.windows.net/','/mnt/mountpointstorageadfmanohar','fs.azure.account.key.manstoragedemo.blob.core.windows.net','+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA==')


# COMMAND ----------

def read_json_bronze(fileformat,multiline,schema,path):
    df=spark.read.format(fileformat).option("multiline",multiline).schema(schema).load(path)
    return df

# COMMAND ----------

data_schema = StructType([
    StructField("employee_age", LongType()),
    StructField("employee_name", StringType()),
    StructField("employee_salary", LongType()),
    StructField("id", LongType()),
    StructField("profile_image", StringType())])
schema = StructType([
    StructField("data", ArrayType(data_schema)),
    StructField("message", StringType()),
    StructField("status", StringType())])

# COMMAND ----------

df_json_api=read_json_bronze("json","true",schema,'/mnt/mountpointstorageadfmanohar/Bronze/json_employee')
display(df_json_api)

# COMMAND ----------

df_json_api.printSchema()

# COMMAND ----------

df

# COMMAND ----------


def explode_data(df):
    exploded_df = df.withColumn("exploded_data", explode_outer(df.data))
    exploded_df = exploded_df.select(
        "exploded_data.employee_age",
        "exploded_data.employee_name",
        "exploded_data.employee_salary",
        "exploded_data.id",
        "exploded_data.profile_image",
        "message",
        "status"
    )
    return exploded_df


# COMMAND ----------

df_explode_json=explode_data(df_json_api)
display(df_explode_json)

# COMMAND ----------

def write_merge(df,fileformat,mode,path,mergeSchema=True):
    df_write=df.write.format(fileformat).mode(mode).option("mergeSchema", mergeSchema).save(path)
    return df_write

# COMMAND ----------

df_json_silver=write_merge(df_explode_json,"delta","overwrite","/mnt/mountpointstorageadfmanohar/Silver")

# COMMAND ----------

df_explode_json.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 

# COMMAND ----------

