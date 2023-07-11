# Databricks notebook source
from pyspark.sql.functions import explode_outer,col,sum
from pyspark.sql.types import StructType,StructField,LongType,ArrayType,StringType
from delta.tables import DeltaTable

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

df_explode_json.printSchema()

# COMMAND ----------

df_explode_json.select([sum(col(x).isNull().cast("integer")) .alias(x)for x in df_explode_json.columns]).show()

# COMMAND ----------


def saveToDeltaWithOverwrite1(resultDf, silverLayerPath,database_name,target_table_name,mergeCol):
    base_path=silverLayerPath+f"{database_name}/{target_table_name}"
    if not DeltaTable.isDeltaTable(spark,f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        resultDf.write.mode("overwrite").format("delta").option("path",base_path).saveAsTable(f"{database_name}.{target_table_name}")
    else:
        deltaTable = DeltaTable.forPath(spark, f"{base_path}")
        primaryKeys = " AND ".join("old." + col + " = new." + col for col in mergeCol)
        deltaTable.alias("old").merge(resultDf.alias("new"),primaryKeys).whenMatchedUpdateAll().whenNotMatchedInsertAll()\
        .execute()

# COMMAND ----------

saveToDeltaWithOverwrite1(df_explode_json,"/mnt/mountpointstorageadfmanohar/Silver","employee_db","emp_details",["id"])

# COMMAND ----------

def read_delta(fileformat,path):
    df_json=spark.read.format(fileformat).load(path)
    return df_json

# COMMAND ----------

df_delta=read_delta("delta","/mnt/mountpointstorageadfmanohar/Silveremployee_db/emp_details/")
display(df_delta)

# COMMAND ----------

df_table_1=df_delta.select("employee_age","employee_salary","id")
display(df_table_1)

# COMMAND ----------

df_table_2=df_delta.select("id","profile_image","message","status")
display(df_table_2)

# COMMAND ----------

def join_two_df(df1,df2,colname,jointype):
    df3=df1.join(df2,colname,jointype)
    return df3

# COMMAND ----------

df_join_two=join_two_df(df_table_1,df_table_2,"id",'inner')
display(df_join_two)

# COMMAND ----------

def write_into_gold(df,fileformat,mode,path):
    df_write=df.write.format(fileformat).mode(mode).save(path)
    return df_write


# COMMAND ----------

write_into_gold(df_join_two,"parquet","append","/mnt/mountpointstorageadfmanohar/Gold")

# COMMAND ----------

def dividing_into_two_tables(df,lowervalue,uppervalue):
    df1=df.filter(df.employee_age.between(lowervalue,uppervalue))
    return df1

# COMMAND ----------

df_between_thirty=dividing_into_two_tables(df_explode_json,lowervalue=0,uppervalue=50)
display(df_between_thirty)

# COMMAND ----------

df_above_50=dividing_into_two_tables(df_explode_json,lowervalue=50,uppervalue=100)
display(df_above_50)

# COMMAND ----------

df_explode_json.filter(df_explode_json.employee_age>100)