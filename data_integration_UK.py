# Databricks notebook source
import requests 
import json 
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

class DatabhubAPI:
  def __init__(self, url):
    """
    Args:
      url (str): link for the data source
    """
    self.url = url
    
  def make_request(self):
    """
    Returns:
      response (json): 
    """
    url_call = self.url
    print(url_call)
    
    #Getting response
    print('**making request to API**')
    response=requests.get(url_call)
    if response.status_code != 200:
      print(response.text)
      raise Exception ('API call failed and returned code:' + str(response.status_code))
    response_json = response.json()
    return response_json
  
def json_to_dataframe(json, schema=None):
  """
  Args:
  json : json file to be converted
  
  Returns: spark dataframe
  """
  print('**converting json to spark df**')
  partitionNum = 100
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json], partitionNum))

def prepare_final_df(pyspark_df):
  """
  Args:
  pyspark_df : dataframe to prepare for the final insert step
  
  Returns: spark dataframe
  """
  print('**preparing  final dataframe**')
  if pyspark_df.count != 0:
    pyspark_df = pyspark_df.withColumn('SLICE_TIME', col('Date').cast('timestamp'))\
                    .withColumn('INSERT_TIME', current_timestamp())
    return pyspark_df
  else: raise Exception ('Dataframe is empty')

def slice_definition(df, timestamp_field_string):
  """
  Args:
  df : dataframe to check for time slices
  timestamp_field_string(str) : time field in dataframe
  
  Returns: spark dataframe
  """
  print('**extracting slice from data**')
  max_date = df.agg({str(timestamp_field_string): "max"}).collect()[0]
  min_date = df.agg({str(timestamp_field_string): "min"}).collect()[0]
  end_slice = max_date[0]
  start_slice = min_date[0]
  return  start_slice, end_slice

# COMMAND ----------

try: 
  url = 'https://pkgstore.datahub.io/core/bond-yields-uk-10y/quarterly_json/data/170473d2a939930586fb1762c6353a97/quarterly_json.json'
  api = DatabhubAPI(url)
  response_json = api.make_request()
  df = json_to_dataframe(response_json)
  final_df = prepare_final_df(df)
except Exception as error: print(error)

# COMMAND ----------

slices = slice_definition(final_df, 'Date')

# COMMAND ----------

final_df.registerTempTable("bond_yield_df") 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS  RAW_DATABASE;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS RAW_DATABASE.uk_quarterly_bond_yields  (REPORT_DATE DATE, RATE FLOAT, SLICE_TIME TIMESTAMP, INSERT_TIME TIMESTAMP);

# COMMAND ----------

sqlContext.sql("DELETE FROM raw_database.uk_quarterly_bond_yields WHERE report_date >= '{}' and report_date <= '{}'".format(slices[0],slices[1]))

# COMMAND ----------

sqlContext.sql("INSERT INTO raw_database.uk_quarterly_bond_yields (select * from bond_yield_df)")

# COMMAND ----------

# remove dbfs artifacts incase you have ran this multiple times
#dbutils.fs.rm("dbfs:/user/hive/warehouse/raw_database.db/uk_quarterly_bond_yields", True)