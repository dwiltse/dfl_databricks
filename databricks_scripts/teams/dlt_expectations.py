# Databricks notebook source
# MAGIC %md
# MAGIC Link to example: https://adb-3328355783918896.16.azuredatabricks.net/?o=3328355783918896#notebook/3731133622557732

# COMMAND ----------


# In this example, we'll store our rules as a delta table for more flexibility & reusability. 
# While this isn't directly related to Unit test, it can also help for programatical analysis/reporting.
 
data = [
 # tag/table name      name              constraint
 ("user_bronze_dlt",  "correct_schema", "_rescued_data IS NULL"),
 ("user_silver_dlt",  "valid_id",       "id IS NOT NULL AND id > 0"),
 ("spend_silver_dlt", "valid_id",       "id IS NOT NULL AND id > 0"),
 ("user_gold_dlt",    "valid_age",      "age IS NOT NULL"),
 ("user_gold_dlt",    "valid_income",   "annual_income IS NOT NULL"),
 ("user_gold_dlt",    "valid_score",    "spending_core IS NOT NULL")
]
#Typically only run once, this doesn't have to be part of the DLT pipeline.
spark.createDataFrame(data=data, schema=["tag", "name", "constraint"]).write.mode("overwrite").save("/demos/product/dlt_unit_test/expectations")

# COMMAND ----------

#Return the rules matching the tag as a format ready for DLT annotation.
from pyspark.sql.functions import expr, col
 
def get_rules(tag):
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.load("/demos/product/dlt_unit_test/expectations").where(f"tag = '{tag}'")
  for row in df.collect():
    rules[row['name']] = row['constraint']
  return rules
