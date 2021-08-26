# Databricks notebook source
# MAGIC %md #Task 
# MAGIC * Using the Databricks community edition notebook, the language of your choice (SQL, Python, Scala), and the data source of your choice, build an ETL chain to design and deliver the data model that would cover a certain area of human knowledge or answer a specific question.
# MAGIC * Using the reporting tool of your choice (or Databricks built-in visualizations), create a report that would state the problem and demonstrate your findings.
# MAGIC 
# MAGIC ** My Approach **
# MAGIC * As a first step, i use the Datahubapi to fetch some interest rate data for UK and US
# MAGIC * Pyspark and SQL are used to process and analyse the data 
# MAGIC * for ease of testing, everything (notebooks, visualization) is done on databricks

# COMMAND ----------

# MAGIC %md ### Workflow
# MAGIC * Run the "data_integration_{country_code}" notebooks (in any order) to fetch data and create the tables needed in the analysis scripts
# MAGIC * The "analysis" notebook can then be run to build our model and subsquent analysis. Here we have all our queries, charts and summary
# MAGIC * Alternative to the steps above the run_pipeline notebook can run all but the user paths will need to be redefined. replace {databricks_user_name} with yours 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Notes
# MAGIC * Data cleaning/transformation: We use a raw to model two layer approach. 
# MAGIC    * In the RAW database, we only ingest data without cleaning and minimal transformation.
# MAGIC    * The Model database is where we then combine both dataset from UK and US and create the bonds_model
# MAGIC    
# MAGIC * In a production enviroment, we would not duplicate the data integration notebooks and use a parameterised one

# COMMAND ----------

# MAGIC %md ***Questions***
# MAGIC * Provide an overview UK Long Term Interest Rate
# MAGIC * Calculate and show the trend line over the period
# MAGIC * Calculate and show the quarter on quarter changes in interest rate?
# MAGIC * Do UK and US long term rates correlate?
# MAGIC * Calculate the yearly moving average of the US Monthly Long term rate?
# MAGIC * Summarise findings 
# MAGIC 
# MAGIC ****The Datasets****
# MAGIC * Quarterly long term interest rate (Bond yield) of the UK since 1984 sourced from Datahub.  https://datahub.io/core/bond-yields-uk-10y
# MAGIC * Monthly long term interest rate (Bond yield) of the US since 1953 sourced from Datahub. https://datahub.io/core/bond-yields-us-10y