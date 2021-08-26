-- Databricks notebook source
-- MAGIC %md #Task 
-- MAGIC * Using the Databricks community edition notebook, the language of your choice (SQL, Python, Scala), and the data source of your choice, build an ETL chain to design and deliver the data model that would cover a certain area of human knowledge or answer a specific question.
-- MAGIC * Using the reporting tool of your choice (or Databricks built-in visualizations), create a report that would state the problem and demonstrate your findings.

-- COMMAND ----------

-- MAGIC %md ***Questions***
-- MAGIC * Provide an overview UK Long Term Interest Rate
-- MAGIC * Calculate and show the trend line over the period
-- MAGIC * Calculate and show the quarter on quarter changes in interest rate?
-- MAGIC * Do UK and US long term rates correlate?
-- MAGIC * Calculate the yearly moving average of the US Monthly Long term rate?
-- MAGIC * Summarise findings 
-- MAGIC 
-- MAGIC ****The Datasets****
-- MAGIC * Quarterly long term interest rate (Bond yield) of the UK since 1984 sourced from Datahub. 
-- MAGIC * Monthly long term interest rate (Bond yield) of the US since 1953 sourced from Datahub

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **DATA MODEL**

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS model_database;

-- COMMAND ----------

CREATE OR REPLACE TABLE model_database.BONDS_MODEL AS (
WITH UK_AVG_RATES AS (
      SELECT DATE_PART('YEAR', REPORT_DATE) YEAR, 
       AVG(RATE) AS AVERAGE_UK_BONDS_RATE 
       FROM  RAW_DATABASE.uk_quarterly_bond_yields
       GROUP BY YEAR),
     US_AVG_RATES AS (
      SELECT DATE_PART('YEAR', REPORT_DATE) YEAR, 
       AVG(RATE) AS AVERAGE_US_BONDS_RATE 
       FROM  RAW_DATABASE.us_monthly_bond_yields
       GROUP BY YEAR)
SELECT US_AVG_RATES.YEAR, AVERAGE_US_BONDS_RATE, AVERAGE_UK_BONDS_RATE FROM US_AVG_RATES
LEFT JOIN UK_AVG_RATES 
ON US_AVG_RATES.YEAR = UK_AVG_RATES.YEAR
WHERE US_AVG_RATES.YEAR IS NOT NULL AND UK_AVG_RATES.YEAR IS NOT NULL)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Provide an overview 10-yr UK Long Term Interest Rate**
-- MAGIC * In the chart, we provide a simple visualisation of the dataset. 
-- MAGIC * Given this chart we can already assume a downward trend in long term interest rates over the quarters observed

-- COMMAND ----------

SELECT 
  REPORT_DATE, 
  RATE  || '%' AS INTEREST_RATE, 
  LAG(RATE) OVER (ORDER BY REPORT_DATE) AS PREVIOUS_RATE,
  (RATE -  LAG(RATE) OVER (ORDER BY REPORT_DATE))/ LAG(RATE) OVER (ORDER BY REPORT_DATE) * 100 AS QoQ_change 
FROM RAW_DATABASE.UK_QUARTERLY_BOND_YIELDS
ORDER BY REPORT_DATE ASC 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Calculate and show the trend line over the period**
-- MAGIC * In the chart above we use linear regression as a method to show the trend line of data over the years. The result is consistent with the overview we got in chart 1. 
-- MAGIC * A trend line is more powerful when the overall tendency of the dataset is not obvious

-- COMMAND ----------

WITH CALCULATED_VARIABLES AS (
  SELECT DATE_PART('YEAR', REPORT_DATE) AS YEAR_GROUP,
        AVG(RATE) OVER (PARTITION BY YEAR(REPORT_DATE)) AS ybar,
        RATE AS y,
        AVG(DATEDIFF((SELECT MIN(REPORT_DATE) FROM RAW_DATABASE.UK_QUARTERLY_BOND_YIELDS),REPORT_DATE)) OVER (PARTITION BY DATE_PART('YEAR', REPORT_DATE)) AS xbar,
        DATEDIFF((SELECT MIN(REPORT_DATE) FROM RAW_DATABASE.UK_QUARTERLY_BOND_YIELDS),REPORT_DATE) AS x
  FROM RAW_DATABASE.UK_QUARTERLY_BOND_YIELDS
),
  TREND_SLOPE AS (
  SELECT MAX(xbar) AS xbar_max, MAX(ybar) AS ybar_max, sum((x-xbar)*(y-ybar))/sum((x-xbar)*(x-xbar)) AS slope
  FROM CALCULATED_VARIABLES)

 SELECT CALCULATED_VARIABLES.* ,
   (CALCULATED_VARIABLES.x * (SELECT slope FROM TREND_SLOPE) + (SELECT ((ybar_max - xbar_max) * slope) FROM TREND_SLOPE)) AS TREND_LINE
  FROM CALCULATED_VARIABLES

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Calculate and show the quarter on quarter changes in interest rate**
-- MAGIC * QoQ changes is a good metric to under the degree of volatility 

-- COMMAND ----------

SELECT 
  REPORT_DATE, 
  RATE  || '%' , 
  LAG(RATE) OVER (ORDER BY REPORT_DATE) AS PREVIOUS_RATE,
  ((RATE -  LAG(RATE) OVER (ORDER BY REPORT_DATE))/ LAG(RATE) OVER (ORDER BY REPORT_DATE) * 100) || '%' AS QOQ_PERCENTAGE_CHANGE
FROM RAW_DATABASE.UK_QUARTERLY_BOND_YIELDS
ORDER BY REPORT_DATE ASC 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Do UK and US long term rates correlate?**
-- MAGIC * With a value  close to 1, we can confirm that the US and UK interest rates  are highly correlated since 1985

-- COMMAND ----------

WITH CALCULATED_VARIABLES AS (
    SELECT 
    SUM(AVERAGE_US_BONDS_RATE) US_SUM, 
    SUM(AVERAGE_UK_BONDS_RATE) UK_SUM,
    SUM(AVERAGE_US_BONDS_RATE * AVERAGE_US_BONDS_RATE) AS US_SUM_SQUARED,
    SUM(AVERAGE_UK_BONDS_RATE * AVERAGE_UK_BONDS_RATE) AS UK_SUM_SQUARED,
    SUM(AVERAGE_US_BONDS_RATE * AVERAGE_UK_BONDS_RATE) AS TOTAL_SUM,
    COUNT(*) AS ROW_COUNT FROM model_database.BONDS_MODEL
  )
SELECT ((TOTAL_SUM - (US_SUM * UK_SUM / ROW_COUNT)) / SQRT((US_SUM_SQUARED - POWER(US_SUM, 2.0) / ROW_COUNT) * (UK_SUM_SQUARED - POWER(UK_SUM, 2.0) / ROW_COUNT))) AS CORRELATION_COEFFICIENT
FROM CALCULATED_VARIABLES

-- COMMAND ----------

-- MAGIC %md ##Summary 
-- MAGIC * The UK and US Long term rates have both been on the decline over the years measured
-- MAGIC * We see a strong correlation the rates for both countries
-- MAGIC * As a follow up, we can look into the root cause of the and main effects from it