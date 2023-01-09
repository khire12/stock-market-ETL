CREATE DATABASE STOCK_MARKET;
CREATE SCHEMA YAHOO;

USE DATABASE STOCK_MARKET;
USE SCHEMA YAHOO;



CREATE OR REPLACE TABLE yfinance_daily
(
 OPEN DOUBLE, 
 HIGH DOUBLE, 
 LOW DOUBLE, 
 CLOSE DOUBLE, 
 ADJ_CLOSE DOUBLE,
 VOLUME INTEGER,
 STOCK_INDEX VARCHAR,
 DATE_ DATE
);


CREATE STORAGE INTEGRATION stock_market_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<AWS IAM Role ARN>' // the one that was created previously
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket name>/<folder name>/');



CREATE STAGE stock_martket_s3_stage
  URL = 's3://<bucket name>/<folder name>/'
  STORAGE_INTEGRATION = stock_market_s3_integration;


CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = csv
  FIELD_DELIMETER = ','
  SKIP_HEADER = 1
  EMPTY_FIELD_AS_NULL = true;


CREATE OR REPLACE PIPE stock_market.yahoo.stock_market_snowpipe
AUTO_INGEST = true
as
  COPY INTO stock_market.yahoo.yfinance_daily
  FROM (
      SELECT $2, $3, $4, $5, $6, $7, $8, $9 
      FROM @stock_market.yahoo.stock_martket_s3_stage
  )
  FILE_FORMAT = csv_format;