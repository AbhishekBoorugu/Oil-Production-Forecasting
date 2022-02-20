import pandas as pd
import numpy as np
import os, random, sys
from google.cloud import storage
from datetime import datetime
from google.cloud import bigquery
import time

def web_scraper(event, context):
    tables = pd.read_html("https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=MCRFPUS2&f=M")
    df = tables[4]

    df = df.melt(id_vars=["Year"], 
        var_name="Month", 
        value_name="Value")

    df.dropna(inplace=True)

    df = df[df.Year >= 2002]
    df.reset_index(inplace=True, drop=True)

    df['Year1']=''
    df['Date']=''

    for index, row in df.iterrows():
        df.Year1[index] = str(round(row.Year))
        df.Date[index] = str('01')+df.Month[index]+df.Year1[index]
        df.Date[index] = datetime.strptime(df.Date[index], '%d%b%Y').strftime("%Y-%m-%d")
    
    df['Value'] = df['Value'].astype(int)
    df.drop(['Year','Year1','Month'], inplace=True, axis=1)
    df.rename(columns = {'Value' : 'Oil_prod'},inplace=True)
    df = df[['Date', 'Oil_prod' ]]

    df.to_csv('/tmp/Oil_Production.csv', index=False)
    filename= '/tmp/Oil_Production.csv'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('s2a3_projectbucket')
    blob = bucket.blob('Pipeline/Production/Oil_Production.csv')
    blob.upload_from_filename(filename)

    print('File {} uploaded to {}.'.format(filename, bucket))

    # Wait until file becomes available
    time.sleep(15)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Truncate the existing table
    try:
        sql_query = '''
        TRUNCATE TABLE forecasting-339006.Oil_Production.monthly_statistics
        '''
        client.query(sql_query)
    except:
        pass

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "forecasting-339006.Oil_Production.monthly_statistics"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Month", "DATE"),
            bigquery.SchemaField("Oil_Production", "INTEGER"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://s2a3_projectbucket/Pipeline/Production/*.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


    modelling_query = '''
    # GENERATE ARIMA MODEL ON MONTHLY OIL PRODUCTION DATA
    CREATE OR REPLACE MODEL Oil_Production.arima_model
    OPTIONS(
    MODEL_TYPE='ARIMA',
    TIME_SERIES_TIMESTAMP_COL='Month', 
    TIME_SERIES_DATA_COL='Oil_Production',
    HOLIDAY_REGION='US'
    ) AS

    SELECT 
        Month,
        Oil_Production
    FROM
    `forecasting-339006.Oil_Production.monthly_statistics`;
    '''

    query_job = client.query(modelling_query, location='asia-south1')
    rows = query_job.result()  # Waits for query to finish making it Synchronous
    job = client.get_job(query_job.job_id, location=query_job.location)
    print("Job with {id} has completed execution!!".format(id=job.state))

    HORIZONS = ['"12"', '"24"', '"36"', '"48"', '"60"']
    CONFIDENCE_LEVELS = ['"0.80"', '"0.85"', '"0.90"', '"0.95"']

    prediction_queries = list()

    delete_sameDay_Forecasts = '''DELETE FROM `forecasting-339006.Oil_Production.production_forecast` WHERE refresh_date = CURRENT_DATE();'''
    predDel_job = client.query(delete_sameDay_Forecasts, location='asia-south1')
    rows = predDel_job.result()
    print("Any exisitng same day forecasts have been deleted!")

    for horizon in HORIZONS:
        for confidence in CONFIDENCE_LEVELS:
            prediction_queries.append(
                '''
                # PREDICT FUTURE VALUES WITH DIFFERENT CONFIDENCE_LEVELS
                DECLARE HORIZON STRING DEFAULT {HR}; #number of values to forecast
                DECLARE CONFIDENCE_LEVEL STRING DEFAULT {CI};

                EXECUTE IMMEDIATE format("""
                    INSERT INTO `forecasting-339006.Oil_Production.production_forecast` 
                    SELECT
                    *,
                    CONFIDENCE_LEVEL as confidence,
                    {HR} as forecast_horizon,
                    CURRENT_DATE() AS refresh_date
                    FROM 
                    ML.FORECAST(MODEL Oil_Production.arima_model, 
                                STRUCT(%s AS horizon, 
                                        %s AS confidence_level)
                                )
                    ORDER BY forecast_timestamp
                    """, HORIZON, CONFIDENCE_LEVEL);
                '''.format(HR= horizon , CI= confidence)
            )

    for query in prediction_queries:
        prediction_job = client.query(query, location='asia-south1')
        rows = prediction_job.result()
        job = client.get_job(query_job.job_id, location=query_job.location)
        print("Job with {id} has completed execution!!".format(id=query_job.job_id))

    final_table_query  = '''-- STACKING ACTUAL AND FORECASTED VALUES TO CREATE A FINAL TABLE
    CREATE OR REPLACE TABLE `forecasting-339006.Oil_Production.actual_forecasted` AS (
    SELECT Month, 
        Oil_Production, 
        NULL as Forecasted_Production,
        NULL as Forecasted_LowerInterval,
        NULL as Forecasted_UpperInterval,
        NULL as Confidence_Level,
        NULL as forecast_interval,
        NULL as standard_error,
        NULL as Margin_Of_Error,
        NULL as refresh_date
    FROM `forecasting-339006.Oil_Production.monthly_statistics`

    UNION ALL

    SELECT DATE(forecast_timestamp) as Month, 
        NULL as Oil_Production,
        forecast_value as Forecasted_Production,
        confidence_interval_lower_bound	as Forecasted_LowerInterval,
        confidence_interval_upper_bound	as Forecasted_UpperInterval,
        confidence as Confidence_Level,
        forecast_interval,
        ROUND(standard_error, 3),
        ROUND(CASE 
            WHEN confidence = 0.8 THEN standard_error * 1.28
            WHEN confidence = 0.85 THEN standard_error * 1.44
            WHEN confidence = 0.90 THEN standard_error * 1.64
            WHEN confidence = 0.95 THEN standard_error * 1.96
        END, 3) as Margin_Of_Error,
        refresh_date
    FROM `forecasting-339006.Oil_Production.production_forecast`
    WHERE refresh_date = (SELECT MAX(refresh_date) FROM `forecasting-339006.Oil_Production.production_forecast`)
    );'''

    finalTable_job = client.query(final_table_query, location='asia-south1')
    rows = finalTable_job.result()
    print("Actual_Forecast Table has been updated!")

    return 'Success!'