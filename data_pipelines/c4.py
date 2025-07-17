# Custom libraries
from pyspark.sql import functions as F
import os
import sys
from pyspark.sql.functions import current_timestamp, lit , max , col 
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

import logging
import traceback
from pricing_utils.utility import configure_logger
from pricing_utils.utility import pre_commit_checks
from platform_helpers import environment
from pricing_utils.utility import program_metadata
from pricing_utils.utility import get_input_output_json 
from pricing_utils.utility import pre_checks_individual_tables




class pat_c4_cost:
    """
    Summary:
    The c4_cost class represents a data pipeline for processing cost data.
    Attributes:
        None
    Methods:
        get_c4_cost(input_config, output_config):
            Fetches the latest product costs.
        transform_data(input_config, output_config):
            Processes the data by loading files from source to output location
    """

    # Defining Methods

    def get_c4_cost(self, input_config , output_config , catalog_name):
        """
        Fetches the latest product costs for the provided country list.

        Args:
            input_config (Config): The configuration object for input_tables
            output_config (Config): The configuration object for output_tables
        Returns:
            DataFrame: A DataFrame containing c4_cost for specified countries.
        """
        # Read the source table 
        t1_cost_df = spark.sql(
                        f"SELECT * FROM {input_config['c4_cost_ww']['catalog_name']}.{input_config['c4_cost_ww']['schema']}.{input_config['c4_cost_ww']['table_name']}"
                 ) 
        try:
            # Read the pat output table to get previous load date
            pat_c4_cost_df = spark.sql(
                                f"SELECT * FROM {catalog_name}.{output_config['pat_c4_cost_ww']['schema']}.{output_config['pat_c4_cost_ww']['table_name']}"
                            )
            if 'load_ts' in pat_c4_cost_df.columns:
                pat_c4_cost_df = pat_c4_cost_df.drop("load_ts")
            if 'created_on' in pat_c4_cost_df.columns:
                pat_c4_cost_df = pat_c4_cost_df.drop("created_on")
            if 'updated_on' in pat_c4_cost_df.columns:
                pat_c4_cost_df = pat_c4_cost_df.drop("updated_on")
            load_upto = str(
                    pat_c4_cost_df.select(max("load_upto")).collect()[0][0]
                )
        except Exception as e:
            # In case job is running first time, set default load_upto if no previous data is available
            print("Exception occured while fetching previous data from pat_c4_cost_ww")
            print(traceback.format_exc())
            load_upto = "2024-11-12 07:03:57.873" ## Min date in C4 cost table
            pat_c4_cost_df = None
            print("No previous data available, job running in full mode")

        print( f"previous data read upto {load_upto}")
        t1_cost_df.createOrReplaceTempView("c4_cost_table")

        # Selecting required columns with latest timestamp format updated to yyyy-mm-dd
        sql_query = f"""SELECT DISTINCT
                    product_id,
                    start_date,
                    prod_line,
                    country_code,
                    cost_status,
                    output_currency,
                    status,
                    matrl_cost,
                    vtrdx_cost,
                    malad_cost,
                    vwrty_cost,
                    vrlty_cost,
                    ovblc_cost,
                    ofxdc_cost,
                    total_cost,
                    mcc,
                    spn,
                    CASE 
                        WHEN latest_timestamp LIKE '%-___-%' THEN 
                        date_format(to_date(latest_timestamp, 'dd-MMM-yy'), 'yyyy-MM-dd')
                        ELSE latest_timestamp
                    END AS latest_timestamp,
                    price_descriptor,
                    mot,
                    geo_code,
                    geo_level,
                    country_name,
                    region,
                    cast(inserted_on as timestamp) as load_upto
                FROM
                    c4_cost_table
                WHERE
                    inserted_on > '{load_upto}' and cost_status NOT LIKE 'NOT%' and matrl_cost > '0.0' and to_date(start_date) < current_date()
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY product_id, country_name 
                    ORDER BY to_date(start_date, 'dd-MMM-yy') DESC,
                    (CASE 
                        WHEN latest_timestamp LIKE '%-___-%' THEN 
                        date_format(to_date(latest_timestamp, 'dd-MMM-yy'), 'yyyy-MM-dd')
                        ELSE latest_timestamp
                    END) DESC
                ) = 1;"""

        t1_cost_df = spark.sql(sql_query)
        if (t1_cost_df.count() == 0):
             print("No new data found in c4_cost_table source table, exiting the job")
             return None
        else :
            t1_cost_df.persist()    
            if pat_c4_cost_df is not None:
                # Union of latest data with previous data
             t1_cost_df = t1_cost_df.union(pat_c4_cost_df)
             t1_cost_df.createOrReplaceTempView("final_c4_cost_table")

            # select the latest records based on latest_timestamp
            sql_query_final = """
                        SELECT DISTINCT
                        product_id,
                        start_date,
                        prod_line,
                        country_code,
                        cost_status,
                        output_currency,
                        status,
                        matrl_cost,
                        vtrdx_cost,
                        malad_cost,
                        vwrty_cost,
                        vrlty_cost,
                        ovblc_cost,
                        ofxdc_cost,
                        CAST(
                            COALESCE(CAST(matrl_cost AS DOUBLE), 0.0) * (1 + COALESCE(CAST(malad_cost AS DOUBLE), 0.0) / 100.0) +
                            COALESCE(CAST(vwrty_cost AS DOUBLE), 0.0) +
                            COALESCE(CAST(vtrdx_cost AS DOUBLE), 0.0) +
                            COALESCE(CAST(vrlty_cost AS DOUBLE), 0.0) +
                            COALESCE(CAST(ovblc_cost AS DOUBLE), 0.0) +
                            COALESCE(CAST(ofxdc_cost AS DOUBLE), 0.0)
                            AS DOUBLE) AS total_cost,
                        mcc,
                        spn,
                        latest_timestamp,
                        price_descriptor,
                        mot,
                        geo_code,
                        geo_level,
                        country_name,
                        region,
                        load_upto
                        FROM (
                                SELECT
                                *,
                                ROW_NUMBER() OVER (PARTITION BY product_id,
                                country_name ORDER BY start_date DESC,
                                latest_timestamp DESC) AS rank
                            FROM
                                final_c4_cost_table
                            WHERE
                            cost_status NOT LIKE 'NOT%'
                            AND country_code is not null
                            AND product_id is not null
                            AND to_date(start_date) < current_date()
                            )
                        WHERE
                        rank = 1;
                """
            final_df = spark.sql(sql_query_final)

            # Update the load_upto value with latest read fron source table
            window_spec = Window.partitionBy()
            final_df = final_df.withColumn("load_upto", max(col('load_upto')).over(window_spec))
        return final_df

    def transform_data(self, input_config , output_config , catalog_name):
        """
        Transforms the data using the provided Spark session and configuration.

        Args:
            
            input_config (dict): The config parameters for data transformation.
            output_config (dict): The config parameters for output.
        
        Raises:
            Exception: If an error occurs during the data transformation.

        Returns:
            None
        """
        c4_cost_df = self.get_c4_cost(input_config , output_config ,catalog_name)
        if c4_cost_df is None:
            return None
        else:
            c4_cost_df.createOrReplaceTempView("pat_c4_cost_ww_stg")
            return c4_cost_df


if __name__ == "__main__":
    try:
        print("\n\n ======= PIPELINE EXECUTION STARTED =======")
        # Setting Log Level
        custom_logger = configure_logger(__name__, logging.INFO)
        # Get the catalog name
        default_catalog_name = environment.get_catalog("pricing_engineering")
        custom_logger.info("Catalog Name: " + default_catalog_name)

        # Get the pipeline context
        pipeline_context = program_metadata(dbutils=dbutils)
        custom_logger.info(pipeline_context)
        input_output_config_content = get_input_output_json(
            config_file_name="transformation_config.json",
            full_config_file_path=f"file:{os.getcwd()}/./",
            dbutils=dbutils,
            spark=spark,
        )
        output_table_name = "pat_c4_cost_ww"
        custom_logger.info(f"Output table name is {output_table_name}")
        output_config = input_output_config_content['output']
        input_config = input_output_config_content['input']
        basic_config = input_output_config_content['basic_config']

        pipeline = pat_c4_cost()
        pat_c4_cost_df = pipeline.transform_data(input_config ,output_config , default_catalog_name)
        if pat_c4_cost_df is None:
            print("No new data to process")
        else:
            pre_commit_checks(
            pipeline_table_name=output_table_name,
            input_output_config_content=input_output_config_content,
            default_catalog_name=default_catalog_name,
            pipeline_context=pipeline_context,
            spark=spark,
            dbutils=dbutils,
            custom_logger=custom_logger
            )
            pat_c4_cost_df = pat_c4_cost_df.withColumn("created_on", current_timestamp())\
                                            .withColumn("updated_on", lit(None).cast(TimestampType()))
            final_table_name = f"{default_catalog_name}.{output_config[output_table_name]['schema']}.{output_config[output_table_name]['table_name']}"
            print(f"Writing to table {final_table_name}")
            pat_c4_cost_df.write.format("delta").mode('overwrite').option("overwriteSchema","true").saveAsTable(final_table_name)     
    except Exception as e:
        print(f"PIPELINE FAILED: {e}")
        print(traceback.format_exc())
        raise Exception(f"Pipeline failed with error {e.with_traceback()}")
    finally:
        print("\n\n ======= PIPELINE EXECUTION COMPLETED =======")
