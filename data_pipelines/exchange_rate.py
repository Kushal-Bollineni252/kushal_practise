from pyspark.sql import functions as F
import os
import logging
import traceback
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import TimestampType

# Custom libraries
from pricing_utils.utility import get_input_output_json
from pricing_utils.utility import pre_check_multiple_tables
from pricing_utils.utility import pre_checks_individual_tables

from pricing_utils.utility import pre_commit_checks
from pricing_utils.utility import configure_logger
from pricing_utils.utility import program_metadata
from platform_helpers import logger
from platform_helpers import environment



class PAT_exchange_rate:
    """
    Summary:
    The PAT exchange_rate class represents a list of functions required to fetch exchange_rate data for IPG and PSG.

    Attributes:
        None
    
    Methods:
        get_final_exchange_rate_data(self, config, basic_configs, catalog_name):
           Fetching exchange_rate data from source and apply filters on it. 
        process_data(self, config, pat_exchange_rate_df):
            Writing the data to Delta table.
        transform_data(self, config, basic_configs, catalog_name):
            Processes the data by loading and transforming files
            from source to output location
        cleanup():
            Cleans up the staging tables if any.
    """

    def get_final_exchange_rate_data(self, input_config):
        catalog = input_config['pat_exchange_rate_input']['catalog_name']
        schema = input_config['pat_exchange_rate_input']['schema']
        table_name = input_config['pat_exchange_rate_input']['table_name']

        pat_exchange_rate_df = spark.sql(f"""
            SELECT  to_currency_code AS currency_code, 
                    to_currency_name AS currency_name, 
                    CAST(exchange_rate AS DOUBLE) AS exchange_rate,
                    category,
                    year_month
            FROM {catalog}.{schema}.{table_name}
            LATERAL VIEW explode(
                CASE 
                    WHEN use_case = 'pricing - IPG' THEN ARRAY('Print')
                    WHEN use_case = 'pricing - PSG' THEN ARRAY('Business', 'Consumer')
                    ELSE ARRAY()
                END
            ) AS category
            WHERE is_latest = true 
                and from_currency_code = 'USD' 
                and use_case in ('pricing - IPG', 'pricing - PSG')
        """)

        return pat_exchange_rate_df

    def transform_data(self, input_config):
        """
        Transforms the data using the provided Spark session and configuration.
        Args:
            input_config (dict): The config parameters for input data  
        Raises:
            Exception: If an error occurs during the data transformation.
        Returns:
            None
        """
        try:
            pat_exchange_rate_df = self.get_final_exchange_rate_data(input_config)
            pat_exchange_rate_df.createOrReplaceTempView('pat_exchange_rates_ww_stg')
            return pat_exchange_rate_df
        except Exception as e:
            print(f"\t Error transforming data : {str(e.with_traceback())}")
            raise Exception(f"Failed with error in transforming data {e}").with_traceback(tb)

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

        output_table_name = "pat_exchange_rates_ww"
        custom_logger.info(f"Output table name is {output_table_name}")

        output_config = input_output_config_content['output']
        input_config = input_output_config_content['input']
        basic_config = input_output_config_content['basic_config']

        pipeline = PAT_exchange_rate()
        pat_exchange_rate_df = pipeline.transform_data(input_config)

        pre_commit_checks(
            pipeline_table_name=output_table_name,
            input_output_config_content=input_output_config_content,
            default_catalog_name=default_catalog_name,
            pipeline_context=pipeline_context,
            spark=spark,
            dbutils=dbutils,
            custom_logger=custom_logger
         )
        
        final_table_name = f"{default_catalog_name}.{output_config[output_table_name]['schema']}.{output_config[output_table_name]['table_name']}"
        print(f"Writing to table {final_table_name}")
        pat_exchange_rate_df.write.format("delta").mode('overwrite').saveAsTable(final_table_name)
        custom_logger.info(f"the pipeline is successful for {final_table_name}")

    except Exception as e:
        custom_logger.error(f"PIPELINE FAILED: {e}")
        tb = e.__traceback__
        while tb.tb_next is not None:
            tb = tb.tb_next
        custom_logger.error(e.with_traceback(tb))
        custom_logger.info(f"the pipeline run failed for {output_table_name}")
        raise Exception(f"Pipeline failed with error {e}").with_traceback(tb)
    
    finally:
        print("\n\n ======= PIPELINE EXECUTION COMPLETED =======")