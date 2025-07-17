# Custom libraries
import os
import logging
import sys
import traceback
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import TimestampType
from pricing_utils.utility import configure_logger
from pricing_utils.utility import pre_commit_checks
from platform_helpers import environment
from pricing_utils.utility import program_metadata
from pricing_utils.utility import get_input_output_json
from pricing_utils.utility import pre_checks_individual_tables



class pat_geo_dim:
    """
    Summary:
    The geo_dim class represents a data pipeline for processing
    and transforming geo dimensions of pat.

    Attributes:
    None

    Methods:
    get_geo_dim(config, basic_configs, catalog_name):
            Reads latest data for geo dimensions from source table.
    transform_data(config, basic_configs, catalog_name):
        Processes data by loading and transforming
        files from source to output location
    cleanup(spark):
        Cleans up the staging tables if any
    """
      
    def get_geo_dim(self, config, basic_configs, catalog_name):
        """
        Reads and processes data from source table.
        Args:
            config: config parameter
        Returns:
            DataFrame: The processed vat values data DataFrame.
        """

        geo_df = spark.sql(f"""
                SELECT * FROM {config['dim_geo_v1']['catalog_name']}.{config['dim_geo_v1']['schema']}.{config['dim_geo_v1']['table_name']}
                """)
        geo_df.createOrReplaceTempView("pat_geo_dim_stg")
        return geo_df


    def transform_data(self, config, basic_configs, catalog_name):
        """
        Transforms the data using the provided Spark session and configuration.
        Args:
            config (dict): The config parameters for data transformation.
        Raises:
            Exception: If an error occurs during the data transformation.
        Returns:
            None
        """
        try:
            geo_df = self.get_geo_dim(config, basic_configs, catalog_name)
            return geo_df
        
        except Exception as e:
            print(f"\t Error processing data : {str(e.with_traceback())}")





if __name__ == "__main__":
    try:
        print("\n\n ======= PIPELINE EXECUTION STARTED =======")
        #setting Log Level
        custom_logger = configure_logger(__name__, logging.INFO)
        # get the catalog name
        default_catalog_name = environment.get_catalog("pricing_engineering")
        custom_logger.info("Catalog Name: " + default_catalog_name)
    
        # get the pipeline context
        pipeline_context = program_metadata(dbutils=dbutils)
        custom_logger.info(pipeline_context)

        # read the config file
        # get the relative path from the parameters in case defined
        input_output_config_content = get_input_output_json(
            config_file_name="transformation_config.json",
            full_config_file_path=f"file:{os.getcwd()}/./",
            dbutils=dbutils,
            spark=spark,
        )
        output_table_name = "pat_geo_dim"
        custom_logger.info(f"Output table name is {output_table_name}")
        output_config = input_output_config_content['output']
        input_config = input_output_config_content['input']
        basic_config = input_output_config_content['basic_config']
        pipeline = pat_geo_dim()
        pat_geo_dim_df = pipeline.transform_data(input_config, basic_config, default_catalog_name)
        pat_geo_dim_df = pat_geo_dim_df.withColumn("created_on", current_timestamp())\
                                            .withColumn("updated_on", lit(None).cast(TimestampType()))

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
        pat_geo_dim_df.write.format("delta").mode('overwrite').option("overwriteSchema","true").saveAsTable(final_table_name)


    except Exception as e:
        print(f"PIPELINE FAILED: {e}")
        print(traceback.format_exc())
        raise Exception(f"Pipeline failed with error {e.with_traceback()}")
    finally:
        # spark.stop()
        print("\n\n ======= PIPELINE EXECUTION COMPLETED =======")
