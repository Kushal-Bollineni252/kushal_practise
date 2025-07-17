import sys
import os
import logging
import traceback
# Custom libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import TimestampType
from pricing_utils.utility import configure_logger
from pricing_utils.utility import pre_commit_checks
from platform_helpers import environment
from pricing_utils.utility import program_metadata
from pricing_utils.utility import get_input_output_json
from pricing_utils.utility import pre_checks_individual_tables



class PAT_DS_Cluster:
    """
    Summary:
    The PAT DS cluster class represents a data pipeline for processing
    and transforming inventory data.
    Latest cluster calculation is done based on latest product data
    for the specified countries.

    Attributes:
        None

    Methods:
        get_cluster_input_tables(self, config, reader)
            Fetches the input tables and creates views for each table
            based on filter conditions
        calculate_latest_cluster(spark, config, reader):
            Processes the data by loading new files, parsing them and
            calculating latest cluster for the products
        transform_data(self, spark, config, reader, writer):
            Processes the data by loading and transforming files
            from source to output location
    """

   

    def get_cluster_input_tables(self, input_config):
        """
        Fetches the input tables and creates views for each table
        based on filter conditions
        Args:
            config (Config): The configuration object.
        Returns:
        """
        input_tables = [input_config[table]['table_name'] for table in input_config]
        for table in input_tables:
            table_columns = spark.sql(f"Select * from {input_config[table]['catalog_name']}.{input_config[table]['schema']}.{table}").columns

            # Column names for different tables with same data
            if 'print' in table:
                brand_column = 'ctx_vendor_first'
            else:
                brand_column = 'Brand'

            # Country name based on table name
            if '_us_' in table:
                country_code = 'US'
            elif '_uk_' in table:
                country_code = 'GB'
            elif '_fr_' in table or '_france_' in table:
                country_code = 'FR'
            else:
                country_code = 'country_code'

            # Standard Query for all cluster input tables
            query = f"""SELECT SKU,
                               CLUSTER,
                               CASE WHEN PATH like '2___q_,%' THEN 
                               SUBSTRING(PATH FROM POSITION(',' IN PATH) + 1)
                               WHEN PATH LIKE '2___q_' THEN NULL
                               ELSE PATH
                               END AS PATH,
                               cluster_size,
                               REPLACE(fiscal_quarter, 'q', 'Q')
                               AS fiscal_quarter,
                               '{country_code}' AS country_code
                               FROM {input_config[table]['catalog_name']}.{input_config[table]['schema']}.{table}
                               WHERE {brand_column} = 'HP' or {brand_column} = 'hp'"""

            # Filtering conditions for various tables based on the columns
            if 'country' in table_columns and 'monitor_emea' in table:
                query = query.replace(f"'{country_code}' AS country_code",
                                      "CASE WHEN country = 'United Kingdom' THEN 'GB' \
                                      WHEN country = 'France' THEN 'FR' \
                                      WHEN country = 'Germany' THEN 'DE' \
                                      ELSE country END AS country_code")

            if 'country' in table_columns and '_na_' in table:
                query = query.replace(f"'{country_code}' AS country_code",
                                      "CASE WHEN country = 'united states' \
                                      THEN 'US' END as country_code")

            if 'country' in table_columns and 'bps_emea' in table:
                query = query.replace(f"'{country_code}' AS country_code",
                                      "CASE WHEN country = 'united kingdom' THEN 'GB' \
                                      WHEN country = 'france' THEN 'FR' \
                                      WHEN country = 'germany' THEN 'DE' \
                                        ELSE country END AS country_code")

            if 'cluster_size' not in table_columns and 'asp_mean' in table_columns:
                query = query.replace("cluster_size", "asp_mean as cluster_size")

            if 'fiscal_quarter' not in table_columns:
                query = query.replace("REPLACE(fiscal_quarter, 'q', 'Q')", "year_quarter")

            if f'{brand_column}' not in table_columns:
                query = query.replace(f"WHERE {brand_column} = 'HP' or {brand_column} = 'hp'", "")

            if 'monitor_us' in table:
                query = query.replace("cluster_size",
                                      "cast(cluster_size as decimal) \
                                        as cluster_size")

            # stores the current table in dataframe
            current_table = spark.sql(query)
            table_name = str(table)
            # Creates a temporary view for the current_table
            current_table.createOrReplaceTempView(table_name)

    def calculate_latest_cluster(self, input_config , basic_config):
        """
        Calculates the latest cluster path based on the provided country list.
        Args:
            config (Config): The configuration object.
        Returns:
            DataFrame: A DataFrame containing the latest cluster path for
            the specified products and countries.
        """
        ams_countries = basic_config['ams_countries']
        emea_countries = basic_config['emea_countries']
        # Fetching input tables and creating views
        self.get_cluster_input_tables(input_config)
        query1 = f"""SELECT
                    UPPER(combined.SKU) AS SKU,
                    combined.CLUSTER,
                    combined.PATH,
                    combined.cluster_size,
                    REPLACE(combined.fiscal_quarter, 'Q', 'Q0') AS fiscal_quarter,
                    combined.country_code as country_code
                    FROM (
                        SELECT * FROM bps_emea_clustering
                        Where country_code in {emea_countries}
                        UNION
                        SELECT * FROM monitor_emea_clustering
                        Where country_code in {emea_countries}
                        UNION
                        SELECT * FROM monitor_us_clustering
                        Where country_code in {ams_countries}
                        UNION
                        SELECT * FROM bps_na_clustering
                        Where country_code in {ams_countries}
                        UNION
                        SELECT * FROM print_france_clustering_time_v2
                        Where country_code = 'FR'
                        UNION
                        SELECT * FROM print_uk_clustering_time_v8
                        where country_code = 'GB'
                    ) AS combined
                    WHERE LENGTH(UPPER(combined.SKU)) = 7
                    OR LENGTH(UPPER(combined.SKU)) = 6;
                """

        # Union dataframe of all cluster input tables
        pat_union_cluster_df = spark.sql(query1)
        pat_union_cluster_df.createOrReplaceTempView('pat_union_cluster_df')

        # Fetching the latest clusters
        query2 = """SELECT
                    old.SKU as product_number,
                    latest.CLUSTER as cluster_id,
                    old.PATH as cluster_name,
                    latest.cluster_size,
                    old.country_code,
                    latest.fiscal_quarter as latest_fiscal_quarter,
                    latest.PATH AS latest_cluster_name
                    FROM pat_union_cluster_df AS old
                    LEFT JOIN (SELECT
                                SKU,
                                CLUSTER,
                                PATH,
                                cluster_size,
                                fiscal_quarter,
                                country_code
                                FROM (
                                        SELECT
                                        SKU,
                                        CLUSTER,
                                        PATH,
                                        cluster_size,
                                        fiscal_quarter,
                                        country_code,
                                        FIRST(fiscal_quarter) OVER (PARTITION BY SKU, country_code 
                                        ORDER BY CASE WHEN PATH IS NOT NULL THEN fiscal_quarter ELSE NULL END DESC
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                        ) AS fiscal_quarter_last
                                        FROM pat_union_cluster_df
                                    ) AS ranked_data
                                WHERE fiscal_quarter = fiscal_quarter_last) AS latest
                                ON old.SKU = latest.SKU
                                AND old.country_code = latest.country_code"""
                                
        pat_inventory_cluster_df = spark.sql(query2)\
            .dropDuplicates(['product_number', 'country_code'])
        return pat_inventory_cluster_df

   

    def transform_data(self, input_config , basic_config):
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
            cluster_df = self.calculate_latest_cluster(input_config, basic_config)
            cluster_df.createOrReplaceTempView('pat_inventory_cluster_stg')
            return cluster_df
        except Exception as e:
            print(f"\t Error processing data : {str(e.with_traceback())}")


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
        # Read the config file
        # Get the relative path from the parameters in case defined
        input_output_config_content = get_input_output_json(
            config_file_name="transformation_config.json",
            full_config_file_path=f"file:{os.getcwd()}/./",
            dbutils=dbutils,
            spark=spark,
        )
        output_table_name = "pat_inventory_cluster"
        custom_logger.info(f"Output table name is {output_table_name}")
        output_config = input_output_config_content['output']
        input_config = input_output_config_content['input']
        basic_config = input_output_config_content['basic_config'] 
        pipeline = PAT_DS_Cluster()
        pat_inventory_cluster_df = pipeline.transform_data(input_config , basic_config)
        pat_inventory_cluster_df = pat_inventory_cluster_df.withColumn("created_on", current_timestamp())\
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
        pat_inventory_cluster_df.write.format("delta").mode('overwrite').option("overwriteSchema","true").saveAsTable(final_table_name)
    except Exception as e:
        print(f"PIPELINE FAILED: {e}")
        print(traceback.format_exc())
        raise Exception(f"Pipeline failed with error {e.with_traceback()}")
    finally:
        print("\n\n ======= PIPELINE EXECUTION COMPLETED =======")

