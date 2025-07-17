from databricks.connect import DatabricksSession
import os

# Custom libraries
from pricing_utils.reader import Reader
from pricing_utils.writer import Writer
from pricing_utils.qc_framework import QC_Framework
from pricing_utils.email_service import Email_Service
from pricing_utils.audit import Audit
from pricing_utils.configurations import FileConfig


class PAT_ground_truth:
    """
    Summary:
        The 'gud_pat_groundtruths' pipeline generates ground truth data for product pricing in different countries.
        It includes information about the product, its category, and the source of the data.

    Attributes:
        None

    Methods:
        get_input_tables(self, config, reader)
            Fetches the input table and creates dataframe for it

        cleanup(spark):
            Cleans up the staging tables if any.

        run_qc(self, config, cluster_df):
            Runs quality control (QC) checks on output table
            with provided configuration.
    """

    def writeDataToTable(self, data_frame, audit_table, table_name, write_mode):
        """
        Writes the given DataFrame to a table.

        Parameters:
        - data_frame: The DataFrame to be written.
        - audit_table: The audit table to record the write operation.
        - table_name: The name of the table to write the data to.
        - write_mode: The write mode to use. Can be 'overwrite', 'append', or 'ignore'.

        Returns:
        None
        """
        writer = Writer(audit_table)
        writer.write_to_table(data_frame, table_name, write_mode)

    def get_input_tables(self, config, reader, audit_table):
        """
        Fetches the input table and creates dataframe for it
        Args:
            config (Config): The configuration object.
            reader (Reader): The reader object.
        Returns:
        """
        countries = config.get_basic_configs()['countries']

        groundtruth_df = reader.read_sql_query(f"""Select * from
                                               {config.get_input_table('gud_pat_groundtruth')}
                                               Where country_code in {countries}""")
        qc_status = self.run_qc(config, groundtruth_df, audit_table)

        # Joining groundtruth_df with pat_product_dim to get the family column for only 4 countries defined in config
        groundtruth_df.createOrReplaceTempView("groundtruth_df")
        pat_product_table = config.get_input_table('pat_product_dim')
        groundtruth_family_df = reader.read_sql_query(f"""
            SELECT gt.*, pd.family 
            FROM groundtruth_df AS gt
            LEFT JOIN {pat_product_table} AS pd 
            ON gt.product_number = pd.product_number
        """)
        if qc_status != 'ERROR':
                self.writeDataToTable(groundtruth_family_df, audit_table, config.get_output_table(
                        'gud_pat_groundtruth_family'), 'overwrite')
                print(f"\t\t groundtruths data with family  written to output table {config.get_output_table('gud_pat_groundtruth_family')}")
        else:
            raise Exception("QC failed. Please check the QC results table for more details.")
        groundtruth_df.unpersist()

    def run_qc(self, config, groundtruth_df, audit_table):
        """
        Runs quality control (QC) checks on the data using provided configs.
        Args:
            config (object): The config object containing parameters for QC.
            cluster_df: dataframe to run qc checks on before writing to table.
        Returns:
            None
        Raises:
            None
        """
        basic_configs = config.get_basic_configs()
        qc_config_file_path = "file:" + \
            os.path.abspath("../../") + \
            basic_configs.get('qc_config_file_path')

        qc_framework = QC_Framework(qc_config_file_path,
                                    config.get_output_table('qc_metrics_table'),
                                    config.get_output_table('qc_results_table'),
                                    schema_name='pricing_engineering')
        email_service = Email_Service(config.get_pipeline_name())

        groundtruth_df = groundtruth_df.persist()

        qc_results_df, qc_status = qc_framework.compute_metrics_and_run_qc_checks(
            tables_filter_list='gud_pat_groundtruths', data_df=groundtruth_df)

        print(f"\t\t INFO QC Status is : {qc_status}")

        if qc_status == 'ERROR':
            audit_table.add_entry(
                audit_table.failed, log_level=audit_table.error,
                message="QC Status is " + qc_status)
            email_service.create_and_send_qc_report(
                qc_results_df, qc_status, basic_configs.get('email_recipients'))

        return qc_status

    def cleanup(self, spark, config):
        """
        Cleans up the staging tables.
        """
        print("\t Cleaned up staging tables ")


if __name__ == "__main__":
    try:
        print("\n\n ======= PIPELINE EXECUTION STARTED =======")
        # Read the config file
        config = FileConfig()

        audit_table = Audit(config.get_pipeline_name())
        reader = Reader(audit_table)

        pipeline = PAT_ground_truth()

        spark = DatabricksSession.builder.getOrCreate()

        pipeline.get_input_tables(config, reader, audit_table)
        pipeline.cleanup(spark, config)

    except Exception as e:
        print(f"PIPELINE FAILED: {e}")
        print(e.with_traceback())
        audit_table.add_entry(
            audit_table.failed, log_level=audit_table.error,
            message="PIPELINE FAILED" + str(e))
    audit_table.add_entry(audit_table.success,
                          log_level=audit_table.success,
                          message="PIPELINE EXECUTION COMPLETED")
