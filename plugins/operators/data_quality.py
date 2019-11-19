from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import DataQualityChecks


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_tables=[],
                 dim_tables=[],
                 staging_tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_tables = fact_tables
        self.dim_tables = dim_tables
        self.staging_tables = staging_tables
        

    def execute(self, context):
        self.log.info("Data Quality Executing now...")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        log_string = "Count of records:" + "\n"
        for table in self.staging_tables + self.fact_tables + self.dim_tables:
            records = redshift_hook.get_records(DataQualityChecks.count_check.format(table))
            num_records = records[0][0]
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f"Data quality check failed. {table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
            elif num_records < 1:
                self.log.info(f"Data quality check failed. {table} contained 0 rows")
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            else:
                log_string += f"Number of records in table, {table} - {num_records} \n"
        self.log.info(log_string)
        
        staging_records = redshift_hook.get_records( DataQualityChecks.count_check.format(self.staging_tables[0]) 
                                            + " WHERE page='NextSong'" )
        fact_records = redshift_hook.get_records( DataQualityChecks.count_check.format(self.fact_tables[0]) )
        self.log.info("Staging Table and Fact Table Comparision:")
        self.log.info("Staging Table: {} - {}".format(self.staging_tables[0], staging_records[0][0]) )
        self.log.info("Fact Table: {} - {}".format(self.fact_tables[0], fact_records[0][0]) )
        if(staging_records[0][0] == fact_records[0][0]):
            self.log.info("Count between Staging Table and Fact Table matches.")
        else:
            self.log.info("Incorrect data in Fact Table.")
            raise ValueError("Incorrect data in Fact Table.")
            
        self.log.info("Data Quality Ended.")
        