from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """     
        COPY {} 
        FROM '{}' 
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}' 
        JSON '{}' ;
    """

    @apply_defaults
    def __init__(self,
                 creation_query="",
                 table_name="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_path="",
                 json_data_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.creation_query = creation_query
        self.table = table_name
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.json_data_format=json_data_format

    def execute(self, context):
        self.log.info("Loading Staging Redshift table - {}".format(self.table))
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting existing Redshift table")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        
        self.log.info("Creating table")
        redshift.run(self.creation_query.format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            "us-west-2",
            self.json_data_format
        )
        redshift.run(formatted_sql)
        
        self.log.info('{} loading completed.'.format(self.table))

