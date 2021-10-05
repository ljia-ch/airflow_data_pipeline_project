from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Copy JSON data from S3 to Redshift staging tables.
    
    Parameters
    redshift_conn_id: Redshift Connection ID 
    aws_credentials_id:  AWS credentials ID
    table:            Destination table to store S3 JSON data
    s3_bucket:        S3 bucket where JSON data resides
    s3_key:           Path in S3 bucket where JSON data resides
    copy_json_option: column mapping option, default as 'auto'.
    region:           AWS region of source data
    
    """
    ui_color = '#358140'
    
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    REGION AS '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 copy_json_option='auto';
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region
        

    def execute(self, context):
        self.log.info("Prepare to copy data from S3 bucket files to Redshift staging tables")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Delete any data from redshift staging data")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copy data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_json_option,
            self.region
        )





