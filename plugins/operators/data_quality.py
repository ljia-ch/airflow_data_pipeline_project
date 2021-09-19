from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL 
    
    Parameters
    redshift_conn_id: Redshift Connection ID 
    test_sql: SQL query to run on Redshift for data validation
    expected_result: Expected result to match the test result.
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_sql="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.test_sql=test_sql
        self.expected_result=expected_result

    def execute(self, context):
        self.log.info("Start data validation...")
        redshift_hook = PostgresHook(Postgres_conn_in=self.redshift_conn_id)
        
        self.log.info("Got credentials.")
        records=redshift_hook.get_records(self.test_sql)
        
        if records[0][0] != self.expected_result:
            raise ValueError(f"Data quality check failed. {records[0][0]} does not equal {self.expected_result}")
        else:
            self.log.info("Data quality check passed!!!")