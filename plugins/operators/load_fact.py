from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import sql_queries

class LoadFactOperator(BaseOperator):
    """
    Load data from staging tables to Fact tables
    
    [Parameters List]
    redshift_conn_id:    AWS redshift connection id 
    target_table:        Fact table name
    target_columns:      Fact table column names 
    query:               Query names to use in sql_queries
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
