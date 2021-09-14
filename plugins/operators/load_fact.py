from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import sql_queries

class LoadFactOperator(BaseOperator):
    """
    Load data from staging tables to Fact tables
    
    Parameters
    redshift_conn_id:    AWS redshift connection id 
    target_table:        Fact table name
    query_name:          Query name to use in sql_queries
    insert_mode:         Defined how to insert data in target table. 
                         (
                         append(default): append on top of existing data
                         truncate_insert: delete current and insert new data
                         )
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 target_table = "",
                 query_name = "",
                 insert_mode = "append",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.target_columns = target_columns
        self.query_name = query_name
        self.insert_mode = insert_mode

    def execute(self, context):
        
        self.log.info('LoadFactOperator not implemented start')
        
        # Prepare SQL query to load data
        self.log.info("Prepare load data from staging table to {} fact table".format(self.target_table))
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into fact table")
        table_insert_sql = """
            INSERT INTO {self.target_table}
            {self.query_name}
        """
        redshift_hook.run(table_insert_sql)
        
