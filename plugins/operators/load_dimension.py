from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_name="",
                 append_mode=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_name=sql_name
        self.append_mode=append_mode
        self.primary_key=primary_key

    def execute(self, context):
        self.log.info('Start load dimension table')
        self.log.info("Get credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_insert:
            
            # append insert sql need delete the records matched with table to be inserted to avoid dupes
            table_insert_sql = """
            create temp table stage_{self.table} (like {self.table});
            
            insert into stage_{self.table}
            {self.sql_name};
            
            delete from {self.table}
            using stage_{self.table}
            where {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};
            
            insert into {self.table}
            select * from stage_{self.table};
            
            """
        else:
            # run truncate table and then prepare the insert table sql statements
            self.log.info(f"Delete data from dimentation {self.table} in Redshift")
            redshift_hook.run(f"TRUNCATE TABLE {SELF.TABLE};")
            
            table_insert_sql = """
                insert into {self.table}
                {self.select_sql}
            """
        self.log.info("Insert {self.table} data into dimension table in Redshift")
        redshift_hook.run(table_insert_sql)
            
