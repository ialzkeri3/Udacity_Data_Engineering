from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    copy_sql = """
        INSERT INTO '{}'
        '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 sql_statement='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        
    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Inseritng data in Redshift")
        
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.table_name,
            self.sql_statement,
            credentials.key,
            credentials.secret
        )
        redshift.run(formatted_sql)
        
        self.log.info(f'Loading {slef.table_name} finished !')