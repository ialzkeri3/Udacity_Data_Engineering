from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_keys",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION as '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table_name = '',
                 s3_bucket = '',
                 s3_key = '',
                 path = '',
                 delimiter = '',
                 headers = 1,
                 quote_char = '',
                 file_type = '',
                 aws_credentials = {},
                 region = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.aws_credentials = aws_credentials
        self.path = path
        self.quote_char = qoute_char
        self.file_type = file_type
        self.region = region
        self.headers = headers

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.key,
            credentials.secret,
            self.region
        )
        
        if self.file_type == 'json':
            file_sql = "json 'auto';"
            
            if self.table_name == 'staging_events':
                file_sql = f"json '{self.path}'"
        
        if self.file_type == 'csv':
            file_sql = """
                        DELIMITER '{}'
                        IGNOREHEADER '{}'
                        CSV QOUTE AS '{}';
            """.format(self.delimeter, self.headers,self.qoute_char)
        
        full_sql = '{} {}'.format(copy_sql, file_sql)        
        redshift.run(full_sql)

        self.log.info("Staging process ended.")
