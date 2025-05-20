from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info(f"Loading data into {self.table} table in Redshift")
        self.log.info(f"Executing SQL: {self.sql_statement}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(self.sql_statement)
