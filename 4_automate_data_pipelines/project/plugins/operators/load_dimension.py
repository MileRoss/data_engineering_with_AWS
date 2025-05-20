from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 insert_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info(f"Loading data into {self.table} table in Redshift")
        self.log.info(f"Executing SQL: {self.sql_statement}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "truncate-insert":
            self.log.info(f"Truncating table {self.table} before insert")
            redshift.run(f'DELETE FROM "{self.table}"')

        redshift.run(self.sql_statement)
