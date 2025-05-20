from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    def __init__(self,
                 redshift_conn_id="",
                 test_cases="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.test_cases = test_cases
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for test in self.test_cases:
            sql = test["check_sql"]
            expected = test["expected_result"]

            self.log.info(f"Running data quality check: {sql}")
            result = redshift_hook.get_records(sql)

            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError(f"Data quality check failed. Query returned no results: {sql}")

            actual = result[0][0]
            if actual != expected:
                raise ValueError(f"Data quality check failed. Expected {expected} but got {actual} for query: {sql}")

            self.log.info(f"Data quality check passed: {sql}")
