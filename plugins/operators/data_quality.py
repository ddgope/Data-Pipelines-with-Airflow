import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 check_sql="",
                 expected_value="",
                 describe="",
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.check_sql = check_sql
        self.expected_value = expected_value
        self.describe = describe

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("\n".join([
            'DataQuality check',
            self.describe,
            'expected value is {}'.format(self.expected_value),
            self.check_sql
        ]))

        records = redshift_hook.get_records(self.check_sql)
        if (records[0][0] < 1): # len(records) < 1 or len(records[0][0])
            raise ValueError(f"Data quality check failed. returned no results")
        if int(self.expected_value) != records[0][0]:
            raise ValueError(f"Data quality check failed. \n expected: {self.expected_value} \n acutal: {records[0][0]}")
        self.log.info(f"Data quality on \n {self.describe} \n check passed with \n expected: {self.expected_value} \n acutal: {records[0][0]}")       
