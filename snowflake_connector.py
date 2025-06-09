
import sys
import os

from config import SnowflakeConfig

import snowflake.connector
import pandas as pd


class SnowflakeConnector:
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self):
        # This part remains the same
        self.conn = snowflake.connector.connect(
            user=self.config.username,
            account=self.config.account,
            role=self.config.role,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
            authenticator = self.config.authenticator
        )
        self.cursor = self.conn.cursor()
        print("Successfully connected to Snowflake.")

    def execute_query(self, query: str):
        print(f"Executing query: {query[:50]}...")
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        column_names = [col[0] for col in self.cursor.description]
        print("Query executed successfully.")
        return pd.DataFrame(result, columns=column_names)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Snowflake connection closed.")