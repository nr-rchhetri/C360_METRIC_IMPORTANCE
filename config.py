# config.py

from dataclasses import dataclass
from dotenv import load_dotenv
import os

# This line loads the variables from your .env file into the environment
load_dotenv()

@dataclass
class SnowflakeConfig:
    # Get credentials securely from environment variables
    # The second argument to .get() is a default value (e.g., None) if the variable isn't found
    username: str = os.environ.get("SNOWFLAKE_USER")
    password: str = os.environ.get("SNOWFLAKE_PASSWORD")
    account: str = os.environ.get("SNOWFLAKE_ACCOUNT")
    warehouse: str = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database: str = os.environ.get("SNOWFLAKE_DATABASE")
    schema: str = os.environ.get("SNOWFLAKE_SCHEMA")
    role: str = os.environ.get("SNOWFLAKE_ROLE")
    authenticator: str = "externalbrowser" # This can remain hardcoded if it's not a secret