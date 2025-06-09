# main.py
from src.config import SnowflakeConfig
from src.snowflake_connector import SnowflakeConnector

def main():
    print("Initializing configuration...")
    # Instantiate the config object (it will load from .env)
    sf_config = SnowflakeConfig()

    # Check if config loaded correctly
    if not sf_config.username or not sf_config.password:
        print("Error: Snowflake credentials not found in .env file.")
        return

    print("Configuration loaded. Creating connector...")
    # Create a connector instance
    connector = SnowflakeConnector(config=sf_config)

    try:
        # Connect to Snowflake
        connector.connect()

        # Define and execute a sample query
        my_query = "SELECT CURRENT_VERSION();"
        df = connector.execute_query(my_query)

        print("\nQuery Result:")
        print(df)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Always make sure to close the connection
        connector.close()

if __name__ == "__main__":
    main()