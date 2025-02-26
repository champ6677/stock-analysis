import snowflake.connector

def get_snowflake_conn():
    # Snowflake connection details
    conn = snowflake.connector.connect(
        user="champ6677",
        password="",
        account="",
        warehouse="",
        database="STOCKS",
        schema="DEV"
    )
    return conn