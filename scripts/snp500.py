import pandas as pd
from scripts.snowflake import get_snowflake_conn

# **Step 1: Fetch S&P 500 Stock List from Wikipedia**
def get_sp500_stocks():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)  # Fetch tables from Wikipedia page
    sp500_df = tables[0]  # The first table contains the stock list
    sp500_df = sp500_df[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']]  # Keep relevant columns
    sp500_df.columns = ['ticker', 'company_name', 'sector', 'sub_sector']  # Rename columns
    return sp500_df

# **Step 2: Connect to Snowflake and Insert Data**

def insert_into_snowflake(df):
    # Snowflake connection details
    conn = get_snowflake_conn()
    cursor = conn.cursor()

    # **Create a temporary table to load data**
    cursor.execute("CREATE TEMP TABLE IF NOT EXISTS temp_sp500 (ticker STRING, company_name STRING, sector STRING, sub_sector STRING)")
    
    # Convert DataFrame to list of tuples for batch insert
    stock_data = list(df.itertuples(index=False, name=None))

    # **Batch Insert into TEMP table**
    insert_query = "INSERT INTO temp_sp500 (ticker, company_name, sector, sub_sector) VALUES (%s, %s, %s, %s)"
    cursor.executemany(insert_query, stock_data)

    # **Merge Data from Temp Table to Main Table**
    merge_query = """
        MERGE INTO stocks AS target
        USING temp_sp500 AS source
        ON target.ticker = source.ticker
        WHEN MATCHED THEN UPDATE SET 
            target.company_name = source.company_name,
            target.sector = source.sector,
            target.sub_sector = source.sub_sector
        WHEN NOT MATCHED THEN INSERT (ticker, company_name, sector, sub_sector)
        VALUES (source.ticker, source.company_name, source.sector, source.sub_sector);
    """
    
    cursor.execute(merge_query)

    # **Truncate the temporary table**
    cursor.execute("TRUNCATE TABLE temp_sp500")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted/Updated {len(stock_data)} S&P 500 stocks in Snowflake.")

# **Run the Script**
if __name__ == "__main__":
    sp500_df = get_sp500_stocks()
    insert_into_snowflake(sp500_df)