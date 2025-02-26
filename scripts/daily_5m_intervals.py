from scripts.snowflake import get_snowflake_conn
import yfinance as yf
from datetime import datetime, timedelta


def fetch_tickers():
    # Snowflake connection details
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    # **Fetch tickers from snowflake table**
    cursor.execute("SELECT DISTINCT TICKER FROM STOCKS");
    stocks = cursor.fetchall();
    formatted_rows = [", ".join(map(str, row)) for row in stocks]
    conn.commit()
    cursor.close()
    conn.close()
    return formatted_rows

def insert_5m_interval(tickers, interval='5m'):
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    # **Step 3: Fetch Data & Store in a Batch List**
    batch_data = []
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()
    try:
        data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval=interval, prepost=False)
        for ticker in tickers:
            # Extract columns for a given ticker from the MultiIndex DataFrame
            df_ticker = data.xs(ticker, level=1, axis=1)
            for index, row in df_ticker.iterrows():
                timestamp = index.strftime('%Y-%m-%d %H:%M:%S')
                open_price = float(row["Open"])
                high_price = float(row["High"])
                low_price = float(row["Low"])
                close_price = float(row["Close"])
                volume = float(row["Volume"])
                batch_data.append((timestamp, ticker, open_price, high_price, low_price, close_price, volume))
    except Exception as e:
        print(f"Error fetching data for {tickers}: {e}")


    # **Step 4: Execute Batch Insert Using `executemany()`**
    if batch_data:
        insert_query = """
            INSERT INTO stock_data_5m (timestamp, ticker, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, batch_data)

    # **Step 5: Commit and Close Connection**
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    tickers = fetch_tickers()
    insert_5m_interval(tickers)

