import time
import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_config = {
    'user': 'readonly_user',
    'password': 'password123',
    'host': 'fn-prod-db-cluster.cluster-ro-cqtlpb5sm2vt.ap-northeast-1.rds.amazonaws.com',
    'database': 'api_backend',
    'port': 3306
}

# Create the connection string
connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(connection_string)

# Input times (in "YYYY-MM-DD HH:MM:SS" format)
start_time = "2025-02-10 00:00:00"
end_time = "2025-02-10 23:59:59"

# Start measuring time
script_start_time = time.time()

try:
    print("Fetching trades data within the specified time range...")
    trades_query = f"""
    SELECT id, open_time, close_time, symbol, open_price, close_price, login, volume, close_time_str, 
           commission, digits, open_time_str, profit, reason, sl, swap, ticket, tp, type_str, created_at,
           CASE 
               WHEN login LIKE '70%' OR login LIKE '3%' THEN lots
               ELSE volume / 100
           END AS FinalLot
    FROM trades
    WHERE (open_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}'))
       OR (close_time BETWEEN UNIX_TIMESTAMP('{start_time}') AND UNIX_TIMESTAMP('{end_time}'));
    """
    trades_df = pd.read_sql(trades_query, engine)
    print(f"Fetched {len(trades_df)} trades.")

    if trades_df.empty:
        print("No trades found within the specified time range.")
    else:
        # Convert open and close times to datetime format
        trades_df['open_time'] = pd.to_datetime(trades_df['open_time'], unit='s')
        trades_df['close_time'] = pd.to_datetime(trades_df['close_time'], unit='s')

        # Identify logins with trades that closed within 0 to 3 seconds after opening
        trades_df['trade_duration'] = (trades_df['close_time'] - trades_df['open_time']).dt.total_seconds()
        quick_trades_logins = trades_df[(trades_df['trade_duration'] >= 0) & (trades_df['trade_duration'] <= 30)]['login'].unique()

        if len(quick_trades_logins) == 0:
            print("No trades found with close times within 0 to 30 seconds of opening.")
        else:
            # Filter trades to only include those with quick close times or for logins with such trades
            trades_df = trades_df[trades_df['login'].isin(quick_trades_logins)]
            print(f"Filtered trades to {len(trades_df)} records based on quick-close criteria.")

            # Fetch account details for filtered trades, only including specified account types
            login_ids = tuple(int(x) for x in trades_df['login'].unique())
            accounts_query = f"""
            SELECT id AS account_id, login, type AS type_account, equity, breachedby, customer_id, starting_balance 
            FROM accounts 
            WHERE login IN {login_ids}
              AND (type LIKE '%real%' OR type LIKE '%p2%' OR type LIKE '%Stellar 1-Step Demo%');
            """
            accounts_df = pd.read_sql(accounts_query, engine)
            print(f"Fetched {len(accounts_df)} accounts with specified account types.")

            if accounts_df.empty:
                print("No matching accounts found with the specified account types.")
            else:
                # Merge trades with account information
                final_df = pd.merge(trades_df, accounts_df, on='login', suffixes=('_trade', '_account'))

                # Fetch customer details including email and name
                customer_ids = tuple(int(x) for x in final_df['customer_id'].unique())
                customers_query = f"""
                SELECT id AS customer_id, email, name, country_id FROM customers 
                WHERE id IN {customer_ids};
                """
                customers_df = pd.read_sql(customers_query, engine)
                print(f"Fetched {len(customers_df)} customers.")

                final_df = pd.merge(final_df, customers_df, on='customer_id', suffixes=('', '_customer'))

                # Fetch country names
                country_ids = tuple(int(x) for x in customers_df['country_id'].unique())
                countries_query = f"""
                SELECT id AS country_id, name AS country_name FROM countries 
                WHERE id IN {country_ids};
                """
                countries_df = pd.read_sql(countries_query, engine)
                print(f"Fetched {len(countries_df)} countries.")

                final_df = pd.merge(final_df, countries_df, on='country_id', suffixes=('', '_country'))

                # Add PnL column
                final_df['PnL'] =  final_df['equity'] - final_df['starting_balance']

                # Generate summary for each login including type account
                summary = final_df.groupby(['login', 'type_account', 'email', 'country_name']).apply(lambda x: pd.Series({
                    'total_trades_count': len(x),
                    'quick_close_trades_count': len(x[(x['trade_duration'] >= 0) & (x['trade_duration'] <= 30)]),
                    'quick_close_trade_percentage': (len(x[(x['trade_duration'] >= 0) & (x['trade_duration'] <= 30)]) / len(x)) * 100,
                    'total_profit': x['profit'].sum(),
                    'quick_close_total_profit': x.loc[(x['trade_duration'] >= 0) & (x['trade_duration'] <= 30), 'profit'].sum(),
                    'quick_close_positive_profit': x.loc[(x['trade_duration'] >= 0) & (x['trade_duration'] <= 30) & (x['profit'] > 0), 'profit'].sum(),
                    'quick_close_negative_profit': x.loc[(x['trade_duration'] >= 0) & (x['trade_duration'] <= 30) & (x['profit'] < 0), 'profit'].sum(),
                    'quick_close_profit_percentage': (
                                x.loc[(x['trade_duration'] >= 0) & (x['trade_duration'] <= 30), 'profit'].sum() / x['profit'].sum() * 100) if x['profit'].sum() != 0 else 0,
                    'PnL': x['PnL'].iloc[0]  # PnL from first row since it's the same per login
                })).reset_index()

                # Save the trades and summary to a new Excel file with multiple sheets
                output_file_name = "filtered_trades_quick_close_summary_with_customer_info.xlsx"
                with pd.ExcelWriter(output_file_name, engine='openpyxl') as writer:
                    final_df.to_excel(writer, sheet_name='Filtered Trades', index=False)
                    summary.to_excel(writer, sheet_name='Login Summary', index=False)

                print(f"Filtered data and summary have been written to {output_file_name}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Ensure the connection is closed
    engine.dispose()
    print("Database connection closed.")

# End measuring time
script_end_time = time.time()
print(f"Time taken to run the script: {script_end_time - script_start_time} seconds")
