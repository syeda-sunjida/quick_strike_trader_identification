import pandas as pd

# Load the output file from the previous code
input_file_name = "filtered_trades_quick_close_summary_with_customer_info.xlsx"
trades_df = pd.read_excel(input_file_name, sheet_name="Filtered Trades")

# List of specific logins for filtering trades under 30 seconds
target_logins = [
13406179,
13438586,
13440348,
22216627

]

# Filter trades for the specified logins and trade duration under 30 seconds
filtered_trades = trades_df[
    (trades_df['login'].isin(target_logins)) &
    (trades_df['trade_duration'] >= 0) &
    (trades_df['trade_duration'] <= 30)
]

# Keep only the specified columns, with login as the first column
columns_to_keep = [
    "login", "open_time_str", "ticket", "symbol", "type_str", "FinalLot",
    "open_price", "sl", "tp", "close_time_str", "close_price",
    "commission", "swap", "profit"
]
filtered_trades = filtered_trades[columns_to_keep]

# Rename columns for readability
filtered_trades.columns = [
    "Login", "Open Time", "Ticket", "Symbol", "Type", "Final Lot",
    "Open Price", "SL", "TP", "Close Time", "Close Price",
    "Commission", "Swap", "Profit"
]

# Save the filtered and formatted trades to a new Excel file
output_file_name = "filtered_trades_under_30s_for_specific_logins_rearranged.xlsx"
with pd.ExcelWriter(output_file_name, engine='openpyxl') as writer:
    filtered_trades.to_excel(writer, sheet_name='Trades Under 30s', index=False)

print(f"Filtered trades with necessary columns have been saved to {output_file_name}")
