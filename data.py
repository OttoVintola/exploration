from stock_list import stocks 
import pandas as pd
import yfinance as yf


def fetch_minute_data(ticker_list):
    """
    Fetches historical market data (Open, High, Low, Close, Volume) at 1-minute intervals
    for each ticker in ticker_list.
    
    Parameters:
        ticker_list (list): List of ticker symbols (with .HE suffix)
    
    Returns:
        data_dict (dict): Dictionary with ticker symbols as keys and corresponding
                          minute-level dataframe as values.
    """
    data_dict = {}
    for ticker in ticker_list:
        try:
            print(f"Fetching 1-minute data for {ticker}...")
            # period='max' is used as requested (though Yahoo Finance limits
            # 1-minute resolution to approximately the last 30 days)
            df = yf.download(ticker, period="max", interval="1m", progress=False)
            if df.empty:
                print(f"No data returned for {ticker}")
            else:
                data_dict[ticker] = df
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    return data_dict


def fetch_financial_measures(ticker_list):
    """
    Fetches financial measures (e.g., P/E, EPS, Market Cap, etc.) for each ticker.
    
    Parameters:
        ticker_list (list): List of ticker symbols (with .HE suffix)
    
    Returns:
        info_dict (dict): Dictionary with ticker symbols as keys and the financial
                          info (a dictionary of measures) as values.
    """
    info_dict = {}
    for ticker in ticker_list:
        try:
            print(f"Fetching financial measures for {ticker}...")
            ticker_obj = yf.Ticker(ticker)
            # The 'info' attribute usually contains a wide variety of financial measures.
            info = ticker_obj.info
            if not info:
                print(f"No financial info returned for {ticker}.")
            info_dict[ticker] = info
        except Exception as e:
            print(f"Error fetching info for {ticker}: {e}")
    return info_dict


def save_financial_indicators_to_parquet(info_dict, folder_path='data'):
    """
    Save financial indicators to a parquet file with the name "{stock}_indicators.parquet" in the data folder

    Parameters:
        info_dict (dict): Dictionary with ticker symbols as keys and the financial
                          info (a dictionary of measures) as values.
        folder_path (str): Path to the folder where the parquet files will be saved.
    """
    for ticker, info in info_dict.items():
        try:
            # Convert the info dictionary to a DataFrame
            df = pd.DataFrame([info])
            # Save to parquet file
            file_name = f"{folder_path}/{ticker}_indicators.parquet"
            df.to_parquet(file_name, index=False)
            print(f"Saved financial indicators for {ticker} to {file_name}")
        except Exception as e:
            print(f"Error saving financial indicators for {ticker}: {e}")


def save_minute_data_to_parquet(data_dict, folder_path='data'):
    """
    Save the minute data to a parquet file with the name "{stock}_time_series.parquet" in the data folder

    Parameters:
        data_dict (dict): Dictionary with ticker symbols as keys and corresponding
                          minute-level dataframe as values.
        folder_path (str): Path to the folder where the parquet files will be saved.
    """
    for ticker, df in data_dict.items():
        try:
            # Save to parquet file
            file_name = f"{folder_path}/{ticker}_time_series.parquet"
            df.to_parquet(file_name)
            print(f"Saved minute data for {ticker} to {file_name}")
        except Exception as e:
            print(f"Error saving minute data for {ticker}: {e}")