import yfinance as yf

helsinki_stock_tickers = [
    "NOKIA.HE",  # Nokia Corporation
    "UPM.HE",    # UPM-Kymmene Corporation
    "SANDVIK.HE",  # Sandvik AB
    "KONE.HE",   # Kone Corporation
    "NESTE.HE",  # Neste Corporation
    "WARTSILA.HE",  # Wärtsilä Corporation
    "STORAENSO.HE",  # Stora Enso Oyj
    "SSAB.HE",   # SSAB AB
    "KEMIRA.HE",  # Kemira Oyj
    "FINNAIR.HE",  # Finnair Oyj
    "ELISA.HE",  # Elisa Corporation
    "FORTUM.HE",  # Fortum Corporation
    "METSO.HE",  # Metso Corporation
    "VALMET.HE",  # Valmet Corporation
    "AMER.HE",   # Amer Sports Corporation
    "YIT.HE",    # YIT Corporation
    "RAUTARUUKKI.HE",  # Rautaruukki Corporation
    "KEMIRA.HE",  # Kemira Oyj
    "OUTOKUMPU.HE",  # Outokumpu Oyj
    "TALVIVAARA.HE",  # Talvivaara Mining Company Plc
    "SSAB.HE",   # SSAB AB
    "NESTE.HE",  # Neste Corporation
    "UPM.HE"]   # UPM-Kymmene Corporation

def fetch_stock_data(tickers):
    """
    Fetches historical stock data for the given tickers from Yahoo Finance.
    
    Parameters:
        tickers (list): List of stock tickers to fetch data for.
        
    Returns:
        dict: A dictionary where keys are ticker symbols and values are DataFrames containing historical stock data.
    """
    data = {}
    for ticker in tickers:
        try:
            stock_data = yf.download(ticker, period="max", interval="1min", group_by='ticker')
            data[ticker] = stock_data
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    return data


if __name__ == "__main__":
    # Fetch data for Helsinki stock tickers
    stock_data = fetch_stock_data(helsinki_stock_tickers)
    
    # Print the fetched data
    for ticker, data in stock_data.items():
        print(f"Data for {ticker}:")
        print(data.head())  # Print the first few rows of the DataFrame