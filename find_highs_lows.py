import pandas as pd
import numpy as np
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the data
file_path = 'BTCUSDT_1m_Binance.pkl'
df = pd.read_pickle(file_path)

# Simplify the DataFrame for example
df = df.head(10000)

# Parameters
look_ahead = 7500  # X candles
price_swing_percent = 1  # Y percent (1% swing)

# Function to find sequential highs and lows with price swing percent implementation
def find_highs_lows(df, look_ahead, price_swing_percent):
    highs = pd.Series(index=df.index, dtype='float64')
    lows = pd.Series(index=df.index, dtype='float64')
    current_mode = 'low'

    current_index = 0
    while current_index < len(df) - look_ahead:
        if current_mode == 'low':
            current_high = df['High'][current_index]
            next_high_index = current_index
            for i in range(current_index + 1, current_index + look_ahead + 1):
                if df['High'][i] > current_high:
                    current_high = df['High'][i]
                    next_high_index = i
                    if df['Low'][i] <= current_high * (1 - price_swing_percent / 100):
                        print("triggered")
                        break  # Confirm the high because price swung down by the threshold percent
            if next_high_index != current_index:
                highs[next_high_index] = current_high
                current_mode = 'high'
                current_index = next_high_index + 1  # Move to the index of the candle after the new high
            else:
                current_index += look_ahead + 1  # Move beyond the look-ahead window

        elif current_mode == 'high':
            current_low = df['Low'][current_index]
            next_low_index = current_index
            for i in range(current_index + 1, current_index + look_ahead + 1):
                if df['Low'][i] < current_low:
                    current_low = df['Low'][i]
                    next_low_index = i
                    if df['High'][i] >= current_low * (1 + price_swing_percent / 100):
                        print("triggered")
                        break  # Confirm the low because price swung up by the threshold percent
            if next_low_index != current_index:
                lows[next_low_index] = current_low
                current_mode = 'low'
                current_index = next_low_index + 1  # Move to the index of the candle after the new low
            else:
                current_index += look_ahead + 1  # Move beyond the look-ahead window

    return highs, lows

# Apply the function
highs, lows = find_highs_lows(df, look_ahead, price_swing_percent)

# Add the highs and lows to the DataFrame
df['Highs'] = highs
df['Lows'] = lows

# Fill non-inflection points with NaN for plotting
df['Highs'] = df['Highs'].where(pd.notna(df['Highs']), np.nan)
df['Lows'] = df['Lows'].where(pd.notna(df['Lows']), np.nan)

# Save the DataFrame back to a pickle file with the new columns
df.to_pickle('BTCUSDT_1m_Binance_Updated.pkl')

# Print some data to verify
print(df[['Open time', 'High', 'Low', 'Highs', 'Lows']].head(100))
