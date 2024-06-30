import mplfinance as mpf
import matplotlib.pyplot as plt
import pandas as pd

# Load the updated DataFrame (if not already in memory)
df = pd.read_pickle('BTCUSDT_1m_Binance_Updated.pkl')
df = df.head(25000)

# Convert 'Open time' to datetime if it's not already
df['Open time'] = pd.to_datetime(df['Open time'])

# Set 'Open time' as the index (required by mplfinance)
df.set_index('Open time', inplace=True)

# Create a plot style
mpf_style = mpf.make_mpf_style(base_mpf_style='charles', rc={'font.size': 8})

# Define additional plot arguments for the high and low inflection points
ap_highs = mpf.make_addplot(df['Highs'], type='scatter', markersize=50, marker='^', color='g')
ap_lows = mpf.make_addplot(df['Lows'], type='scatter', markersize=50, marker='v', color='r')

# Plotting the chart with volume
mpf.plot(df, type='candle', style=mpf_style, figsize=(10, 6), addplot=[ap_highs, ap_lows], title='BTCUSDT OHLCV Chart with High/Low Inflections', volume=False)

# Show the plot
plt.show()
