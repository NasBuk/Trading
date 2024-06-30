import pandas as pd
import mplfinance as mpf
import numpy as np

# Load the data
df = pd.read_pickle('BTCUSDT_1m_Binance_Updated.pkl')

# Convert 'Close time' to datetime if it's not already
df['Close time'] = pd.to_datetime(df['Close time'])

# Set 'Close time' as the index of the DataFrame
df.set_index('Close time', inplace=True)

ma_period = 120
smooth_period = 60  # Secondary smoothing window

# Calculate the SMA and shift it backward by half the ma_period
df[f'SMA_{ma_period}'] = df['Close'].rolling(window=ma_period).mean().shift(-int(ma_period/2))

# Apply additional smoothing to the SMA
df[f'Smoothed_SMA_{ma_period}'] = df[f'SMA_{ma_period}'].rolling(window=smooth_period).mean().shift(-int(smooth_period/2))

# Calculate the first derivative (rate of change) of the Smoothed SMA
df[f'Smoothed_SMA_{ma_period}_deriv'] = df[f'Smoothed_SMA_{ma_period}'].diff()

# Identify where the derivative is zero (crossings)
zero_crossings = df[f'Smoothed_SMA_{ma_period}_deriv'].abs() < 0.01  # Threshold for zero, may need adjustment
df['markers'] = df[f'Smoothed_SMA_{ma_period}'][zero_crossings]

# Define thresholds
positive_threshold = 20
negative_threshold = -20

# Group by zero crossings and calculate resetting cumulative sum
groups = zero_crossings.cumsum()
df['cumulative_deriv'] = df.groupby(groups)[f'Smoothed_SMA_{ma_period}_deriv'].cumsum()

# Reset the cumulative sum if it doesn't cross the thresholds before the next zero crossing
def reset_cumsum(group):
    if group.max() < positive_threshold and group.min() > negative_threshold:
        return pd.Series([0] * len(group), index=group.index)
    return group

df['cumulative_deriv'] = df.groupby(groups)['cumulative_deriv'].transform(reset_cumsum)

# Calculate the sign of 'cumulative_deriv'
signs = np.sign(df['cumulative_deriv'])

# Identify where the sign changes (including from zero to non-zero)
change_points = signs.diff().ne(0)

print(df['cumulative_deriv'][445:449])
print(signs[445:449])
print(change_points[445:449])

print(df['cumulative_deriv'][885:889])
print(signs[885:889])
print(change_points[885:889])

# Plotting setup
apd = [
    mpf.make_addplot(df[f'SMA_{ma_period}'], color='blue', ylabel='SMA'),
    mpf.make_addplot(df[f'Smoothed_SMA_{ma_period}'], color='green', ylabel='Smoothed SMA'),
    mpf.make_addplot(df['markers'], type='scatter', markersize=200, marker='^', color='red'),
    mpf.make_addplot(df[f'Smoothed_SMA_{ma_period}_deriv'], panel=1, color='magenta', ylabel='Derivative'),
    mpf.make_addplot(df['cumulative_deriv'], panel=2, color='orange', ylabel='Cumulative Derivative'),
]

# Plotting with mplfinance
mpf.plot(df, type='candle', style='charles',
         title='BTCUSDT with Threshold-Based Resetting Cumulative Derivative',
         ylabel='Price (USD)',
         volume=False,
         figratio=(12,8),
         addplot=apd,
         panel_ratios=(3, 1, 1))  # Adjust subplot sizes