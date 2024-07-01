import pandas as pd
import numpy as np
import mplfinance as mpf
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(file_path):
    """
    Load the data from a pickle file.
    """
    try:
        df = pd.read_pickle(file_path)
        logging.info(f"Data loaded successfully from {file_path}")
    except FileNotFoundError:
        logging.error(f"File not found. Please check the file path: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading file: {e}")
        raise
    return df

def preprocess_data(df):
    """
    Convert 'Close time' to datetime and set as index.
    """
    if not pd.api.types.is_datetime64_any_dtype(df['Close time']):
        df['Close time'] = pd.to_datetime(df['Close time'])
    df.set_index('Close time', inplace=True)
    logging.info("Data preprocessing completed.")
    return df

def calculate_sma(df, ma_period, smooth_period):
    """
    Calculate SMA and smoothed SMA.
    """
    df[f'SMA_{ma_period}'] = df['Close'].rolling(window=ma_period).mean().shift(-int(ma_period / 2))
    df[f'Smoothed_SMA_{ma_period}'] = df[f'SMA_{ma_period}'].rolling(window=smooth_period).mean().shift(-int(smooth_period / 2))
    logging.info(f"SMA and smoothed SMA calculated with periods {ma_period} and {smooth_period}.")
    return df

def calculate_derivative(df, ma_period, inflection_tol):
    """
    Calculate the first derivative of the moving average and zero crossings.
    """
    df[f'Smoothed_SMA_{ma_period}_deriv'] = df[f'Smoothed_SMA_{ma_period}'].diff()
    zero_crossings = df[f'Smoothed_SMA_{ma_period}_deriv'].abs() < inflection_tol
    df['markers'] = df[f'Smoothed_SMA_{ma_period}'][zero_crossings]
    logging.info("Derivative and zero crossings calculated.")
    return df, zero_crossings

def calculate_cumulative_deriv(df, zero_crossings, noise_threshold):
    """
    Calculate cumulative derivative before inflection and reject regions that don't reach the noise threshold.
    """
    groups = zero_crossings.cumsum()
    df['cumulative_deriv'] = df.groupby(groups)[f'Smoothed_SMA_{ma_period}_deriv'].cumsum()

    def reset_cumsum(group):
        if group.max() < noise_threshold and group.min() > -noise_threshold:
            return pd.Series([0] * len(group), index=group.index)
        return group

    df['cumulative_deriv'] = df.groupby(groups)['cumulative_deriv'].transform(reset_cumsum)
    logging.info("Cumulative derivative calculated and noise filtered.")
    return df

def identify_regions(df):
    """
    Identify region boundaries based on the sign of the cumulative derivative.
    """
    signs = np.sign(df['cumulative_deriv'])
    current_sign = None
    regions = []
    region_change_timestamps = []

    for idx, sign in enumerate(signs):
        if sign != 0 and sign != current_sign:
            if current_sign is not None:
                region_change_timestamps.append(df.index[idx])
            current_sign = sign
        regions.append(current_sign if current_sign is not None else 0)

    df['region_boundaries'] = regions
    df['region_change_markers'] = np.nan
    df.loc[region_change_timestamps, 'region_change_markers'] = df['Close']
    logging.info("Up/down trend regions identified and region change markers set.")
    return df, region_change_timestamps

def identify_high_low_markers(df, region_change_timestamps, X):
    """
    Identify largest/smallest high/low within identified trend regions of 
    an OHLC series and generate marker series for plotting.
    """
    high_markers = []
    low_markers = []

    if region_change_timestamps:
        first_boundary = region_change_timestamps[0]
        start_idx = 0
        end_idx = min(len(df) - 1, df.index.get_loc(first_boundary) + X)
        first_region_sign = -df.loc[first_boundary, 'region_boundaries']

        if first_region_sign > 0:
            high_idx = df['High'].iloc[start_idx:end_idx + 1].idxmax()
            high_markers.append(high_idx)
        elif first_region_sign < 0:
            low_idx = df['Low'].iloc[start_idx:end_idx + 1].idxmin()
            low_markers.append(low_idx)

    for i in range(1, len(region_change_timestamps)):
        current_ts = region_change_timestamps[i]
        previous_ts = region_change_timestamps[i - 1]

        start_idx = max(0, df.index.get_loc(previous_ts) - X)
        end_idx = min(len(df) - 1, df.index.get_loc(current_ts) + X)

        region_sign = -df.loc[current_ts, 'region_boundaries']

        if region_sign > 0:
            high_idx = df['High'].iloc[start_idx:end_idx + 1].idxmax()
            high_markers.append(high_idx)
        elif region_sign < 0:
            low_idx = df['Low'].iloc[start_idx:end_idx + 1].idxmin()
            low_markers.append(low_idx)

    df['high_markers'] = np.nan
    df['low_markers'] = np.nan
    df.loc[high_markers, 'high_markers'] = df['High']
    df.loc[low_markers, 'low_markers'] = df['Low']
    logging.info("High and low markers for HL inflections identified.")
    return df

def plot_data(df, diagnostics, ma_period):
    """
    Plot the data using mplfinance, diagnostic plots show everything to help you tune parameters.
    """
    if diagnostics:
        apd = [
            mpf.make_addplot(df[f'SMA_{ma_period}'], color='blue', ylabel='SMA'),
            mpf.make_addplot(df[f'Smoothed_SMA_{ma_period}'], color='green', ylabel='Smoothed SMA'),
            mpf.make_addplot(df['markers'], type='scatter', markersize=200, marker='^', color='red'),
            mpf.make_addplot(df[f'Smoothed_SMA_{ma_period}_deriv'], panel=1, color='magenta', ylabel='Derivative'),
            mpf.make_addplot(df['cumulative_deriv'], panel=2, color='orange', ylabel='Cumulative Derivative'),
            mpf.make_addplot(df['region_boundaries'], panel=2, color='blue', ylabel='Regions'),
            mpf.make_addplot(df['region_change_markers'], type='scatter', markersize=200, marker='o', color='yellow'),
            mpf.make_addplot(df['high_markers'], type='scatter', markersize=200, marker='^', color='blue'),
            mpf.make_addplot(df['low_markers'], type='scatter', markersize=200, marker='v', color='purple')
        ]

        mpf.plot(df, type='candle', style='charles',
                 title='BTCUSDT with Threshold-Based Resetting Cumulative Derivative',
                 ylabel='Price (USD)',
                 volume=False,
                 figratio=(12, 8),
                 addplot=apd,
                 panel_ratios=(3, 1, 1))
    else:
        apd = [
            mpf.make_addplot(df['high_markers'], type='scatter', markersize=200, marker='^', color='blue'),
            mpf.make_addplot(df['low_markers'], type='scatter', markersize=200, marker='v', color='purple')
        ]

        mpf.plot(df, type='candle', style='charles',
                 title='BTCUSDT Price with High and Low Markers',
                 ylabel='Price (USD)',
                 volume=False,
                 figratio=(12, 8),
                 addplot=apd)
    logging.info("Data plotted.")

if __name__ == "__main__":
    # Adjustable parameters
    ma_period = 120       # Smoothing ma for noisy OHLC
    smooth_period = 60    # Secondary smoothing window for ma
    diagnostics = False   # Set to False to disable diagnostic plots
    noise_threshold = 20  # Adjust to change tolerance to noise in trends, larger values iron out more noise
    X = 30                # Pad regions to search for extrema due to potential ma lag/lead of significant highs/lows
    inflection_tol = 0.1  # Due to volatility, some zero crosses don't hit 0 exactly, adjust to catch more/less inflections, too much catches noise
    file_path = 'BTCUSDT_1m_Binance_Updated.pkl'
    
    # Main execution
    df = load_data(file_path)
    df = preprocess_data(df)
    df = calculate_sma(df, ma_period, smooth_period)
    df, zero_crossings = calculate_derivative(df, ma_period, inflection_tol)
    df = calculate_cumulative_deriv(df, zero_crossings, noise_threshold)
    df, region_change_timestamps = identify_regions(df)
    df = identify_high_low_markers(df, region_change_timestamps, X)
    plot_data(df, diagnostics, ma_period)
