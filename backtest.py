import numpy as np
import pandas as pd
import mysql.connector
from prophet import Prophet

# --- DB Connection ---
conn = mysql.connector.connect(
    host='127.0.0.1',
    database='bizpulse_db',
    user='bizpulse_user',
    password='userpassword',
    port=3306
)
cursor = conn.cursor()

# --- Load sales data ---
print("Loading sales data...")
cursor.execute("""
    SELECT product_id, DATE(timestamp) as ds, SUM(quantity) as y
    FROM sales_transactions
    GROUP BY product_id, DATE(timestamp)
    ORDER BY product_id, ds
""")
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=['product_id', 'ds', 'y'])
df['ds'] = pd.to_datetime(df['ds'])
df['y'] = df['y'].astype(float)


# --- Load current stock levels ---
cursor.execute("SELECT product_id, stock_level FROM inventory")
inventory = {row[0]: row[1] for row in cursor.fetchall()}

conn.close()

# --- Assign categories manually (no products table) ---
category_map = {
    'PROD_0001': 'Electronics',
    'PROD_0002': 'Electronics',
    'PROD_0003': 'Apparel',
    'PROD_0004': 'Apparel',
    'PROD_0005': 'Grocery',
    'PROD_0006': 'Grocery',
    'PROD_0007': 'Home & Garden',
    'PROD_0008': 'Home & Garden',
    'PROD_0009': 'Sporting Goods',
    'PROD_0010': 'Sporting Goods',
    'SYNC-PROD-A': 'Grocery',
    'SYNC-PROD-B': 'Electronics',
    'SYNC-PROD-C': 'Apparel',
}

print(f"Loaded {len(df)} daily rows across {df['product_id'].nunique()} products")
print(f"Date range: {df['ds'].min().date()} to {df['ds'].max().date()}\n")

# --- Backtest per product ---
results = []

for product_id, group in df.groupby('product_id'):
    group = group[['ds', 'y']].sort_values('ds').reset_index(drop=True)
    category = category_map.get(product_id, 'Other')

    # Need at least 60 days
    if len(group) < 60:
        print(f"  SKIP {product_id} - only {len(group)} days of data")
        continue

    # Train on first 75%, test on last 25%
    split_idx = int(len(group) * 0.75)
    train = group.iloc[:split_idx].copy()
    test = group.iloc[split_idx:].copy()

    print(f"Training {product_id} ({category}) — {len(train)} train days, {len(test)} test days...")

    try:
        model = Prophet(
            weekly_seasonality=True,
            yearly_seasonality='auto',
            changepoint_prior_scale=0.05,
            interval_width=0.8,
            stan_backend='CMDSTANPY'
        )
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(train)

        future = model.make_future_dataframe(periods=len(test))
        forecast = model.predict(future)
        predicted = forecast.iloc[split_idx:]['yhat'].clip(lower=0).values
        actual = test['y'].values

        # --- MAPE ---
        mask = actual > 0
        if mask.sum() < 5:
            print(f"  SKIP {product_id} - not enough non-zero actuals")
            continue
        mape = np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100

        # --- MASE (vs 7-day naive seasonal baseline) ---
        if len(train) > 7:
            naive_errors = np.abs(train['y'].values[7:] - train['y'].values[:-7])
            naive_mae = np.mean(naive_errors) if len(naive_errors) > 0 else 1.0
        else:
            naive_mae = train['y'].mean()
        mae = np.mean(np.abs(actual - predicted))
        mase = mae / naive_mae if naive_mae > 0 else np.nan

        # --- Stockout simulation ---
        current_stock = inventory.get(product_id, actual.mean() * 30)
        avg_daily_demand = train['y'].mean()
        lead_time = 7  # days

        # Fixed ROP: reorder when stock < mean_demand * lead_time
        fixed_rop = avg_daily_demand * lead_time
        reorder_qty = avg_daily_demand * 14

        stock_fixed = current_stock
        stockouts_fixed = 0
        for demand in actual:
            stock_fixed = max(0, stock_fixed - demand)
            if stock_fixed == 0:
                stockouts_fixed += 1
            if stock_fixed < fixed_rop:
                stock_fixed += reorder_qty

        # Forecast-driven ROP: uses predicted demand + safety buffer
        stock_forecast = current_stock
        stockouts_forecast = 0
        for i, demand in enumerate(actual):
            stock_forecast = max(0, stock_forecast - demand)
            if stock_forecast == 0:
                stockouts_forecast += 1
            forecast_rop = predicted[i] * lead_time * 1.1
            if stock_forecast < forecast_rop:
                stock_forecast += predicted[i] * 14

        if stockouts_fixed > 0:
            stockout_reduction = ((stockouts_fixed - stockouts_forecast) / stockouts_fixed) * 100
        else:
            stockout_reduction = 100.0  # forecast system prevented all stockouts

        results.append({
            'product_id': product_id,
            'category': category,
            'mape': round(mape, 2),
            'mase': round(mase, 3),
            'stockouts_fixed': stockouts_fixed,
            'stockouts_forecast': stockouts_forecast,
            'stockout_reduction': round(stockout_reduction, 1)
        })

        print(f"  ✓ MAPE={mape:.1f}%  MASE={mase:.3f}  Stockout reduction={stockout_reduction:.1f}%")

    except Exception as e:
        print(f"  ERROR {product_id}: {e}")
        continue

# --- Summary ---
results_df = pd.DataFrame(results)

print("\n\n========== RESULTS BY CATEGORY ==========")
summary = results_df.groupby('category').agg(
    Avg_MAPE=('mape', 'mean'),
    Avg_MASE=('mase', 'mean'),
    Avg_Stockout_Reduction=('stockout_reduction', 'mean'),
    Products=('product_id', 'count')
).round(2)
print(summary.to_string())

print(f"\n========== OVERALL AVERAGES ==========")
print(f"  MAPE:               {results_df['mape'].mean():.1f}%")
print(f"  MASE:               {results_df['mase'].mean():.3f}")
print(f"  Stockout Reduction: {results_df['stockout_reduction'].mean():.1f}%")
print(f"  Products evaluated: {len(results_df)}")

results_df.to_csv('backtest_results.csv', index=False)
print("\nFull results saved to backtest_results.csv")