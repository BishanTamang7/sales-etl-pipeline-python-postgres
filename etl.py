import pandas as pd
from sqlalchemy import create_engine

# EXTRACT
df = pd.read_csv("data/sales_raw.csv")

# TRANSFORM
df = df.dropna(subset=["order_date"])
df["order_date"] = pd.to_datetime(df["order_date"])
df["total_price"] = df["quantity"] * df["price"]

# LOAD
engine = create_engine(
    "postgresql://username:password@localhost:5432/etl_db"
)

df.to_sql("sales", engine, if_exists="append", index=False)

print("ETL process completed successfully")
