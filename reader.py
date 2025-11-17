import pandas as pd
import glob

from config import DATA_DIR

# -----------------------------
# 1. 读取某个币 + 某个周期
# -----------------------------
def load_kline(coin, interval):
    path = f"{DATA_DIR}/{coin}/{interval}.parquet"
    return pd.read_parquet(path)


# -----------------------------
# 2. 读取所有币 + 所有周期
# -----------------------------
def load_all():
    files = glob.glob(f"{DATA_DIR}/**/*.parquet", recursive=True)
    dfs = []

    for f in files:
        coin = f.split("\\")[-2] if "\\" in f else f.split("/")[-2]
        interval = f.split("\\")[-1].replace(".parquet", "") if "\\" in f else f.split("/")[-1].replace(".parquet", "")

        df = pd.read_parquet(f)
        df["coin"] = coin
        df["interval"] = interval
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


# -----------------------------
# 3. 快速特征示例：计算收益率
# -----------------------------
def compute_returns(df):
    df = df.sort_values("t")
    df["c"] = df["c"].astype(float)
    df["ret"] = df["c"].pct_change()
    return df


# -----------------------------
# 4. 简单因子例子：波动率
# -----------------------------
def compute_volatility(df, window=60):
    df = df.sort_values("t")
    df["c"] = df["c"].astype(float)
    df["logret"] = (df["c"] / df["c"].shift(1)).apply(lambda x: 0 if x<=0 else pd.np.log(x))
    df["vol"] = df["logret"].rolling(window).std()
    return df
