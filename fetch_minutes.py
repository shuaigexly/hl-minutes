import os
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from hyperliquid.info import Info

from config import (
    COINS,
    INTERVALS,
    DATA_DIR,
    CHECKPOINT_FILE,
    CHUNK_HOURS,
    API_SLEEP,
    YEARS_BACK,
)

info = Info(skip_ws=True)

# ----------------------------
# 工具函数
# ----------------------------

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    with open(CHECKPOINT_FILE, "r") as f:
        return json.load(f)

def save_checkpoint(ckpt):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(ckpt, f, indent=2)

def save_parquet_incremental(path, df_new):
    # 强制把 t / T 转成 int，避免 datetime 类型混入
    df_new["t"] = df_new["t"].astype("int64")
    df_new["T"] = df_new["T"].astype("int64")

    if os.path.exists(path):
        df_old = pd.read_parquet(path)

        # 同样强制旧数据转换类型
        df_old["t"] = df_old["t"].astype("int64")
        df_old["T"] = df_old["T"].astype("int64")

        df = pd.concat([df_old, df_new], ignore_index=True)
        df.drop_duplicates(subset=["t"], inplace=True)
        df.sort_values("t", inplace=True)
    else:
        df = df_new

    df.to_parquet(path, index=False)
    print(f"💾 Saved: {path} (rows={len(df)})")



# ----------------------------
# 主循环：多币种 + 多周期
# ----------------------------

def fetch_all():
    ckpt = load_checkpoint()
    now_ms = int(time.time() * 1000)
    chunk_ms = CHUNK_HOURS * 3600 * 1000

    for coin in COINS:
        for interval in INTERVALS:

            key = f"{coin}-{interval}"
            print(f"\n🚀 Start: {key}")

            # 初始化起点
            if key not in ckpt:
                start_ms = now_ms - YEARS_BACK * 365 * 24 * 3600 * 1000
            else:
                start_ms = ckpt[key]

            while start_ms < now_ms:
                end_ms = min(start_ms + chunk_ms, now_ms)

                print(f"⏱ {coin} {interval} | {datetime.utcfromtimestamp(start_ms/1000)} → {datetime.utcfromtimestamp(end_ms/1000)}")

                data = info.candles_snapshot(
                    name=coin,
                    interval=interval,
                    startTime=start_ms,
                    endTime=end_ms
                )

                if data:
                    df = pd.DataFrame(data)

                    os.makedirs(f"{DATA_DIR}/{coin}", exist_ok=True)
                    out_path = f"{DATA_DIR}/{coin}/{interval}.parquet"
                    save_parquet_incremental(out_path, df)

                # 更新断点
                ckpt[key] = end_ms
                save_checkpoint(ckpt)

                start_ms = end_ms
                time.sleep(API_SLEEP)


if __name__ == "__main__":
    fetch_all()