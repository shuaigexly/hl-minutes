import os
import time
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from hyperliquid.info import Info
from hyperliquid.utils.error import ClientError
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

def safe_call_with_retry(func, max_retries=10, **kwargs):
    """
    通用重试包装：
    - 专门处理 Hyperliquid 的 429 限流错误
    - 其他错误直接抛出
    """
    for attempt in range(1, max_retries + 1):
        try:
            # 正常调用，比如 func(name=..., interval=..., startTime=..., endTime=...)
            return func(**kwargs)

        except ClientError as e:
            # 429：rate limit
            if getattr(e, "status_code", None) == 429:
                # 指数退避，最多等到 30 秒
                sleep_time = min(5 * attempt, 30) + random.uniform(0, 1)
                print(f"⚠️  收到 429 限流，第 {attempt}/{max_retries} 次重试，休眠 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)
                continue

            # 其他状态码，直接抛出（说明不是限流，是别的问题）
            raise

    raise RuntimeError("连续多次因 429 限流失败，放弃本次区间。")

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

                data = safe_call_with_retry(
                    info.candles_snapshot,
                    name=coin,
                    interval=interval,
                    startTime=int(start_ms),
                    endTime=int(end_ms),
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
