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

def get_available_perp_coins(info):
    """
    自动检测 Hyperliquid 永续合约的全部币种。
    如果 config.COINS 手动指定，则优先使用。
    否则取成交量前20的主流币。
    """
    from config import COINS as CONFIG_COINS

    # 1. 手动指定（优先级最高）
    if CONFIG_COINS is not None:
        print(f"📝 使用 config.py 手动指定的币种: {CONFIG_COINS}")
        return CONFIG_COINS

    # 2. 自动模式：获取成交量最多的前20币种
    print("🔍 COINS=None，自动模式：正在筛选成交量前20币种...")
    return get_top_volume_coins(info, top_n=20)


def get_top_volume_coins(info, top_n=20):
    """
    自动获取 Hyperliquid 永续合约中成交量前 top_n 的主流币种。
    使用 meta_and_asset_ctxs() 中的 dayNtlVlm（每日名义成交量）。
    """
    print("🔍 正在加载全部币种的成交量信息... (meta_and_asset_ctxs)")

    meta, asset_ctxs = info.meta_and_asset_ctxs()

    volume_list = []
    for asset, ctx in zip(meta["universe"], asset_ctxs):
        name = asset["name"]
        vol = float(ctx["dayNtlVlm"])
        volume_list.append((name, vol))

    # 按成交量排序
    volume_list.sort(key=lambda x: x[1], reverse=True)

    top = [c for c, v in volume_list[:top_n]]

    print(f"🔥 成交量前 {top_n} 的币种：")
    print(top)

    return top


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

    COINS = get_available_perp_coins(info)
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
