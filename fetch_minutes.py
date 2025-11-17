# fetch_minutes.py

import os
import json
import time
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
from hyperliquid.info import Info

from config import COINS, INTERVALS, DATA_DIR, CHECKPOINT_FILE, CHUNK_HOURS, API_SLEEP

# 首次全量时向前追溯的年数
YEARS_BACK = 3


def get_info_client() -> Info:
    """
    创建 Hyperliquid Info 客户端（不需要 websocket）
    """
    return Info(skip_ws=True)


def load_checkpoint() -> Dict[str, int]:
    """
    加载所有 coin_interval 的断点，
    格式大概是：
    {
        "BTC_1m": 1731800000000,
        "ETH_5m": 1731700000000,
        ...
    }
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    with open(CHECKPOINT_FILE, "r") as f:
        return json.load(f)


def save_checkpoint(ck: Dict[str, int]) -> None:
    """
    把所有断点写回 checkpoint.json
    """
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(ck, f)


def ensure_dirs() -> None:
    """
    创建数据目录：
    data/BTC/1m.parquet
    data/ETH/5m.parquet
    ...
    """
    for coin in COINS:
        coin_dir = os.path.join(DATA_DIR, coin)
        os.makedirs(coin_dir, exist_ok=True)


def parquet_path(coin: str, interval: str) -> str:
    return os.path.join(DATA_DIR, coin, f"{interval}.parquet")


def fetch_incremental_for_pair(
    info: Info,
    coin: str,
    interval: str,
    checkpoints: Dict[str, int],
) -> List[Dict[str, Any]]:
    """
    对单个 (coin, interval) 做增量/全量拉取，返回新增的 rows 列表。
    """
    key = f"{coin}_{interval}"
    now_ms = int(time.time() * 1000)

    # 决定起点：是断点续传还是从 YEARS_BACK 年前开始
    if key in checkpoints:
        start_ms = checkpoints[key] + 1
        print(f"⏯ [{coin} {interval}] 断点续传，从 {datetime.utcfromtimestamp(start_ms/1000)} 开始拉取")
    else:
        start_ms = now_ms - YEARS_BACK * 365 * 24 * 3600 * 1000
        print(f"🔰 [{coin} {interval}] 首次运行，从 {YEARS_BACK} 年前开始拉取")

    if start_ms >= now_ms:
        print(f"ℹ [{coin} {interval}] 已经是最新，无需拉取")
        return []

    chunk_ms = CHUNK_HOURS * 3600 * 1000
    cursor = start_ms
    all_rows: List[Dict[str, Any]] = []

    while cursor < now_ms:
        chunk_start = cursor
        chunk_end = min(cursor + chunk_ms, now_ms)

        print(
            f"⏱ [{coin} {interval}] 获取区间 "
            f"{datetime.utcfromtimestamp(chunk_start/1000)} → {datetime.utcfromtimestamp(chunk_end/1000)}"
        )

        try:
            data = info.candles_snapshot(
                name=coin,
                interval=interval,
                startTime=chunk_start,
                endTime=chunk_end,
            )
        except Exception as e:
            print(f"❌ [{coin} {interval}] 请求失败: {e}，5 秒后重试")
            time.sleep(5)
            continue

        if data:
            all_rows.extend(data)
            # 以最后一根K线的开盘时间 t 作为断点
            latest_t = data[-1]["t"]
            checkpoints[key] = latest_t
            save_checkpoint(checkpoints)

        cursor = chunk_end + 1
        time.sleep(API_SLEEP)

    return all_rows


def rows_to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    把 SDK 返回的 list[dict] 转成 DataFrame，并处理时间字段
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # t / T 是毫秒时间戳
    if "t" in df.columns:
        df["t"] = pd.to_datetime(df["t"], unit="ms")
    if "T" in df.columns:
        df["T"] = pd.to_datetime(df["T"], unit="ms")
    return df


def save_parquet_incremental(coin: str, interval: str, df_new: pd.DataFrame) -> None:
    """
    把新增数据 df_new 合并到 data/{coin}/{interval}.parquet 中。
    用 t 去重，保证不会重复。
    """
    if df_new.empty:
        print(f"ℹ [{coin} {interval}] 没有新数据，不写入")
        return

    ensure_dirs()
    path = parquet_path(coin, interval)

    if os.path.exists(path):
        df_old = pd.read_parquet(path)
        df = pd.concat([df_old, df_new], axis=0)
        if "t" in df.columns:
            df = df.drop_duplicates(subset=["t"]).sort_values("t")
    else:
        df = df_new

    df.to_parquet(path, index=False)
    print(f"✅ [{coin} {interval}] 保存成功: {path}  共 {len(df)} 行")


def main():
    info = get_info_client()
    checkpoints = load_checkpoint()

    for coin in COINS:
        for interval in INTERVALS:
            print(f"\n=== 开始处理 {coin} {interval} ===")
            rows = fetch_incremental_for_pair(info, coin, interval, checkpoints)
            df = rows_to_df(rows)
            save_parquet_incremental(coin, interval, df)

    print("\n🎉 所有币种 & 周期处理完成")


if __name__ == "__main__":
    main()
