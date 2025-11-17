# config.py

# 想要采集哪些币种
COINS = [
    "BTC",
    "ETH",
    "SOL",
]

# 想要采集哪些周期（必须是 HL 支持的）
# 可选： "1m", "5m", "15m", "30m", "1h", "4h", "1d"
INTERVALS = [
    "1m",
]

# 数据存储目录
DATA_DIR = "data"

# 断点续传文件
CHECKPOINT_FILE = "checkpoint.json"

# 每次 API 请求时间区间（小时）
# 例如每次拉 1 天
CHUNK_HOURS = 24

# API 调用之间的休眠（秒）
API_SLEEP = 0.25

YEARS_BACK = 3
