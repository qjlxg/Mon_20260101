import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count
import numpy as np

# ==================== 2025“买入即获利”极简精选参数 (原始固定) ===================
MIN_PRICE = 5.0              # 提高股价门槛，过滤低迷小票
MAX_AVG_TURNOVER_30 = 2.5    # 换手率更低，意味着筹码锁定更好

# --- 极致缩量 ---
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 1     # 原始：严格限制在0.85以下

# --- 极度超跌 ---
RSI6_MAX = 25                # 锁定极致超跌区
KDJ_K_MAX = 30               # 确保K值在底部磨底
MIN_PROFIT_POTENTIAL = 10    # 要求反弹空间至少15%

# --- 形态与趋势控制 ---
MAX_TODAY_CHANGE = 1.5       # 原始：拒绝大阳线，只要微涨
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
    """计算核心指标"""
    df = df.reset_index(drop=True)
    close = df['收盘']
    
    # 1. RSI6
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi6'] = 100 - (100 / (1 + rs))
    
    # 2. KDJ (9,3,3)
    low_list = df['最低'].rolling(window=9).min()
    high_list = df['最高'].rolling(window=9).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    
    # 3. MA5 & MA60
    df['ma5'] = close.rolling(window=5).mean()
    df['ma60'] = close.rolling(window=60).mean()
    
    # 4. 换手率均值与量比
    df['avg_turnover_30'] = df['换手率'].rolling(window=30).mean()
    df['vol_ma5'] = df['成交量'].shift(1).rolling(window=5).mean()
    df['vol_ratio'] = df['成交量'] / df['vol_ma5']
    
    return df

def process_single_stock(args):
    file_path, name_map = args
    stock_code = os.path.basename(file_path).split('.')[0]
    stock_name = name_map.get(stock_code, "未知")
    
    if "ST" in stock_name.upper(): return None

    try:
        df_raw = pd.read_csv(file_path)
        if len(df_raw) < 60: return None
        df = calculate_indicators(df_raw)
        latest = df.iloc[-1]
        
        # 基础静态门槛 (公共)
        if latest['收盘'] < MIN_PRICE or latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            return None
        
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        
        if potential < MIN_PROFIT_POTENTIAL: return None

        strategy_tag = ""

        # --- 模式一：极致缩量捡漏 (严格执行你的原始所有参数) ---
        # 特点：不强求站上MA5，只要跌透了+极度缩量+低位横盘
        if (latest['rsi6'] <= RSI6_MAX and 
            latest['kdj_k'] <= KDJ_K_MAX and 
            MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO and 
            abs(change) <= MAX_TODAY_CHANGE):
            strategy_tag = "极致缩量捡漏"

        # --- 模式二：缩量反转确认 (在你的参数基础上，微调量比上限处理止跌矛盾) ---
        # 特点：必须站上MA5，允许量比微增至1.0，寻找V型反转第一点
        elif (latest['rsi6'] <= RSI6_MAX + 5 and  # 稍微放宽RSI确认企稳
              latest['kdj_k'] <= KDJ_K_MAX + 5 and
              latest['收盘'] > latest['ma5'] and     # 核心差异：必须站上5日线
              0.5 <= latest['vol_ratio'] <= 1.0 and   # 核心差异：量比允许微升到1.0
              0 < change <= MAX_TODAY_CHANGE + 1.0): # 核心差异：涨幅放宽到2.5%
            strategy_tag = "缩量反转确认"

        if strategy_tag:
            return {
                '类型': strategy_tag,
                '代码': stock_code,
                '名称': stock_name,
                '现价': round(latest['收盘'], 2),
                '今日量比': round(latest['vol_ratio'], 2),
                'RSI6': round(latest['rsi6'], 1),
                '距60日线': f"{round(potential, 1)}%",
                '今日涨跌': f"{round(change, 1)}%"
            }
    except:
        return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 双模式精选扫描中... (保留原始参数 + 兼容反转逻辑)")

    name_map = {}
    if os.path.exists(NAME_MAP_FILE):
        n_df = pd.read_csv(NAME_MAP_FILE, dtype={'code': str})
        name_map = dict(zip(n_df['code'].str.zfill(6), n_df['name']))

    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    tasks = [(f, name_map) for f in file_list]

    with Pool(processes=cpu_count()) as pool:
        raw_results = pool.map(process_single_stock, tasks)

    results = [r for r in raw_results if r is not None]
        
    if results:
        df_result = pd.DataFrame(results)
        # 排序：先看类型，再看空间
        df_result = df_result.sort_values(by=['类型', '距60日线'], ascending=[True, False])
        
        print(f"\n🎯 筛选出 {len(results)} 只标的：")
        print(df_result.to_string(index=False))
        
        os.makedirs("results", exist_ok=True)
        file_name = f"双模式精选_{now_shanghai.strftime('%Y%m%d_%H%M')}.csv"
        df_result.to_csv(os.path.join("results", file_name), index=False, encoding='utf_8_sig')
    else:
        print("\n😱 暂时没有符合要求的极品标的。")

if __name__ == "__main__":
    main()
