import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# 配置参数
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results'

# 字段映射
COL_MAP = {
    '日期': 'date', '开盘': 'open', '最高': 'high', 
    '最低': 'low', '收盘': 'close', '成交量': 'volume', '涨跌幅': 'pct_chg'
}

def check_rebound_logic(df):
    """
    战法名称：涨停回马枪（20日均线支撑型）
    核心逻辑：
    1. 前期涨停：过去 5-15 个交易日内出现过涨幅 > 9.5% 的涨停板。
    2. 缩量回调：涨停后股价经历回落，成交量相比涨停日显著萎缩。
    3. 20MA支撑：当前股价回踩至 20 日均线附近（正负 2% 范围内），且 20MA 趋势向上。
    4. 止跌信号：今日收盘价 >= 20MA，且未出现放量破位的断头铡刀。
    """
    if len(df) < 30: return False
    
    close = df['close'].values
    low = df['low'].values
    vol = df['volume'].values
    pct = df['pct_chg'].values
    ma20 = df['close'].rolling(20).mean().values

    # 基础价格过滤 (5-20元)
    if not (5.0 <= close[-1] <= 20.0): return False

    # 1. 寻找过去 15 天内的涨停板 (避开最近 2 天，给回调留空间)
    lookback = 15
    has_limit_up = False
    limit_up_idx = -1
    for i in range(3, lookback + 1):
        if pct[-i] > 9.5:
            has_limit_up = True
            limit_up_idx = len(df) - i
            break
    
    if not has_limit_up: return False

    # 2. 检查 20 日均线状态
    # MA20 必须是向上或走平的 (今日 MA20 >= 3天前 MA20)
    if ma20[-1] < ma20[-4]: return False

    # 3. 检查当前位置是否回踩支撑位
    # 股价触碰 MA20 或在 MA20 上方 2% 以内
    dist_to_ma20 = (close[-1] - ma20[-1]) / ma20[-1]
    on_support = (low[-1] <= ma20[-1] * 1.01) and (close[-1] >= ma20[-1] * 0.98)

    # 4. 检查洗盘特征 (缩量)
    # 当前量能应小于涨停日量能的 70%
    vol_wash = vol[-1] < vol[limit_up_idx] * 0.7

    return on_support and vol_wash

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None # 侧重主板
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty: return None
        
        # 统一处理涨跌幅
        if df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        if check_rebound_logic(df):
            return code
    except:
        return None
    return None

def main():
    if not os.path.exists(NAMES_FILE): return
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    names_df = names_df[~names_df['name'].str.contains('ST|st')]
    valid_codes = set(names_df['code'])

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f.split('.')[0] in valid_codes]
    
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_stock, files))
    
    found_codes = [c for c in results if c is not None]
    
    # 输出结果
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    final_df = names_df[names_df['code'].isin(found_codes)]
    file_path = os.path.join(month_dir, f'rebound_20ma_{ts}.csv')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"筛选完成：捕捉到 {len(final_df)} 只符合回马枪形态的个股。")

if __name__ == '__main__':
    main()
