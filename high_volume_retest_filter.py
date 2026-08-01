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

def check_high_volume_logic(df):
    """
    战法名称：高量回踩不破战法
    核心逻辑：
    1. 寻找标杆：过去 5-15 天内出现过一根“高量柱”（成交量 > 5日均量2倍，且收阳线）。
    2. 确认支撑：记录该高量柱的最低价（或开盘价）作为防御线。
    3. 缩量回踩：目前处于回调阶段，成交量显著萎缩（小于高量柱的一半）。
    4. 不破必火：当前股价回调至高量柱支撑位附近，且收盘价未跌破高量柱最低价。
    """
    if len(df) < 30: return None
    
    close = df['close'].values
    low = df['low'].values
    vol = df['volume'].values
    open_p = df['open'].values
    
    # 基础价格过滤 (5-30元)
    if not (5.0 <= close[-1] <= 30.0): return None

    # 计算5日均量
    ma_vol5 = df['volume'].rolling(5).mean().values

    # --- 步骤一：寻找过去 15 天内的“高量标杆” ---
    target_idx = -1
    for i in range(2, 16): # 避开今日，寻找之前的标杆
        idx = len(df) - i
        # 条件：放量倍数 > 2 且是阳线
        if vol[idx] > ma_vol5[idx-1] * 2.0 and close[idx] > open_p[idx]:
            target_idx = idx
            break
            
    if target_idx == -1: return None

    # --- 步骤二：锁定防御位 ---
    # 取高量柱的最低价作为硬性支撑位
    support_price = low[target_idx]
    
    # --- 步骤三：检查回踩质量 ---
    # 1. 回踩不破：最新价格必须在支撑位上方，且距离支撑位不超过 3%（即在支撑区）
    if not (close[-1] >= support_price and close[-1] <= support_price * 1.03):
        return None
    
    # 2. 期间未曾有效跌破：从高量柱至今，收盘价均不低于支撑位
    period_closes = close[target_idx+1:]
    if any(pc < support_price for pc in period_closes):
        return None
        
    # 3. 缩量回踩：今日成交量需小于高量柱成交量的 50%
    if vol[-1] > vol[target_idx] * 0.5:
        return None

    return "高量不破"

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None 
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty: return None
        
        # 处理异常数据
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        if check_high_volume_logic(df):
            return code
    except:
        return None
    return None

def main():
    if not os.path.exists(NAMES_FILE): return
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    valid_codes = set(names_df[~names_df['name'].str.contains('ST|st')]['code'])

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
    file_path = os.path.join(month_dir, f'high_vol_retest_{ts}.csv')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"高量回踩筛选完成：共匹配到 {len(final_df)} 只潜力股。")

if __name__ == '__main__':
    main()
