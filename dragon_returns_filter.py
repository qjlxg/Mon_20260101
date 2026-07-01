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

def check_dragon_logic(df):
    """
    战法名称：龙回头战法（二次脉冲伏击）
    核心标准：
    1. 辨真龙：10个交易日内涨幅 > 50%，且涨停次数 >= 3次。
    2. 回调有度：回调幅度在前期涨幅的 30%-50% 之间。
    3. 支撑确认：回调不破 20日均线（生命线）。
    4. 量能萎缩：回调期间成交量萎缩至上涨期平均量能的 50% 以下。
    """
    if len(df) < 40: return None
    
    # 统一处理数据
    df = df.sort_values('date')
    close = df['close'].values
    high = df['high'].values
    vol = df['volume'].values
    pct = df['pct_chg'].values
    ma20 = df['close'].rolling(20).mean().values

    # --- 步骤一：寻找“真龙”基因 ---
    # 回溯过去20天，看是否有过连续走强阶段
    is_dragon = False
    dragon_peak_idx = -1
    dragon_start_idx = -1
    
    for i in range(5, 25): # 检查过去一段时间
        end_idx = len(df) - i
        start_idx = end_idx - 10
        if start_idx < 0: continue
        
        # 10日涨幅计算
        period_gain = (close[end_idx] - close[start_idx]) / close[start_idx]
        # 统计涨停次数 (涨幅 > 9.5% 计为涨停)
        limit_ups = np.sum(pct[start_idx:end_idx+1] > 9.5)
        
        if period_gain >= 0.50 and limit_ups >= 3:
            is_dragon = True
            dragon_peak_idx = end_idx
            dragon_start_idx = start_idx
            break
            
    if not is_dragon: return None

    # --- 步骤二：检查回调深度与支撑 ---
    # 涨幅空间
    up_range = close[dragon_peak_idx] - close[dragon_start_idx]
    # 回调空间
    current_drop = close[dragon_peak_idx] - close[-1]
    
    # 回调幅度在 30% - 50% 之间
    retrace_ratio = current_drop / up_range if up_range > 0 else 0
    if not (0.3 <= retrace_ratio <= 0.55): return None
    
    # 不能有效跌破 20日均线
    if close[-1] < ma20[-1] * 0.98: return None

    # --- 步骤三：检查量能萎缩 ---
    # 上涨期平均量能
    up_vol_avg = vol[dragon_start_idx : dragon_peak_idx + 1].mean()
    # 最近3日平均量能
    recent_vol_avg = vol[-3:].mean()
    
    if recent_vol_avg > up_vol_avg * 0.55: return None

    return "真龙回头"

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None # 侧重主板，妖股多发地
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty: return None
        
        if df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        res = check_dragon_logic(df)
        if res:
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
    file_path = os.path.join(month_dir, f'dragon_returns_{ts}.csv')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"龙回头筛选完成：共匹配到 {len(final_df)} 只潜力标的。")

if __name__ == '__main__':
    main()
