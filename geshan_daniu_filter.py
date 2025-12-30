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

def check_geshan_daniu(df):
    """
    战法名称：隔山打牛战法
    核心逻辑：
    1. 寻找标杆：涨停（pct > 9.5%）次日出现一根大阴线（放量阴），量能需创20日新高。
    2. 震荡洗盘：随后的调整过程中，股价始终不跌破该“放量阴”的最低价。
    3. 缩量信号：在放量阴之后，出现一根明显的“缩量阴”。
    4. 阳线间隔：放量阴与缩量阴之间必须有阳线存在（代表有承接）。
    5. 买点：今日股价突破缩量阴的最高价。
    """
    if len(df) < 30: return None
    
    close = df['close'].values
    low = df['low'].values
    high = df['high'].values
    vol = df['volume'].values
    pct = df['pct_chg'].values
    open_p = df['open'].values

    # 寻找过去20天内的“涨停+放量阴”组合
    # 为保证时效性，放量阴出现在过去10天到3天之间
    for i in range(3, 15):
        idx = len(df) - i
        # 条件1：涨停（前一天）+ 放量阴（idx位置）
        if idx > 0 and pct[idx-1] > 9.5 and close[idx] < open_p[idx]:
            # 量能是20天新高
            if vol[idx] == max(vol[idx-20 : idx+1]):
                big_yin_idx = idx
                big_yin_low = low[idx]
                big_yin_vol = vol[idx]
                
                # 条件2：后续调整不破大阴低点
                subsequent_data = df.iloc[big_yin_idx + 1:]
                if any(subsequent_data['low'] < big_yin_low):
                    continue
                
                # 条件3：放量阴之后存在缩量阴
                # 条件4：放量阴与缩量阴之间要有阳线
                has_yang_between = False
                small_yin_idx = -1
                
                for j in range(big_yin_idx + 1, len(df) - 1):
                    # 判断是否有阳线
                    if close[j] > open_p[j]:
                        has_yang_between = True
                    
                    # 判断是否为缩量阴 (量能显著小于放量阴，且收阴)
                    if has_yang_between and close[j] < open_p[j] and vol[j] < big_yin_vol * 0.6:
                        small_yin_idx = j
                
                # 条件5：买点突破 (今日最高价突破了缩量阴的最高价)
                if small_yin_idx != -1 and has_yang_between:
                    small_yin_high = high[small_yin_idx]
                    if close[-1] > small_yin_high:
                        return "隔山打牛"
                        
    return None

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None 
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty: return None
        
        # 预处理百分比
        if df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        if check_geshan_daniu(df):
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
    file_path = os.path.join(month_dir, f'geshan_daniu_{ts}.csv')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"隔山打牛筛选完成：捕捉到 {len(final_df)} 只符合条件的个股。")

if __name__ == '__main__':
    main()
