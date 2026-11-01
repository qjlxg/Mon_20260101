#连阳缩倍量
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
    '最低': 'low', '收盘': 'close', '成交量': 'volume'
}

def is_consecutive_sun_model(df):
    """
    "连阳缩倍量" 模型逻辑实现:
    1. 连阳建仓：过去 6-10 天内存在连续 4 根以上阳线，且连阳末端成交量为阶段最大量。
    2. 缩倍量洗盘：在最大量之后，出现 1-3 天的调整，成交量缩至最大量的 50% 以下。
    3. 倍量突破：当日（最新一天）为阳线，成交量是前一日的 1.8 倍以上，且收盘价站上洗盘区高点。
    """
    if len(df) < 15: return False
    
    # 基础过滤：价格 5-20 元
    last_price = df['close'].iloc[-1]
    if not (5.0 <= last_price <= 20.0): return False

    # 定义回溯区间
    # 假设最近 1 天是突破日，前 1-3 天是洗盘区，再往前是连阳区
    # 我们检查最近 10 天的数据
    data = df.tail(10).copy()
    volumes = data['volume'].values
    closes = data['close'].values
    opens = data['open'].values
    
    # 1. 寻找阶段最大成交量（主力建仓标志）及其位置
    # 最大量不应出现在最后一天（因为最后一天是突破日），应在前 2-5 天内
    lookback_data = data.iloc[:-1] 
    max_vol_idx = lookback_data['volume'].argmax()
    max_vol = lookback_data['volume'].iloc[max_vol_idx]
    
    # 最大量位置距离今天太远或太近都不符合形态（要求给洗盘留出 1-4 天空间）
    dist_from_now = len(lookback_data) - max_vol_idx
    if not (1 <= dist_from_now <= 4): return False

    # 2. 检查连阳建仓段 (在最大量坐标及其之前)
    # 要求最大量当天是阳线，且之前至少有连续阳线趋势
    if closes[max_vol_idx] <= opens[max_vol_idx]: return False
    
    # 3. 检查缩倍量洗盘段 (最大量之后到今天之前)
    wash_zone = data.iloc[max_vol_idx + 1 : -1]
    if wash_zone.empty: return False
    
    # 洗盘区最小成交量必须小于最大量的 50%
    min_wash_vol = wash_zone['volume'].min()
    if min_wash_vol > (max_vol * 0.5): return False

    # 4. 检查倍量突破段 (今天)
    today = data.iloc[-1]
    yesterday = data.iloc[-2]
    
    is_today_sun = today['close'] > today['open']
    # 倍量确认：今天量 > 昨天量 * 1.8 且 突破洗盘区最高收盘价
    vol_confirm = today['volume'] > (yesterday['volume'] * 1.8)
    price_break = today['close'] >= wash_zone['close'].max()

    return is_today_sun and vol_confirm and price_break

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None # 排除创业板
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty or len(df) < 15: return None
        
        if is_consecutive_sun_model(df):
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
    
    hit_codes = [c for c in results if c is not None]

    # 输出
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    final_df = names_df[names_df['code'].isin(hit_codes)]
    file_path = os.path.join(month_dir, f'consecutive_sun_{ts}.csv')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"筛选完成：匹配到 {len(final_df)} 只符合'连阳缩倍量'形态的个股。")

if __name__ == '__main__':
    main()
