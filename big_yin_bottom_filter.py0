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

def check_big_yin_logic(df):
    """
    战法名称：大阴线黄金坑战法
    包含形态：
    1. 牛回头：上升趋势中回踩10日线，缩量阴线。
    2. 物极必反：连续3-5根阴线下跌，累计跌幅>20%，出现长下影。
    3. 箱体回踩：放量突破箱体后，缩量阴线回踩箱体顶端。
    """
    if len(df) < 40: return None
    
    close = df['close'].values
    low = df['low'].values
    high = df['high'].values
    vol = df['volume'].values
    pct = df['pct_chg'].values
    open_p = df['open'].values

    # 1. 基础过滤：排除高位股 (涨幅已超100%不碰)
    low_60 = df['low'].tail(60).min()
    if close[-1] > low_60 * 2.0: return None

    # 计算均线
    ma10 = df['close'].rolling(10).mean().values
    ma20 = df['close'].rolling(20).mean().values

    # --- 形态一：支撑线上的“牛回头” ---
    # 条件：10日线上行，今日是大阴线(<-3%)，回踩10日线不破，且显著缩量
    if ma10[-1] > ma10[-2] and pct[-1] < -3.0:
        if low[-1] <= ma10[-1] * 1.01 and close[-1] >= ma10[-1] * 0.99:
            avg_vol_5 = vol[-6:-1].mean()
            if vol[-1] < avg_vol_5 * 0.7: # 缩量30%以上
                return "牛回头(回踩10MA)"

    # --- 形态二：连续大跌后的“物极必反” ---
    # 条件：最近4天累计跌幅 > 18%，且今日出现长下影线
    last_4_pct = (close[-1] - close[-5]) / close[-5] * 100
    if last_4_pct < -18.0:
        shadow = (min(open_p[-1], close[-1]) - low[-1]) / (high[-1] - low[-1] + 0.001)
        if shadow > 0.5: # 下影线占实体一半以上
            return "物极必反(超跌反弹)"

    # --- 形态三：箱体突破后的回踩 ---
    # 逻辑：突破30日高点后回踩
    box_top = high[-30:-5].max()
    if high[-5:-1].max() > box_top * 1.03: # 5天内有过明显突破
        if close[-1] <= box_top * 1.02 and close[-1] >= box_top * 0.98:
            if vol[-1] < vol[-2]: # 缩量回踩
                return "箱体回踩"

    return None

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None 
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty: return None
        
        # 统一处理百分比
        if df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        signal = check_big_yin_logic(df)
        if signal:
            return {'code': code, 'type': signal}
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
    
    found = [r for r in results if r is not None]
    
    # 输出结果
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    if found:
        res_df = pd.DataFrame(found)
        final_df = pd.merge(res_df, names_df, on='code')
        file_path = os.path.join(month_dir, f'big_yin_bottom_{ts}.csv')
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成：共发现 {len(final_df)} 只黄金坑机会。")

if __name__ == '__main__':
    main()
