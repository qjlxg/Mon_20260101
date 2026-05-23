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

def calculate_macd(prices):
    exp1 = prices.ewm(span=12, adjust=False).mean()
    exp2 = prices.ewm(span=26, adjust=False).mean()
    dif = exp1 - exp2
    dea = dif.ewm(span=9, adjust=False).mean()
    hist = 2 * (dif - dea)
    return dif, dea, hist

def check_yangjia_logic(df):
    """
    战法名称：炒股养家低吸战法（包含四种核心策略）
    """
    if len(df) < 60: return None
    
    close = df['close'].values
    high = df['high'].values
    low = df['low'].values
    vol = df['volume'].values
    
    # 基础过滤：价格 5-20 元
    if not (5.0 <= close[-1] <= 20.0): return None

    # 计算技术指标
    ma10 = df['close'].rolling(10).mean().values
    ma20 = df['close'].rolling(20).mean().values
    dif, dea, hist = calculate_macd(df['close'])
    
    # --- 策略一：突破回踩低吸 ---
    # 逻辑：近期有放量突破前期高点，目前缩量回踩不破
    recent_high = high[-20:-5].max()
    if close[-1] > recent_high * 0.98 and close[-1] < recent_high * 1.05: # 在支撑位附近
        if vol[-1] < vol[-5:-1].mean() and close[-1] >= recent_high:
            return "突破回踩"

    # --- 策略二：MACD底背离 ---
    # 逻辑：股价创新低，但DIF不创新低
    if close[-1] < close[-20:-1].min(): # 股价创新低
        recent_dif = dif.tail(20).values
        if recent_dif[-1] > recent_dif[:-1].min(): # DIF未创新低
            if hist.iloc[-1] > hist.iloc[-2]: # 动能开始转强
                return "MACD底背离"

    # --- 策略三：重要支撑位（均线支撑） ---
    # 逻辑：回踩半年线(120)或年线(250)企稳
    if len(df) >= 250:
        ma120 = df['close'].rolling(120).mean().values[-1]
        if abs(close[-1] - ma120) / ma120 < 0.02 and close[-1] > ma120:
            if vol[-1] < vol[-5:-1].mean(): # 缩量企稳
                return "长线均线支撑"

    # --- 策略四：上升趋势中均线不破 ---
    # 逻辑：MA10/MA20多头，缩量回踩MA20不破
    if ma10[-1] > ma20[-1] and ma20[-1] > ma20[-5]:
        if low[-1] <= ma20[-1] * 1.01 and close[-1] >= ma20[-1]:
            if vol[-1] < vol[-5:-1].mean():
                return "上升通道回踩"

    return None

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty: return None
        
        signal = check_yangjia_logic(df)
        if signal:
            return {'code': code, 'signal': signal}
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
    
    found = [r for r in results if r is not None]
    
    # 输出结果
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    if found:
        res_df = pd.DataFrame(found)
        final_df = pd.merge(res_df, names_df, on='code')
        file_path = os.path.join(month_dir, f'yangjia_low_buy_{ts}.csv')
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成：匹配到 {len(final_df)} 只符合低吸条件的个股。")

if __name__ == '__main__':
    main()
