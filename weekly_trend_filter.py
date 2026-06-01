import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# 配置参数
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results'

# 字段映射字典 (CSV中文转程序变量)
COL_MAP = {
    '日期': 'date',
    '开盘': 'open',
    '最高': 'high',
    '最低': 'low',
    '收盘': 'close',
    '成交量': 'volume'
}

def calculate_macd(prices, fast=12, slow=26, signal=9):
    """计算 MACD 指标"""
    exp1 = prices.ewm(span=fast, adjust=False).mean()
    exp2 = prices.ewm(span=slow, adjust=False).mean()
    dif = exp1 - exp2
    dea = dif.ewm(span=signal, adjust=False).mean()
    return dif, dea

def check_strategy(df):
    """
    实现图片中的筛选逻辑：
    1. MACD 水上：DIF > 0 且 DEA > 0
    2. 主升浪形态：DIF > DEA (多头排列)
    3. 放量：最新成交量 > 过去5周期均量的 1.3 倍
    """
    if len(df) < 35: return False
    
    dif, dea = calculate_macd(df['close'])
    
    last_dif = dif.iloc[-1]
    last_dea = dea.iloc[-1]
    last_vol = df['volume'].iloc[-1]
    avg_vol = df['volume'].iloc[-6:-1].mean() # 过去5个周期的均量
    
    # 逻辑判断
    is_water_up = (last_dif > 0) and (last_dea > 0)
    is_gold_cross = last_dif > last_dea
    vol_breakout = last_vol > (avg_vol * 1.3)
    
    return is_water_up and is_gold_cross and vol_breakout

def process_stock(file_name):
    code = file_name.split('.')[0]
    # 排除 30 开头的创业板
    if code.startswith('30'): return None
    
    try:
        file_path = os.path.join(DATA_DIR, file_name)
        # 读取CSV并重命名表头以便处理
        df = pd.read_csv(file_path)
        df = df.rename(columns=COL_MAP)
        
        if df.empty or len(df) < 30: return None
        
        # 基础过滤：价格 5.0 - 20.0 元
        last_price = df['close'].iloc[-1]
        if not (5.0 <= last_price <= 20.0): return None
        
        # --- 日线筛选 ---
        is_daily_hit = check_strategy(df)
        
        # --- 周线筛选 ---
        df['date'] = pd.to_datetime(df['date'])
        df_weekly = df.resample('W', on='date').agg({
            'open':'first', 
            'high':'max', 
            'low':'min', 
            'close':'last', 
            'volume':'sum'
        }).dropna()
        is_weekly_hit = check_strategy(df_weekly)
        
        return {'code': code, 'daily': is_daily_hit, 'weekly': is_weekly_hit}
    except:
        return None

def main():
    # 1. 加载名称并排除 ST
    if not os.path.exists(NAMES_FILE): return
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    names_df = names_df[~names_df['name'].str.contains('ST|st')]
    valid_codes = set(names_df['code'])

    # 2. 扫描数据目录并并行执行
    if not os.path.exists(DATA_DIR): return
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f.split('.')[0] in valid_codes]
    
    daily_list, weekly_list = [], []
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_stock, files))
    
    for res in results:
        if res:
            if res['daily']: daily_list.append(res['code'])
            if res['weekly']: weekly_list.append(res['code'])

    # 3. 输出与保存
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    for label, codes in [('daily', daily_list), ('weekly', weekly_list)]:
        out_df = names_df[names_df['code'].isin(codes)]
        file_name = f"{label}_pick_{ts}.csv"
        out_df.to_csv(os.path.join(month_dir, file_name), index=False, encoding='utf-8-sig')
        print(f"{label} 筛选完成，匹配到 {len(out_df)} 只股票")

if __name__ == '__main__':
    main()
