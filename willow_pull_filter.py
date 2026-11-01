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

def is_willow_pull(df):
    """
    "倒拔垂杨柳" 战法逻辑实现:
    1. 上升趋势：股价位于 10日、20日均线上方。
    2. 大阴线形态：高开低走，实体较长，且是一根放量阴线。
    3. 假阴真强：虽是阴线，但收盘价最好不跌破昨日收盘价太多（假阴线）或回踩不破重要均线。
    4. 巨量承接：当日成交量 > 过去5日均量的 1.8 倍。
    """
    if len(df) < 20: return False
    
    # 获取最近两日数据
    today = df.iloc[-1]
    yesterday = df.iloc[-2]
    
    # 计算均线
    ma10 = df['close'].rolling(window=10).mean().iloc[-1]
    
    # 逻辑判定
    # 1. 趋势：收盘价在10日均线之上
    trend_ok = today['close'] > ma10
    
    # 2. 形态：高开低走的大阴线 (开盘 > 收盘)
    is_black_candle = today['open'] > today['close']
    # 阴线实体长度 (开盘到收盘跌幅超过 2%)
    candle_entity = (today['open'] - today['close']) / yesterday['close'] > 0.02
    
    # 3. 巨量：放量 1.8 倍以上 (体现"倒拔"的力量)
    avg_vol_5 = df['volume'].iloc[-6:-1].mean()
    vol_breakout = today['volume'] > (avg_vol_5 * 1.8)
    
    # 4. 强力承接 (假阴线逻辑)：
    # 即使是阴线，收盘价不低于昨日收盘价的 -2% (防止真杀跌)
    support_ok = today['pct_chg'] > -2.0
    
    # 5. 价格区间过滤 (5-20元)
    price_ok = 5.0 <= today['close'] <= 20.0

    return all([trend_ok, is_black_candle, candle_entity, vol_breakout, support_ok, price_ok])

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None # 排除创业板
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty or len(df) < 20: return None
        
        # 转换百分比列为数值
        if isinstance(df['pct_chg'].iloc[-1], str):
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        # 判定形态
        if is_willow_pull(df):
            return code
    except:
        return None
    return None

def main():
    # 1. 加载股票名称
    if not os.path.exists(NAMES_FILE): return
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    names_df = names_df[~names_df['name'].str.contains('ST|st')]
    valid_codes = set(names_df['code'])

    # 2. 扫描文件并并行处理
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f.split('.')[0] in valid_codes]
    
    with ProcessPoolExecutor() as executor:
        hit_codes = list(executor.map(process_stock, files))
    
    results = [c for c in hit_codes if c is not None]

    # 3. 输出结果
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    final_df = names_df[names_df['code'].isin(results)]
    file_path = os.path.join(month_dir, f'willow_pull_{ts}.csv')
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"筛选完成：发现 {len(final_df)} 只符合'倒拔垂杨柳'形态的个股。")

if __name__ == '__main__':
    main()
