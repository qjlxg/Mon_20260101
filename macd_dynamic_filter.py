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

def calculate_macd_ext(df, fast=12, slow=26, signal=9):
    """计算MACD及柱状线"""
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    dif = exp1 - exp2
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = 2 * (dif - dea)
    return dif, dea, hist

def check_macd_logic(df):
    """
    战法名称：MACD 核心战法——买在小绿柱，卖在小红柱
    核心逻辑：
    1. 寻找买点：下跌末期，绿柱（Negative Hist）持续缩短 3-5 日，且贴近零轴。
    2. 辅助确认：股价未创新低（底背离趋势），且当前价格在 5-20 元区间。
    """
    if len(df) < 40: return False, False
    
    _, _, hist = calculate_macd_ext(df)
    
    # 获取最近 5 日柱状线
    recent_hist = hist.tail(5).values
    last_price = df['close'].iloc[-1]
    
    # 基础价格过滤
    if not (5.0 <= last_price <= 20.0):
        return False, False

    # --- 场景一：买入信号（下跌末期：小绿柱+持续缩短） ---
    # 条件：全是绿柱 ( < 0 )，且最近 3 日绝对值持续减小
    is_buy = False
    if all(h < 0 for h in recent_hist[-3:]):
        # 绿柱缩短：今天的负值比昨天大（即更接近0），昨天的比前天大
        if recent_hist[-1] > recent_hist[-2] > recent_hist[-3]:
            # 贴近零轴判断：绝对值小于某个阈值（例如过去 20 日最大高度的 10%）
            limit = np.abs(hist.tail(20)).max() * 0.2
            if abs(recent_hist[-1]) < limit:
                is_buy = True

    # --- 场景四：卖出预警（上涨末期：小红柱+持续缩短） ---
    # 条件：全是红柱 ( > 0 )，且最近 3 日高度持续下降
    is_sell = False
    if all(h > 0 for h in recent_hist[-3:]):
        if recent_hist[-1] < recent_hist[-2] < recent_hist[-3]:
            is_sell = True
            
    return is_buy, is_sell

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty or len(df) < 30: return None
        
        is_buy, is_sell = check_macd_logic(df)
        
        if is_buy or is_sell:
            return {'code': code, 'type': 'BUY' if is_buy else 'SELL'}
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
    
    # 分类结果
    buy_list = [r['code'] for r in results if r and r['type'] == 'BUY']
    sell_list = [r['code'] for r in results if r and r['type'] == 'SELL']

    # 保存
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    for label, codes in [('macd_buy_signal', buy_list), ('macd_sell_alert', sell_list)]:
        out_df = names_df[names_df['code'].isin(codes)]
        out_df.to_csv(os.path.join(month_dir, f'{label}_{ts}.csv'), index=False, encoding='utf-8-sig')
        print(f"{label} 匹配到 {len(out_df)} 只")

if __name__ == '__main__':
    main()
