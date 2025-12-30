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

def is_three_yin_one_yang(df):
    """
    战法一：【三阴生阳】
    1. 前置条件：三根阴线前有一根涨幅 > 7% 的大阳线（最好是涨停）。
    2. 洗盘特征：连续 3 根阴线，且这 3 天成交量持续缩减（缩量洗盘）。
    3. 趋势要求：处于上升趋势或横盘回踩支撑位（收盘价不破大阳线开盘价）。
    """
    if len(df) < 10: return False
    
    # 检查最近 4 天：1阳 + 3阴
    last_4 = df.tail(4).copy()
    c = last_4['close'].values
    o = last_4['open'].values
    v = last_4['volume'].values
    p = last_4['pct_chg'].values
    
    # 条件1：前第4天是大阳线 (涨幅 > 7%)
    if p[0] < 7.0: return False
    
    # 条件2：后面3天是阴线 (收盘 < 开盘)
    if not all(c[i] < o[i] for i in range(1, 4)): return False
    
    # 条件3：缩量 (第2、3、4天量能低于第1天大阳线量能，且整体呈萎缩趋势)
    if not (v[1] < v[0] and v[3] < v[1]): return False
    
    # 条件4：空间支撑 (第4天收盘价不跌破第1天大阳线的开盘价)
    if c[3] < o[0]: return False
    
    return True

def is_double_cannon(df):
    """
    战法二：【涨停双响炮】
    1. K线组合：两根大阳线（涨幅>7%）中间夹着小K线。
    2. 洗盘间隔：两根大阳线中间的小K线洗盘天数 2-7 天。
    3. 均线配合：5日线与20日线在洗盘期间保持向上或形成金叉。
    4. 突破：最新一根阳线放量并反包中间的小K线。
    """
    if len(df) < 15: return False
    
    # 检查最后一根是否是大阳线
    today = df.iloc[-1]
    if today['pct_chg'] < 7.0: return False
    
    # 向前寻找前一根大阳线（间隔2到7天）
    found_first_cannon = False
    for i in range(2, 9):
        prev_idx = -i
        prev_k = df.iloc[prev_idx]
        
        if prev_k['pct_chg'] > 7.0:
            # 找到前炮，检查中间的洗盘区
            wash_zone = df.iloc[prev_idx + 1 : -1]
            # 实体不能超过阳线两端（简化为收盘价在第一根阳线范围内）
            if wash_zone['close'].max() <= prev_k['high'] and wash_zone['close'].min() >= prev_k['low']:
                # 检查均线趋势 (5日线 > 20日线 或 正在向上)
                ma5 = df['close'].rolling(5).mean()
                ma20 = df['close'].rolling(20).mean()
                if ma5.iloc[-1] >= ma20.iloc[-1]:
                    found_first_cannon = True
                    break
    
    return found_first_cannon

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith('30'): return None
    
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df.empty or len(df) < 20: return None
        
        # 统一处理涨跌幅格式
        if df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        # 基础过滤：价格 5-20 元
        last_close = df['close'].iloc[-1]
        if not (5.0 <= last_close <= 20.0): return None

        res_three_yin = is_three_yin_one_yang(df)
        res_double_cannon = is_double_cannon(df)
        
        if res_three_yin or res_double_cannon:
            return {
                'code': code, 
                'type': '三阴生阳' if res_three_yin else '涨停双响炮'
            }
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
        task_results = list(executor.map(process_stock, files))
    
    # 结果分类
    three_yin_list = [r['code'] for r in task_results if r and r['type'] == '三阴生阳']
    cannon_list = [r['code'] for r in task_results if r and r['type'] == '涨停双响炮']

    # 输出保存
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_dir, exist_ok=True)
    ts = now.strftime('%Y%m%d_%H%M%S')

    for label, codes, title in [('three_yin', three_yin_list, '三阴生阳'), ('double_cannon', cannon_list, '涨停双响炮')]:
        out_df = names_df[names_df['code'].isin(codes)]
        file_path = os.path.join(month_dir, f'{label}_{ts}.csv')
        out_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"{title} 战法匹配到: {len(out_df)} 只")

if __name__ == '__main__':
    main()
