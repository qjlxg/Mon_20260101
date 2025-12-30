import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# --- 路径配置 ---
DATA_DIR = 'stock_data'
OUTPUT_DIR = 'results/yin_line_strategy'
NAMES_FILE = 'stock_names.csv'

def get_indicators(df):
    df = df.copy()
    # 基础均线
    for m in [5, 10, 20, 60]:
        df[f'ma{m}'] = df['close'].rolling(m).mean()
    # 均线斜率（用于判断方向是否向上）
    df['ma5_up'] = df['ma5'] > df['ma5'].shift(1)
    df['ma10_up'] = df['ma10'] > df['ma10'].shift(1)
    df['ma60_up'] = df['ma60'] > df['ma60'].shift(1)
    # 成交量指标：前5日平均成交量
    df['v_ma5_avg'] = df['volume'].shift(1).rolling(5).mean()
    return df

def check_yin_logic(df):
    """
    完全匹配图片战法逻辑：
    1. 趋势：MA60向上且收盘在MA60上
    2. 避坑：成交额 > 1亿
    """
    if len(df) < 60: return None
    
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    # --- 原则一：趋势为王 (核心过滤) ---
    if not (curr['close'] > curr['ma60'] and curr['ma60_up']):
        return None

    # --- 原则二：避坑指南 (成交额 > 1亿) ---
    # 注意：CSV中成交额单位通常是元，1亿 = 100,000,000
    if curr['amount'] < 100000000:
        return None

    is_yin = curr['close'] < curr['open']
    signals = []

    # --- 战法形态 1：缩量回调阴线 ---
    # 图片要点：股价在5/10日线上 + 均线向上 + 缩量至5日均量50%以下
    if is_yin and curr['close'] > curr['ma5'] and curr['close'] > curr['ma10']:
        if curr['ma5_up'] and curr['ma10_up']:
            if curr['volume'] < (curr['v_ma5_avg'] * 0.5):
                signals.append("缩量回调阴线")

    # --- 战法形态 2：回踩均线阴线 ---
    # 图片要点：踩而不破 + 均线向上
    if is_yin and not signals: # 避免重复统计
        for m in [5, 10, 20]:
            if curr[f'ma{m}_up']: # 均线必须向上
                if curr['low'] <= curr[f'ma{m}'] and curr['close'] >= curr[f'ma{m}']:
                    signals.append(f"回踩MA{m}阴线")
                    break

    # --- 战法形态 3：放量假阴线 (假阴真阳) ---
    # 图片要点：收盘 > 前收 + 高开 + 放量1.5倍以上
    if is_yin and curr['close'] > prev['close'] and curr['open'] > prev['close']:
        if curr['volume'] > (prev['volume'] * 1.5):
            # 图片细节：收盘接近最高价，上影线短
            if (curr['high'] - curr['close']) / curr['close'] < 0.01:
                signals.append("放量假阴线")

    return "+".join(signals) if signals else None

def run_task():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 加载名称映射
    name_map = {}
    if os.path.exists(NAMES_FILE):
        name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        name_map = dict(zip(name_df['code'], name_df['name']))

    files = glob.glob(f"{DATA_DIR}/*.csv")
    date_str = datetime.now().strftime('%Y-%m-%d')
    results = []

    for f in files:
        try:
            df = pd.read_csv(f)
            # 统一列名处理
            df = df.rename(columns={
                '日期': 'date', '收盘': 'close', '开盘': 'open', 
                '成交量': 'volume', '成交额': 'amount', '最高': 'high', '最低': 'low'
            })
            
            df = get_indicators(df)
            match = check_yin_logic(df)
            
            if match:
                code = os.path.basename(f).replace('.csv', '')
                results.append({
                    '日期': date_str,
                    '代码': code,
                    '名称': name_map.get(code, '未知'),
                    '当前价': round(df['close'].iloc[-1], 2),
                    '形态类型': match,
                    '成交额(亿)': round(df['amount'].iloc[-1] / 100000000, 2)
                })
        except Exception as e:
            continue

    if results:
        res_df = pd.DataFrame(results)
        save_path = f"{OUTPUT_DIR}/yin_signals_{date_str}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"✅ 扫描完成，共发现 {len(res_df)} 个符合图片逻辑的目标。")
    else:
        print("今日无符合阴线买入战法的目标。")

if __name__ == "__main__":
    run_task()
