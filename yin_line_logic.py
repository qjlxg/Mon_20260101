import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# --- 配置区 ---
DATA_DIR = 'stock_data'
OUTPUT_DIR = 'results/online_yin_final'
NAMES_FILE = 'stock_names.csv'

def get_indicators(df):
    df = df.copy()
    # 1. 基础均线系统 (5, 10, 20, 30, 60)
    for m in [5, 10, 20, 30, 60]:
        df[f'ma{m}'] = df['收盘'].rolling(m).mean()
    
    # 2. 均线斜率与粘合度
    df['ma10_up'] = df['ma10'] > df['ma10'].shift(1)
    df['ma60_up'] = df['ma60'] > df['ma60'].shift(1)
    # 粘合度：5, 10, 20日线间距标准差
    df['ma_std'] = df[['ma5', 'ma10', 'ma20']].std(axis=1) / df['ma10']
    
    # 3. 成交量指标
    df['vol_avg_10'] = df['成交量'].rolling(10).mean() # 近10日均量
    df['v_ma5'] = df['成交量'].rolling(5).mean()      # 近5日均量
    
    # 4. 价格波动
    df['change'] = df['收盘'].pct_change() * 100
    return df

def check_final_logic(df):
    if len(df) < 60: return None
    curr = df.iloc[-1]
    prev = df.iloc[-2]
    
    # --- A. 识别强势股基因 (原则1: 市场热点/强势启动) ---
    recent_15 = df.tail(15)
    # 包含：大阳线(>7%)、跳空缺口、连续涨停(此处简略为两天涨幅大)
    has_big_yang = (recent_15['change'] > 7).any()
    has_gap = (recent_15['最低'] > recent_15['最高'].shift(1)).any()
    is_fanning = curr['ma5'] > curr['ma10'] > curr['ma20'] and prev['ma_std'] < 0.03 # 粘合后发散
    
    # 基础门槛：必须是强势基因股且处于60日线上方
    is_strong_stock = (has_big_yang or has_gap or is_fanning) and curr['收盘'] > curr['ma60']
    
    # --- B. 避坑指南: 成交额 > 1亿 ---
    if curr['成交额'] < 100000000: return None

    signals = []
    is_yin = curr['收盘'] < curr['开盘'] or curr['change'] < 0
    
    # --- C. 核心战法: 线上阴线买 (10日线附近) ---
    # 1. 回踩10日线支撑 (允许1%误差，原则1&3)
    # 股价迅速腾空脱离5日线(偏离>5%)后回踩
    has_jumped = (df['最高'].tail(8) > df['ma5'].tail(8) * 1.05).any()
    on_ma10 = curr['最低'] <= curr['ma10'] * 1.01 and curr['收盘'] >= curr['ma10'] * 0.98
    
    # 2. 缩量判定 (成交量 < 10日均量的1.2倍 且 < 5日均量)
    is_shrink = curr['成交量'] < curr['vol_avg_10'] * 1.2 and curr['成交量'] < curr['v_ma5']

    if is_strong_stock and has_jumped and on_ma10 and is_yin and is_shrink and curr['ma10_up']:
        signals.append("线上阴线买(10日线支撑)")

    # --- D. 极端条件: 强弩之末 (连续大跌后的转势) ---
    # 连续3根以上大阴线，且远离均线，第四根又是大阴线但动能衰竭
    is_extreme_drop = (df['change'].shift(1) < -4).tail(3).all() and curr['change'] < -5
    if is_extreme_drop:
        signals.append("强弩之末(博反弹)")

    # --- E. 卖出预警: 3倍量抛出 (原则2) ---
    if curr['成交量'] > curr['vol_avg_10'] * 3:
        signals.append("3倍量卖出预警")

    return "+".join(signals) if signals else None

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 加载名称映射
    name_map = {}
    if os.path.exists(NAMES_FILE):
        try:
            n_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
            name_map = dict(zip(n_df['code'], n_df['name']))
        except: pass

    files = glob.glob(f"{DATA_DIR}/*.csv")
    date_str = datetime.now().strftime('%Y-%m-%d')
    results = []

    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip() for c in df.columns]
            df = get_indicators(df)
            match = check_final_logic(df)
            
            if match:
                code = os.path.basename(f).replace('.csv', '')
                curr_p = round(df['收盘'].iloc[-1], 2)
                ma10_p = round(df['ma10'].iloc[-1], 2)
                results.append({
                    '代码': code,
                    '名称': name_map.get(code, '未知'),
                    '当前价': curr_p,
                    '10日线': ma10_p,
                    '信号': match,
                    '偏离度%': round((curr_p - ma10_p) / ma10_p * 100, 2),
                    '成交额(亿)': round(df['成交额'].iloc[-1] / 100000000, 2)
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results).sort_values(by='偏离度%')
        res_df.to_csv(f"{OUTPUT_DIR}/final_yin_{date_str}.csv", index=False, encoding='utf-8-sig')
        print(f"🎯 战法扫描完成：发现 {len(results)} 个高价值目标")
    else:
        print("今日未发现符合所有条件的强力信号")

if __name__ == "__main__":
    main()
