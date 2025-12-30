import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# --- 配置区 (路径保持不变) ---
DATA_DIR = 'stock_data'
OUTPUT_DIR = 'results/online_yin_final'
NAMES_FILE = 'stock_names.csv'

def get_indicators(df):
    df = df.copy()
    # 核心均线系统
    for m in [5, 10, 20, 60]:
        df[f'ma{m}'] = df['收盘'].rolling(m).mean()
    
    # 趋势判定：MA10向上且股价在MA60上
    df['ma10_up'] = df['ma10'] > df['ma10'].shift(1)
    df['ma60_up'] = df['ma60'] > df['ma60'].shift(1)
    
    # 成交量：5日均量
    df['v_ma5'] = df['成交量'].rolling(5).mean()
    df['vol_avg_10'] = df['成交量'].rolling(10).mean()
    
    # 涨跌幅计算
    df['change'] = df['收盘'].pct_change() * 100
    return df

def check_logic(df):
    if len(df) < 60: return None
    curr = df.iloc[-1]
    
    # --- 条件1：价格限制 (5-20元) ---
    if not (5.0 <= curr['收盘'] <= 20.0):
        return None

    # --- 条件2：成交额限制 ( > 3亿) ---
    if curr['成交额'] < 300000000:
        return None

    # --- 条件3：强势基因 (15天内必须有涨停或9.5%+大阳) ---
    recent_15 = df.tail(15)
    if not (recent_15['change'] > 9.5).any():
        return None

    # --- 条件4：线上阴线买核心形态 ---
    is_yin = curr['收盘'] < curr['开盘'] or curr['change'] <= 0
    # 靠近10日线支撑位 (原则：靠近均线买入，允许0.5%误差)
    on_ma10 = curr['最低'] <= curr['ma10'] * 1.005 and curr['收盘'] >= curr['ma10'] * 0.99
    # 缩量判定 (突破放量，整理缩量)
    is_shrink = curr['成交量'] < df['v_ma5'].iloc[-1]
    
    # --- 最终判定 ---
    if is_yin and on_ma10 and is_shrink and curr['ma10_up'] and curr['收盘'] > curr['ma60']:
        # 补充：3倍量卖出预警 (原则2)
        if curr['成交量'] > curr['vol_avg_10'] * 3:
            return "3倍量卖出预警"
        return "线上阴线买(精选)"
    
    # 补充：强弩之末逻辑 (极端大跌后的转势)
    if (df['change'].shift(1) < -5).tail(3).all() and curr['change'] < -5:
        return "强弩之末(博反弹)"

    return None

def main():
    if not os.path.exists(OUTPUT_DIR): 
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    
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
            match = check_logic(df)
            
            if match:
                code = os.path.basename(f).replace('.csv', '')
                curr_p = df['收盘'].iloc[-1]
                ma10_p = df['ma10'].iloc[-1]
                results.append({
                    '代码': code,
                    '名称': name_map.get(code, '未知'),
                    '当前价': round(curr_p, 2),
                    '10日线支撑': round(ma10_p, 2),
                    '偏离度%': round((curr_p - ma10_p) / ma10_p * 100, 2),
                    '成交额(亿)': round(df['成交额'].iloc[-1] / 100000000, 2),
                    '战法形态': match
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results)
        # 按偏离度绝对值升序排，把最靠近支撑位的放最上面
        res_df['abs_bias'] = res_df['偏离度%'].abs()
        res_df = res_df.sort_values(by='abs_bias').drop(columns=['abs_bias'])
        res_df.to_csv(f"{OUTPUT_DIR}/yin_signals_{date_str}.csv", index=False, encoding='utf-8-sig')
        print(f"🎯 扫描完成：精选出 {len(results)} 个高价值目标")
    else:
        print("今日未发现符合严苛条件的信号")

if __name__ == "__main__":
    main()
