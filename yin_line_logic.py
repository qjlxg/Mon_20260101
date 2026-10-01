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
    # 核心均线系统
    for m in [5, 10, 20, 60]:
        df[f'ma{m}'] = df['收盘'].rolling(m).mean()
    
    # 趋势指标
    df['ma10_up'] = df['ma10'] > df['ma10'].shift(1)
    df['v_ma5'] = df['成交量'].rolling(5).mean()
    df['change'] = df['收盘'].pct_change() * 100
    return df

def check_logic(df):
    if len(df) < 60: return None, None
    curr = df.iloc[-1]
    
    # 1. 价格过滤 (5-20元) 与 成交额过滤 ( > 3亿)
    if not (5.0 <= curr['收盘'] <= 20.0) or curr['成交额'] < 300000000:
        return None, None

    # 2. 强势基因判定 (15天内有过涨停或9.5%以上大阳)
    recent_15 = df.tail(15)
    if not (recent_15['change'] > 9.5).any():
        return None, None

    # 3. 线上形态判断 (阴线 或 微跌收盘)
    is_yin = curr['收盘'] < curr['开盘'] or curr['change'] <= 0
    
    # 4. 寻找支撑位与偏离度计算
    # 核心原则：阴线位置越靠近均线越好。逻辑：先看MA10，再看MA5
    support_ma_key = None
    if curr['最低'] <= curr['ma10'] * 1.01 and curr['收盘'] >= curr['ma10'] * 0.985:
        support_ma_key = 'ma10'
    elif curr['最低'] <= curr['ma5'] * 1.01 and curr['收盘'] >= curr['ma5'] * 0.985:
        support_ma_key = 'ma5'
    
    if not support_ma_key: return None, None

    # 5. 腾空动力判定：近期必须有过远离5日线 > 7% 的爆发阶段
    has_jumped = (df['最高'].tail(10) > df['ma5'].tail(10) * 1.07).any()
    
    # 6. 精准缩量判断 (成交量 < 5日均量的80%，体现洗盘)
    is_shrink = curr['成交量'] < (df['v_ma5'].iloc[-1] * 0.8)

    if is_yin and has_jumped and is_shrink and curr['ma10_up']:
        return f"回踩{support_ma_key.upper()}阴线", support_ma_key
    
    return None, None

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    
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
            match_type, ma_key = check_logic(df)
            
            if match_type:
                code = os.path.basename(f).replace('.csv', '')
                curr_p = df['收盘'].iloc[-1]
                ma_val = df[ma_key].iloc[-1]
                # 计算并四舍五入偏离度
                bias = round((curr_p - ma_val) / ma_val * 100, 2)
                
                results.append({
                    '日期': date_str,
                    '代码': code,
                    '名称': name_map.get(code, '未知'),
                    '当前价': round(curr_p, 2),
                    '形态类型': match_type,
                    '偏离度%': bias,
                    '成交额(亿)': round(df['成交额'].iloc[-1] / 100000000, 2)
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results)
        # 核心逻辑：按偏离度的绝对值升序排。离均线0%最近的排在最前面
        res_df['abs_bias'] = res_df['偏离度%'].abs()
        res_df = res_df.sort_values(by='abs_bias').drop(columns=['abs_bias'])
        
        # 仅保留偏离度在 [-1.5%, 1.5%] 之间的个股，进一步精减结果
        res_df = res_df[res_df['偏离度%'].between(-1.5, 1.5)]
        
        save_path = f"{OUTPUT_DIR}/yin_signals_{date_str}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"🎯 扫描完成：精选出 {len(res_df)} 个“贴线”目标。")
    else:
        print("今日未发现符合严苛条件的“贴线”信号。")

if __name__ == "__main__":
    main()
