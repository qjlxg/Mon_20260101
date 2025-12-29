import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- 配置 ---
DATA_DIR = 'stock_data'
OUTPUT_DIR = 'results/yin_line_strategy'

class YinLineStrategy:
    """严格执行图片逻辑的阴线买入战法"""
    
    @staticmethod
    def prepare_indicators(df):
        df = df.copy()
        for m in [5, 10, 20, 60]:
            df[f'ma{m}'] = df['close'].rolling(m).mean()
        # 5日平均成交量 (用于缩量判断)
        df['v_ma5_avg'] = df['volume'].shift(1).rolling(5).mean()
        return df

    @staticmethod
    def check_rules(df):
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 基础准则：趋势为王 (股价在60日线上，且60日线向上)
        if not (curr['close'] > curr['ma60'] and curr['ma60'] > prev['ma60']):
            return None

        # 避坑指南：日成交额 > 1亿
        if (curr['close'] * curr['volume']) < 100000000:
            return None

        is_yin = curr['close'] < curr['open']
        signals = []

        # 1. 缩量回调阴线
        # 条件：股价在5/10日线上，且成交量 < 前5日均量的50%
        if is_yin and curr['close'] > curr['ma5'] and curr['close'] > curr['ma10']:
            if curr['volume'] < (curr['v_ma5_avg'] * 0.5):
                signals.append("缩量回调")

        # 2. 回踩均线阴线
        # 条件：均线向上走，回调不破均线 (MA5/10/20均可)
        if is_yin:
            for m in [5, 10, 20]:
                if curr[f'ma{m}'] > prev[f'ma{m}']: # 均线向上
                    if curr['low'] <= curr[f'ma{m}'] and curr['close'] >= curr[f'ma{m}']:
                        signals.append(f"回踩MA{m}")
                        break

        # 3. 放量假阴线
        # 条件：开盘和收盘都比前收高，成交量放大大1.5倍以上
        if is_yin and curr['open'] > prev['close'] and curr['close'] > prev['close']:
            if curr['volume'] > (prev['volume'] * 1.5):
                # 接近当天最高价 (洗盘陷阱核心)
                if (curr['high'] - curr['close']) / curr['close'] < 0.01:
                    signals.append("放量假阴线")

        return "+".join(signals) if signals else None

def run_strategy():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    results = []
    
    # 模拟大盘环境检查 (原则三：大盘大跌不买)
    # 这里假设你可以获取指数数据，若无则跳过，此处演示逻辑
    market_crash = False 
    if market_crash: return

    for f in os.listdir(DATA_DIR):
        if not f.endswith('.csv'): continue
        try:
            df = pd.read_csv(os.path.join(DATA_DIR, f))
            if len(df) < 60: continue
            
            df = YinLineStrategy.prepare_indicators(df)
            match_type = YinLineStrategy.check_rules(df)
            
            if match_type:
                results.append({
                    '代码': f.replace('.csv', ''),
                    '形态类型': match_type,
                    '收盘价': df['close'].iloc[-1],
                    '成交额(万)': round((df['close'].iloc[-1] * df['volume'].iloc[-1])/10000, 2),
                    '日期': datetime.now().strftime('%Y-%m-%d')
                })
        except: continue

    if results:
        res_df = pd.DataFrame(results)
        res_df.to_csv(f"{OUTPUT_DIR}/yin_signals_{datetime.now().strftime('%Y-%m-%d')}.csv", index=False, encoding='utf-8-sig')
        print(f"🔥 发现 {len(res_df)} 个符合图片战法的目标")

if __name__ == "__main__":
    run_strategy()
