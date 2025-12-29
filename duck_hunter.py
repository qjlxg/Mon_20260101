import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count
import re

# 配置常量
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results'

def analyze_logic(file_path):
    try:
        # 1. 加载数据
        df = pd.read_csv(file_path)
        if len(df) < 120: return None # 必须有足够数据计算MA60/120和MACD
        
        # 自动映射中文表头
        df = df.rename(columns={
            '日期':'date', '股票代码':'code', '开盘':'open', 
            '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'
        })
        
        # 格式化代码
        code_raw = str(df.iloc[-1]['code']).split('.')[0]
        code = code_raw.zfill(6)
        
        # 排除板块：只要沪深A股 (60, 00开头)
        if not (code.startswith('60') or code.startswith('00')):
            return None

        # 2. 基础过滤：价格 5.0 - 20.0 元
        curr = df.iloc[-1]
        if not (5.0 <= curr['close'] <= 20.0):
            return None

        # 3. 计算技术指标
        # 均线系统
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma10'] = df['close'].rolling(10).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['ma60'] = df['close'].rolling(60).mean()
        
        # 成交量均线
        df['vol_ma5'] = df['volume'].rolling(5).mean()
        df['vol_ma60'] = df['volume'].rolling(60).mean()
        
        # MACD (12, 26, 9)
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = exp1 - exp2
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['dif'] - df['dea']) * 2

        # 核心观察区间
        prev = df.iloc[-2]
        recent_10 = df.iloc[-10:-1] # 过去两周
        recent_20 = df.iloc[-20:-1] # 过去一个月

        # --- 网络实战精华：老鸭头变异版筛选逻辑 ---
        
        # 【A. 鸭脖子与趋势】60日均线必须向上，且当前股价在MA60之上 (生命线原则)
        cond_trend = curr['ma60'] > df.iloc[-5]['ma60'] and curr['close'] > curr['ma60']
        
        # 【B. 鸭头高度】过去20天内最高价曾站上过MA60的15%以上 (确保前期有强势拉升)
        cond_head = recent_20['high'].max() > curr['ma60'] * 1.15
        
        # 【C. 鸭鼻孔-地量洗盘】在回调过程中，近期成交量出现过极度萎缩 (地量 < 60日均量的0.7倍)
        cond_nostril = recent_10['volume'].min() < curr['vol_ma60'] * 0.7
        
        # 【D. 鸭嘴启动-均线逻辑】
        # 1. 股价在MA5和MA10之上
        # 2. MA5 向上 (ma5 > prev_ma5)
        cond_ma_logic = curr['close'] > curr['ma5'] and curr['close'] > curr['ma10'] and curr['ma5'] > prev['ma5']
        
        # 【E. 动量逻辑-水上金叉】MACD的DIF必须在零轴之上，且今日红柱变长或金叉
        cond_macd = curr['dif'] > 0 and curr['macd'] > prev['macd']
        
        # 【F. 量价配合】今日放量 (量比>1.2) 或 属于强势回踩支撑企稳 (缩量小阳)
        cond_vol = curr['volume'] > curr['vol_ma5'] * 1.2 or (curr['close'] > curr['open'] and curr['volume'] > prev['volume'])

        # 综合判定
        if cond_trend and cond_head and cond_nostril and cond_ma_logic and cond_macd and cond_vol:
            return {'code': code, 'price': round(curr['close'], 2)}
            
    except Exception:
        return None
    return None

def main():
    if not os.path.exists(NAMES_FILE):
        print(f"Error: {NAMES_FILE} not found.")
        return
    
    # 排除ST及退市 (修正正则)
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    names_df = names_df[~names_df['name'].str.contains(r'ST|退|\*ST', na=False)]
    valid_codes = set(names_df['code'].apply(lambda x: x.zfill(6)).tolist())

    # 并行扫描 CSV
    files = [f for f in glob.glob(f'{DATA_DIR}/*.csv') if os.path.basename(f).split('.')[0].zfill(6) in valid_codes]
    
    if not files:
        print("No stock data files found.")
        return

    with Pool(cpu_count()) as p:
        results = p.map(analyze_logic, files)
    
    results = [r for r in results if r is not None]
    
    if results:
        res_df = pd.DataFrame(results)
        final_df = pd.merge(res_df, names_df, on='code', how='left')
        
        now = datetime.now()
        dir_path = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
        os.makedirs(dir_path, exist_ok=True)
        
        save_path = os.path.join(dir_path, f"duck_hunter_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        final_df[['code', 'name', 'price']].to_csv(save_path, index=False)
        print(f"Found {len(final_df)} potential 'Old Duck Head' stocks.")
    else:
        print("No matches today.")

if __name__ == "__main__":
    main()
