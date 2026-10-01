import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results/macd_water'

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 100: return None
        df = df.rename(columns={'日期':'date','股票代码':'code','收盘':'close','涨跌幅':'pct_chg'})
        
        # MACD计算
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = exp1 - exp2
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['dif'] - df['dea']) * 2
        
        curr, prev = df.iloc[-1], df.iloc[-2]
        ma20 = df['close'].rolling(20).mean().iloc[-1]

        # 核心：DIF>0 且 (金叉 或 红柱连续增长)
        cond_water = curr['dif'] > 0 and curr['dea'] > 0
        cond_cross = (prev['macd'] <= 0 and curr['macd'] > 0) or (curr['macd'] > prev['macd'] > 0)
        
        if cond_water and cond_cross and curr['close'] > ma20:
            return {
                'date': curr['date'],
                'code': str(curr['code']).split('.')[0].zfill(6),
                'price': curr['close'],
                'dif': round(curr['dif'], 3)
            }
    except: return None

def main():
    if not os.path.exists(NAMES_FILE): return
    os.makedirs(OUTPUT_BASE, exist_ok=True)
    files = glob.glob(f'{DATA_DIR}/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(analyze_logic, files) if r is not None]
    if results:
        res_df = pd.DataFrame(results)
        names = pd.read_csv(NAMES_FILE, dtype={'code': str})
        names['code'] = names['code'].apply(lambda x: x.zfill(6))
        res_df = pd.merge(res_df, names, on='code', how='left')
        save_path = f"{OUTPUT_BASE}/macd_water_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(save_path, index=False)
        print(f"水上金叉发现: {len(res_df)} 只")

if __name__ == "__main__":
    main()
