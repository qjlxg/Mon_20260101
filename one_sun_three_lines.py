import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results/one_sun'

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        df = df.rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','涨跌幅':'pct_chg','换手率':'turnover'})
        
        # 基础过滤：排除次新、价格区间
        if len(df) < 180: return None
        curr = df.iloc[-1]
        if not (5.0 <= curr['close'] <= 35.0): return None
        if curr['pct_chg'] < 5.0: return None # 必须是大阳线
        
        # 计算均线
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        ma10 = df['close'].rolling(10).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        vol_ma5 = df['volume'].rolling(5).mean().iloc[-2]

        # 核心：收盘在三线上，开盘在三线下，且量比翻倍
        cond_pierce = curr['close'] > max(ma5, ma10, ma20) and curr['open'] < min(ma5, ma10, ma20)
        cond_vol = curr['volume'] > vol_ma5 * 1.8
        
        if cond_pierce and cond_vol and curr['turnover'] > 3.0:
            return {
                'date': curr['date'],
                'code': str(curr['code']).split('.')[0].zfill(6),
                'price': curr['close'],
                'pct_chg': f"{curr['pct_chg']}%",
                'turnover': f"{curr['turnover']}%"
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
        save_path = f"{OUTPUT_BASE}/one_sun_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(save_path, index=False)
        print(f"一阳穿三线发现: {len(res_df)} 只")

if __name__ == "__main__":
    main()
