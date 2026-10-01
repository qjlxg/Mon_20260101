import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count
import re

# 配置常量
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results' # 基础目录保持不变

def analyze_logic(file_path):
    try:
        # 1. 加载数据
        df = pd.read_csv(file_path)
        
        # 【过滤1：排除次新股】要求上市时间超过180个交易日
        if len(df) < 180: return None
        
        # 表头映射
        df = df.rename(columns={
            '日期':'date', '股票代码':'code', '开盘':'open', 
            '收盘':'close', '最高':'high', '最低':'low', 
            '成交量':'volume', '涨跌幅':'pct_chg', '换手率':'turnover'
        })
        
        # 格式化代码并过滤板块 (只要沪深A股 60/00)
        code_raw = str(df.iloc[-1]['code']).split('.')[0]
        code = code_raw.zfill(6)
        if not (code.startswith('60') or code.startswith('00')):
            return None

        # 2. 基础过滤：价格与活跃度
        curr = df.iloc[-1]
        
        # 价格区间：5 - 28元
        if not (5.0 <= curr['close'] <= 28.0): return None
        
        # 【过滤2：强势突破基因】当日涨幅 >= 3.0%
        cond_strong = curr['pct_chg'] >= 3.0
        # 【过滤3：换手活跃度】换手率 > 3.0%
        cond_active = curr['turnover'] > 3.0
        
        if not (cond_strong and cond_active):
            return None

        # 3. 技术指标计算
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma10'] = df['close'].rolling(10).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['ma60'] = df['close'].rolling(60).mean()
        df['vol_ma5'] = df['volume'].rolling(5).mean()
        df['vol_ma60'] = df['volume'].rolling(60).mean()
        
        # MACD (12, 26, 9)
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = exp1 - exp2
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['dif'] - df['dea']) * 2

        prev = df.iloc[-2]
        recent_10 = df.iloc[-10:-1]
        recent_20 = df.iloc[-20:-1]

        # --- 分级判定逻辑 ---
        
        # 【A级：基础强势】
        cond_basic_trend = curr['close'] > curr['ma5'] > curr['ma10']
        cond_basic_slope = curr['ma5'] > prev['ma5']
        cond_basic_vol = (curr['volume'] > curr['vol_ma5'] * 1.2) or (curr['close'] >= curr['ma10'] and curr['volume'] <= curr['vol_ma5'])
        
        if not (cond_basic_trend and cond_basic_slope and cond_basic_vol):
            return None
        
        level = "A"

        # 【AA级：标准形态】
        cond_aa_trend = curr['ma60'] > df.iloc[-5]['ma60']
        cond_aa_macd = curr['macd'] > prev['macd']
        
        if cond_aa_trend and cond_aa_macd:
            level = "AA"

            # 【AAA级：极品老鸭头】
            cond_aaa_head = recent_20['high'].max() > curr['ma60'] * 1.08
            cond_aaa_nostril = recent_10['volume'].min() < curr['vol_ma60'] * 0.8
            cond_aaa_water = curr['dif'] > 0
            
            if cond_aaa_head and cond_aaa_nostril and cond_aaa_water:
                level = "AAA"

        return {
            'filter_date': curr['date'],
            'code': code, 
            'name': None, 
            'level': level,
            'price': round(curr['close'], 2), 
            'pct_chg': f"{curr['pct_chg']}%",
            'turnover': f"{curr['turnover']}%",
            'vol_ratio': round(curr['volume'] / curr['vol_ma5'], 2)
        }
            
    except Exception:
        return None

def main():
    if not os.path.exists(NAMES_FILE): return
    
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    names_df = names_df[~names_df['name'].str.contains(r'ST|退|\*ST', na=False)]
    valid_codes = set(names_df['code'].apply(lambda x: x.zfill(6)).tolist())

    files = [f for f in glob.glob(f'{DATA_DIR}/*.csv') if os.path.basename(f).split('.')[0].zfill(6) in valid_codes]
    
    if not files:
        print("Stock data directory is empty.")
        return

    with Pool(cpu_count()) as p:
        results = p.map(analyze_logic, files)
    
    results = [r for r in results if r is not None]
    
    if results:
        res_df = pd.DataFrame(results)
        name_dict = names_df.set_index(names_df['code'].apply(lambda x: x.zfill(6)))['name'].to_dict()
        res_df['name'] = res_df['code'].map(name_dict)
        
        # 排序逻辑
        res_df = res_df.sort_values(by=['level', 'pct_chg'], ascending=[False, False])
        
        # --- 路径修改：统一保存到 results/duck_hunter/ ---
        now = datetime.now()
        dir_path = os.path.join(OUTPUT_BASE, 'duck_hunter') # 锁定子目录名
        os.makedirs(dir_path, exist_ok=True)
        
        # 文件名保持日期唯一，方便共振分析脚本检索最新文件
        save_path = os.path.join(dir_path, f"duck_hunter_{now.strftime('%Y%m%d')}.csv")
        
        final_cols = ['filter_date', 'code', 'name', 'level', 'price', 'pct_chg', 'turnover', 'vol_ratio']
        res_df[final_cols].to_csv(save_path, index=False, encoding='utf-8-sig') # 增加编码防止乱码
        
        counts = res_df['level'].value_counts()
        print("-" * 50)
        print(f"筛选日期: {res_df['filter_date'].iloc[0]}")
        print(f"总计入选: {len(res_df)} 只")
        print(f"AAA 级 (极品老鸭): {counts.get('AAA', 0)} 只")
        print(f"AA  级 (标准形态): {counts.get('AA', 0)} 只")
        print(f"A    级 (基础强势): {counts.get('A', 0)} 只")
        print(f"保存路径: {save_path}")
        print("-" * 50)
    else:
        print(f"今日 ({datetime.now().strftime('%Y-%m-%d')}) 无符合强势老鸭头形态的股票。")

if __name__ == "__main__":
    main()
