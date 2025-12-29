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
        # 1. 加载数据与表头映射
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        df = df.rename(columns={
            '日期':'date', '股票代码':'code', '开盘':'open', 
            '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'
        })
        
        # 格式化代码并过滤板块 (只要沪深A股 60/00)
        code_raw = str(df.iloc[-1]['code']).split('.')[0]
        code = code_raw.zfill(6)
        if not (code.startswith('60') or code.startswith('00')):
            return None

        # 2. 价格过滤: 5.0 - 20.0 元
        curr = df.iloc[-1]
        if not (5.0 <= curr['close'] <= 20.0):
            return None

        # 3. 计算技术指标
        df['ma5'] = df['close'].rolling(5).mean()
        df['ma10'] = df['close'].rolling(10).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['ma60'] = df['close'].rolling(60).mean()
        df['vol_ma5'] = df['volume'].rolling(5).mean()
        df['vol_ma60'] = df['volume'].rolling(60).mean()
        
        # MACD 计算
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = exp1 - exp2
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['dif'] - df['dea']) * 2

        prev = df.iloc[-2]
        recent_10 = df.iloc[-10:-1]
        recent_20 = df.iloc[-20:-1]

        # --- 分级判定逻辑 ---
        
        # 【A级：基础强势逻辑】(您最初要求的逻辑)
        # 股价在5,10日线上 + 均线向上 + (放量突破1.2倍 或 回踩MA10不破)
        cond_basic_trend = curr['close'] > curr['ma5'] > curr['ma10']
        cond_basic_slope = curr['ma5'] > prev['ma5']
        cond_basic_vol = (curr['volume'] > curr['vol_ma5'] * 1.2) or (curr['close'] >= curr['ma10'] and curr['volume'] <= curr['vol_ma5'])
        
        if not (cond_basic_trend and cond_basic_slope and cond_basic_vol):
            return None
        
        level = "A"

        # 【AA级：标准形态】(加入中期趋势与动量)
        # MA60向上 + MACD红柱增长（或刚金叉）
        cond_aa_trend = curr['ma60'] > df.iloc[-5]['ma60']
        cond_aa_macd = curr['macd'] > prev['macd']
        
        if cond_aa_trend and cond_aa_macd:
            level = "AA"

            # 【AAA级：极品老鸭头】(网络实战高成功率指标)
            # 前期有明显鸭头高度(12%) + 出现过地量洗盘(鸭鼻孔 < 0.8倍均量) + MACD DIF在水上
            cond_aaa_head = recent_20['high'].max() > curr['ma60'] * 1.12
            cond_aaa_nostril = recent_10['volume'].min() < curr['vol_ma60'] * 0.8
            cond_aaa_water = curr['dif'] > 0
            
            if cond_aaa_head and cond_aaa_nostril and cond_aaa_water:
                level = "AAA"

        return {'code': code, 'name': None, 'price': round(curr['close'], 2), 'level': level}
            
    except Exception:
        return None

def main():
    if not os.path.exists(NAMES_FILE):
        print(f"Error: {NAMES_FILE} not found.")
        return
    
    # 加载名称并排除ST
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    names_df = names_df[~names_df['name'].str.contains(r'ST|退|\*ST', na=False)]
    valid_codes = set(names_df['code'].apply(lambda x: x.zfill(6)).tolist())

    # 扫描数据文件
    files = [f for f in glob.glob(f'{DATA_DIR}/*.csv') if os.path.basename(f).split('.')[0].zfill(6) in valid_codes]
    
    if not files:
        print("No files to process.")
        return

    # 并行处理加速
    with Pool(cpu_count()) as p:
        results = p.map(analyze_logic, files)
    
    results = [r for r in results if r is not None]
    
    if results:
        res_df = pd.DataFrame(results)
        # 匹配股票名称
        name_dict = names_df.set_index(names_df['code'].apply(lambda x: x.zfill(6)))['name'].to_dict()
        res_df['name'] = res_df['code'].map(name_dict)
        
        # 按级别排序 (AAA -> AA -> A)
        res_df = res_df.sort_values(by='level', ascending=False)
        
        # 保存结果
        now = datetime.now()
        dir_path = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
        os.makedirs(dir_path, exist_ok=True)
        
        save_path = os.path.join(dir_path, f"duck_hunter_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        res_df[['code', 'name', 'price', 'level']].to_csv(save_path, index=False)
        
        # 打印统计
        counts = res_df['level'].value_counts()
        print(f"筛选完成！统计: AAA级 {counts.get('AAA',0)} 只, AA级 {counts.get('AA',0)} 只, A级 {counts.get('A',0)} 只")
    else:
        print("今日无匹配股票。")

if __name__ == "__main__":
    main()
