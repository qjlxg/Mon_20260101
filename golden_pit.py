import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results/golden_pit'

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        # 为了计算MA5和观察趋势，数据量要求提升至60天以确保稳定
        if len(df) < 60: return None
        
        df = df.rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','最低':'low','最高':'high'})
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        recent_20 = df.iloc[-20:] # 观察过去20天
        
        # --- 1. 基础属性过滤 (价格、创业板) ---
        code_str = str(curr['code']).split('.')[0].zfill(6)
        
        # 价格限制：5.0 <= 现价 <= 20.0
        if not (5.0 <= curr['close'] <= 20.0):
            return None
            
        # 排除创业板 (30开头)
        if code_str.startswith('30'):
            return None
            
        # 只保留深沪A股 (60:沪市主板, 00:深市主板/中小板, 688:科创板)
        # 如果不需要科创板，可从元组中删除 '688'
        if not code_str.startswith(('60', '00', '688')):
            return None

        # --- 2. 核心指标计算 ---
        pit_low = recent_20['low'].min()
        avg_vol_20 = df['volume'].rolling(20).mean().iloc[-1]
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        
        # --- 3. 强化版黄金坑逻辑 ---
        
        # A. 坑底反弹幅度 5%-12%
        rebound_pct = (curr['close'] / pit_low) - 1
        cond_rebound = 0.05 < rebound_pct < 0.12
        
        # B. 今日收阳线，且站稳5日均线 (过滤低位无效震荡)
        cond_sun = (curr['close'] > curr['open']) and (curr['close'] > ma5)
        
        # C. 量能：摆脱地量且量比放大 (成交量 > 20日均量的0.8倍 且 大于昨日成交量)
        cond_vol = (curr['volume'] > avg_vol_20 * 0.8) and (curr['volume'] > prev['volume'])
        
        # D. 排除大阴线：过去3天内没有出现过跌幅 > 5% 的实体大阴线 (排除下跌中继)
        recent_3days = df.iloc[-3:]
        no_big_drop = all((recent_3days['close'] / recent_3days['open']) > 0.95)
        
        # E. 活跃度：今日振幅 > 3% (代表资金介入博弈)
        amplitude = (curr['high'] - curr['low']) / prev['close'] > 0.03
        
        # --- 4. 综合判定 ---
        if cond_rebound and cond_sun and cond_vol and no_big_drop and amplitude:
            return {
                'date': curr['date'],
                'code': code_str,
                'name': '', # 预留位置
                'price': curr['close'],
                'rebound': f"{round(rebound_pct*100, 2)}%",
                'amplitude': f"{round((curr['high'] - curr['low']) / prev['close'] * 100, 2)}%",
                'vol_ratio': round(curr['volume'] / avg_vol_20, 2)
            }
    except: 
        return None

def main():
    if not os.path.exists(NAMES_FILE): 
        print(f"错误: 找不到名称文件 {NAMES_FILE}")
        return
        
    os.makedirs(OUTPUT_BASE, exist_ok=True)
    files = glob.glob(f'{DATA_DIR}/*.csv')
    
    # 多进程执行分析
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(analyze_logic, files) if r is not None]
    
    if results:
        res_df = pd.DataFrame(results)
        
        # 读取名称文件并清洗代码
        names = pd.read_csv(NAMES_FILE, dtype={'code': str})
        names['code'] = names['code'].apply(lambda x: str(x).split('.')[0].zfill(6))
        
        # 移除原有的空name列，合并真实的名称
        if 'name' in res_df.columns: res_df = res_df.drop(columns=['name'])
        res_df = pd.merge(res_df, names, on='code', how='left')
        
        # --- 5. 最终结果过滤 ---
        
        # 排除 ST 股票
        if 'name' in res_df.columns:
            res_df = res_df[~res_df['name'].str.contains('ST', na=False)]
        
        # 按照反弹幅度升序排列 (优先看刚起步的)
        res_df = res_df.sort_values(by='rebound')
        
        # 保存结果
        save_path = f"{OUTPUT_BASE}/golden_pit_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"分析完成！")
        print(f"符合黄金坑形态 + 价格5-20元 + 非ST/创业板: {len(res_df)} 只")
        print(f"结果已保存至: {save_path}")
    else:
        print("未发现符合条件的股票。")

if __name__ == "__main__":
    main()
