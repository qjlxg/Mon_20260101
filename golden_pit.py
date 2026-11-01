import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 配置常量
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results/golden_pit'

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        # 增加数据量要求至120天，以支撑MA60等中线指标计算
        if len(df) < 120: return None
        
        df = df.rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','最低':'low','最高':'high'})
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        recent_20 = df.iloc[-20:]
        
        # --- 1. 基础属性过滤 (保留原逻辑并略微优化) ---
        code_str = str(curr['code']).split('.')[0].zfill(6)
        
        # 价格限制
        if not (5.0 <= curr['close'] <= 25.0): # 略放宽上限至25元，因近期行情波动
            return None
            
        # 排除创业板 (30开头)
        if code_str.startswith('30'):
            return None
            
        # 只保留深沪A股
        if not code_str.startswith(('60', '00', '688')):
            return None

        # --- 2. 核心指标计算 ---
        # 均线系统
        ma5 = df['close'].rolling(5).mean()
        ma10 = df['close'].rolling(10).mean()
        ma60 = df['close'].rolling(60).mean() # 中线生命线
        
        # 量能指标
        vol_ma20 = df['volume'].rolling(20).mean()
        pit_low = recent_20['low'].min()
        
        # --- 3. 实战增强过滤逻辑 (新增筛选维度) ---
        
        # A. 中线趋势过滤：拒绝阴跌股。股价必须在60日均线附近或上方，确保是大趋势向好下的“坑”
        # 逻辑：现价 > 60日线 * 97% (允许微破60日线洗盘)
        cond_trend = curr['close'] > (ma60.iloc[-1] * 0.97)
        
        # B. 坑底缩量确认：黄金坑必须有缩量洗盘过程。
        # 逻辑：过去15天内，必须出现过成交量小于20日均量0.6倍的“地量”
        pit_area_vol = df.iloc[-15:-2]['volume']
        cond_pit_vol_dry = any(pit_area_vol < vol_ma20.iloc[-1] * 0.6)
        
        # C. 填坑反转强度：今日成交量必须有效放大，且站稳多根均线
        # 逻辑：今日量比 > 1.2 且 价格同时站上5日和10日线
        cond_strong_start = (curr['volume'] > vol_ma20.iloc[-1] * 1.1) and \
                            (curr['close'] > ma5.iloc[-1]) and \
                            (curr['close'] > ma10.iloc[-1])
        
        # D. 反弹幅度控制 (保留原有逻辑，调整范围)
        rebound_pct = (curr['close'] / pit_low) - 1
        cond_rebound = 0.03 < rebound_pct < 0.15 # 略微放宽，捕捉刚启动和确认后的信号
        
        # E. 排除近期大跌 (保留原逻辑)
        recent_3days = df.iloc[-3:]
        no_big_drop = all((recent_3days['close'] / recent_3days['open']) > 0.95)
        
        # F. 活跃度 (保留原逻辑)
        amplitude = (curr['high'] - curr['low']) / prev['close'] > 0.03
        
        # --- 4. 综合判定 (只有全部满足才输出) ---
        if cond_trend and cond_pit_vol_dry and cond_strong_start and \
           cond_rebound and no_big_drop and amplitude and (curr['close'] > curr['open']):
            
            return {
                'date': curr['date'],
                'code': code_str,
                'name': '', 
                'price': curr['close'],
                'rebound': f"{round(rebound_pct*100, 2)}%",
                'amplitude': f"{round((curr['high'] - curr['low']) / prev['close'] * 100, 2)}%",
                'vol_ratio': round(curr['volume'] / vol_ma20.iloc[-1], 2),
                'ma60_pos': "线上" if curr['close'] > ma60.iloc[-1] else "线缘"
            }
    except Exception as e:
        return None

def main():
    if not os.path.exists(NAMES_FILE): 
        print(f"错误: 找不到名称文件 {NAMES_FILE}")
        return
        
    os.makedirs(OUTPUT_BASE, exist_ok=True)
    files = glob.glob(f'{DATA_DIR}/*.csv')
    
    print(f"开始分析 {len(files)} 只股票...")

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
            res_df = res_df[~res_df['name'].str.contains('退', na=False)]
        
        # 按照量比降序排列 (优先看资金流入最明显的)
        res_df = res_df.sort_values(by='vol_ratio', ascending=False)
        
        # 保存结果
        save_path = f"{OUTPUT_BASE}/golden_pit_pro_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        
        print(f"分析完成！")
        print(f"实战强化筛选后符合条件: {len(res_df)} 只 (结果已大幅压缩)")
        print(f"结果已保存至: {save_path}")
    else:
        print("未发现符合条件的股票。")

if __name__ == "__main__":
    main()
