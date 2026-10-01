import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count
import numpy as np

# ==================== 2025“温和低吸”精选参数 (已优化) ===================
MIN_PRICE = 5.0              # 股价门槛
MAX_AVG_TURNOVER_30 = 3.5    # 换手率放宽至 3.5%，增加活跃度容忍

# --- 缩量洗盘：从极致缩量改为不放量即可 ---
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 1.05      # 只要成交量不明显超过5日均量即可

# --- 超跌区域：从极度超跌转为筑底区 ---
RSI6_MAX = 35                # 放宽至35，捕获更多筑底标的
KDJ_K_MAX = 40               # K值在40以下均视为弱势筑底
MIN_PROFIT_POTENTIAL = 10    # 偏离60日线10%即可，不强求15%

# --- 形态与趋势控制 ---
MAX_TODAY_CHANGE = 2.0       # 允许小幅反弹，上限放宽至2%
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
    """计算核心指标"""
    df = df.reset_index(drop=True)
    close = df['收盘']
    
    # 1. RSI6
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi6'] = 100 - (100 / (1 + rs))
    
    # 2. KDJ (9,3,3)
    low_list = df['最低'].rolling(window=9).min()
    high_list = df['最高'].rolling(window=9).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    
    # 3. MA5 & MA60
    df['ma5'] = close.rolling(window=5).mean()
    df['ma60'] = close.rolling(window=60).mean()
    
    # 4. 换手率均值与量比
    df['avg_turnover_30'] = df['换手率'].rolling(window=30).mean()
    df['vol_ma5'] = df['成交量'].shift(1).rolling(window=5).mean()
    df['vol_ratio'] = df['成交量'] / df['vol_ma5']
    
    return df

def process_single_stock(args):
    file_path, name_map = args
    stock_code = os.path.basename(file_path).split('.')[0]
    stock_name = name_map.get(stock_code, "未知")
    
    if "ST" in stock_name.upper():
        return None

    try:
        df_raw = pd.read_csv(file_path)
        if len(df_raw) < 60: return None
        
        df = calculate_indicators(df_raw)
        latest = df.iloc[-1]
        
        if latest['收盘'] < MIN_PRICE or latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            return None
        
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        
        if potential < MIN_PROFIT_POTENTIAL or change > MAX_TODAY_CHANGE:
            return None
        
        if latest['rsi6'] > RSI6_MAX or latest['kdj_k'] > KDJ_K_MAX:
            return None
        
        if latest['收盘'] < latest['ma5']:
            return None
            
        if not (MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO):
            return None

        return {
            '代码': stock_code,
            '名称': stock_name,
            '最新日期': latest['日期'],
            '现价': round(latest['收盘'], 2),
            '今日量比': round(latest['vol_ratio'], 2),
            'RSI6': round(latest['rsi6'], 1),
            'K值': round(latest['kdj_k'], 1),
            '距60日线空间': f"{round(potential, 1)}%",
            '今日涨跌': f"{round(change, 1)}%"
        }
    except:
        return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 温和版精选扫描开始... 寻找稳健低吸机会")

    name_map = {}
    if os.path.exists(NAME_MAP_FILE):
        n_df = pd.read_csv(NAME_MAP_FILE, dtype={'code': str})
        name_map = dict(zip(n_df['code'].str.zfill(6), n_df['name']))

    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    tasks = [(file_path, name_map) for file_path in file_list]

    with Pool(processes=cpu_count()) as pool:
        raw_results = pool.map(process_single_stock, tasks)

    results = [r for r in raw_results if r is not None]
        
    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values(by='今日量比', ascending=True)
        
        print(f"\n🎯 扫描完成，符合温和低吸逻辑的标的 ({len(results)} 只):")
        print(df_result.to_string(index=False)) 
        
        date_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
        year_month = now_shanghai.strftime('%Y/%m')
        save_path = f"results/{year_month}"
        os.makedirs(save_path, exist_ok=True)
        
        file_name = f"温和精选_低吸_{date_str}.csv"
        df_result.to_csv(os.path.join(save_path, file_name), index=False, encoding='utf_8_sig')
        print(f"\n✅ 扫描报告已保存。")
    else:
        print("\n😱 即使放宽条件仍无标的，说明目前市场整体强度较高或处于普涨中，无需刻意抄底。")

if __name__ == "__main__":
    main()
