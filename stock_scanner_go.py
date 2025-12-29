import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count
import numpy as np

# ==================== 2025“买入即获利”极简精选参数 ===================
MIN_PRICE = 5.0              # 提高股价门槛，过滤低迷小票
MAX_AVG_TURNOVER_30 = 2.5    # 换手率更低，意味着筹码锁定更好

# --- 极致缩量：锁定统计中胜率100%的区间 ---
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 0.85      # 严格限制在0.85以下，只做缩量洗盘

# --- 极度超跌：锁定V型反转高发区 ---
RSI6_MAX = 25                # 锁定极致超跌区
KDJ_K_MAX = 30               # 确保K值在底部磨底
MIN_PROFIT_POTENTIAL = 15    # 要求反弹空间至少15%

# --- 形态与趋势控制 ---
MAX_TODAY_CHANGE = 1.5       # 拒绝大阳线拉升后的追高，只选低位横盘或微涨
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
    
    # --- 自动剔除 ST 股 ---
    if "ST" in stock_name.upper():
        return None

    try:
        df_raw = pd.read_csv(file_path)
        if len(df_raw) < 60: return None
        
        df = calculate_indicators(df_raw)
        latest = df.iloc[-1]
        
        # 1. 基础门槛
        if latest['收盘'] < MIN_PRICE or latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            return None
        
        # 2. 空间与涨跌幅控制 (拒绝大涨，只要低吸)
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        if potential < MIN_PROFIT_POTENTIAL or change > MAX_TODAY_CHANGE:
            return None
        
        # 3. 指标共振：极致超跌
        if latest['rsi6'] > RSI6_MAX or latest['kdj_k'] > KDJ_K_MAX:
            return None
        
        # 4. 止跌确认：价格必须站在5日线之上（拒绝阴跌）
        if latest['收盘'] < latest['ma5']:
            return None
            
        # 5. 极致缩量确认
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
    print(f"🚀 极致缩量精选扫描开始... 目标：高胜率低吸")

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
        # 排序：量比越小越优先（符合统计最高胜率逻辑）
        df_result = df_result.sort_values(by='今日量比', ascending=True)
        
        print(f"\n🎯 经过【胜率看板】优化，仅筛选出 {len(results)} 只极品标的:")
        print(df_result.to_string(index=False)) 
        
        date_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
        year_month = now_shanghai.strftime('%Y/%m')
        save_path = f"results/{year_month}"
        os.makedirs(save_path, exist_ok=True)
        
        file_name = f"极致精选_轮动_{date_str}.csv"
        df_result.to_csv(os.path.join(save_path, file_name), index=False, encoding='utf_8_sig')
        print(f"\n✅ 极精选报告已保存。")
    else:
        print("\n😱 暂无符合极致缩量且超跌止跌的标的，耐心等待是最高级的策略。")

if __name__ == "__main__":
    main()
