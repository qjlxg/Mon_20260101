import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count, Manager
import numpy as np

# ==================== 2025“买入即获利”极简精选参数 (原始固定) ===================
MIN_PRICE = 5.0              # 提高股价门槛
MAX_AVG_TURNOVER_30 = 2.5    # 换手率更低，意味着筹码锁定更好

# --- 极致缩量 ---
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 0.85      # 原始：严格限制在0.85以下

# --- 极度超跌 ---
RSI6_MAX = 25                # 锁定极致超跌区
KDJ_K_MAX = 30               # 确保K值在底部磨底
MIN_PROFIT_POTENTIAL = 15    # 要求反弹空间至少15%

# --- 形态与趋势控制 ---
MAX_TODAY_CHANGE = 1.5       # 原始：拒绝大阳线，只要微涨
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
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
    file_path, name_map, stats_dict = args
    stock_code = os.path.basename(file_path).split('.')[0]
    stock_name = name_map.get(stock_code, "未知")
    
    if "ST" in stock_name.upper(): return None

    try:
        df_raw = pd.read_csv(file_path)
        if len(df_raw) < 60: return None
        df = calculate_indicators(df_raw)
        latest = df.iloc[-1]
        
        # 统计逻辑：记录每一关卡掉队的情况
        stats_dict['total_scanned'] += 1
        
        # 基础门槛检查
        if latest['收盘'] < MIN_PRICE:
            stats_dict['fail_price'] += 1
            return None
        if latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            stats_dict['fail_turnover'] += 1
            return None
        
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        
        if potential < MIN_PROFIT_POTENTIAL:
            stats_dict['fail_potential'] += 1
            return None

        # 超跌逻辑检查
        is_oversold = latest['rsi6'] <= RSI6_MAX and latest['kdj_k'] <= KDJ_K_MAX
        if not is_oversold:
            stats_dict['fail_rsi_kdj'] += 1
        
        # 缩量逻辑检查
        is_shrink_vol = MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO
        if not is_shrink_vol:
            stats_dict['fail_volume'] += 1

        strategy_tag = ""

        # --- 模式一：极致缩量捡漏 (原始参数) ---
        if is_oversold and is_shrink_vol and abs(change) <= MAX_TODAY_CHANGE:
            strategy_tag = "极致缩量捡漏"

        # --- 模式二：缩量反转确认 (兼容逻辑) ---
        elif (latest['rsi6'] <= RSI6_MAX + 5 and 
              latest['kdj_k'] <= KDJ_K_MAX + 5 and
              latest['收盘'] > latest['ma5'] and     
              0.5 <= latest['vol_ratio'] <= 1.0 and   
              0 < change <= MAX_TODAY_CHANGE + 1.0): 
            strategy_tag = "缩量反转确认"

        if strategy_tag:
            return {
                '类型': strategy_tag, '代码': stock_code, '名称': stock_name,
                '现价': round(latest['收盘'], 2), '量比': round(latest['vol_ratio'], 2),
                'RSI6': round(latest['rsi6'], 1), '距60日线': f"{round(potential, 1)}%",
                '今日涨跌': f"{round(change, 1)}%"
            }
    except:
        return None
    return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 极致精选双模扫描开始... (当前时间: {now_shanghai.strftime('%Y-%m-%d %H:%M')})")

    # 使用 Manager 共享字典进行多进程统计
    manager = Manager()
    stats_dict = manager.dict({
        'total_scanned': 0, 'fail_price': 0, 'fail_turnover': 0,
        'fail_potential': 0, 'fail_rsi_kdj': 0, 'fail_volume': 0
    })

    name_map = {}
    if os.path.exists(NAME_MAP_FILE):
        n_df = pd.read_csv(NAME_MAP_FILE, dtype={'code': str})
        name_map = dict(zip(n_df['code'].str.zfill(6), n_df['name']))

    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    tasks = [(f, name_map, stats_dict) for f in file_list]

    with Pool(processes=cpu_count()) as pool:
        raw_results = pool.map(process_single_stock, tasks)

    results = [r for r in raw_results if r is not None]
    
    # --- 输出诊断报告 ---
    print("\n" + "="*40)
    print("📊 市场环境诊断报告 (未入选原因统计)")
    print("-" * 40)
    print(f"扫描总数: {stats_dict['total_scanned']} 只")
    print(f"1. 股价低于 {MIN_PRICE}元: {stats_dict['fail_price']} 只")
    print(f"2. 30日均换手 > {MAX_AVG_TURNOVER_30}%: {stats_dict['fail_turnover']} 只")
    print(f"3. 距60日线空间不足 {MIN_PROFIT_POTENTIAL}%: {stats_dict['fail_potential']} 只")
    print(f"4. RSI6或KDJ未达极致超跌: {stats_dict['fail_rsi_kdj']} 只")
    print(f"5. 成交量比未落在 {MIN_VOLUME_RATIO}-{MAX_VOLUME_RATIO}: {stats_dict['fail_volume']} 只")
    print("="*40)
        
    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values(by=['类型', '距60日线'], ascending=[True, False])
        print(f"\n🎯 最终入选名单 ({len(results)} 只):")
        print(df_result.to_string(index=False))
        
        os.makedirs("results", exist_ok=True)
        file_name = f"双模式精选_{now_shanghai.strftime('%Y%m%d_%H%M')}.csv"
        df_result.to_csv(os.path.join("results", file_name), index=False, encoding='utf_8_sig')
    else:
        print("\n😱 诊断结果：当前市场个股普遍不符合“超跌+缩量”的极致买点，请继续耐心等待。")

if __name__ == "__main__":
    main()
