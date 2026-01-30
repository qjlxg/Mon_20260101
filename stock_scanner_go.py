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

# --- 极致缩量与小阴小阳 ---
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 0.85      # 原始：严格限制在0.85以下
MAX_TODAY_CHANGE = 1.5       # 锁定“小阴小阳”，拒绝剧烈波动

# --- 极度超跌与多周期共振 ---
RSI6_MAX = 25                # 锁定短线极致超跌区
RSI14_MAX = 35               # 中线RSI共振参考
KDJ_K_MAX = 30               # 确保K值在底部磨底
MIN_PROFIT_POTENTIAL = 15    # 要求反弹空间至少15%
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
    df = df.reset_index(drop=True)
    close = df['收盘']
    delta = close.diff()
    
    # 1. 多周期RSI (6, 14)
    def get_rsi(period):
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    df['rsi6'] = get_rsi(6)
    df['rsi14'] = get_rsi(14)
    
    # 2. KDJ (9,3,3) 与 金叉逻辑
    low_list = df['最低'].rolling(window=9).min()
    high_list = df['最高'].rolling(window=9).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    df['kdj_gold'] = (df['kdj_k'] > df['kdj_d']) & (df['kdj_k'].shift(1) <= df['kdj_d'].shift(1))
    
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
        
        # --- 统计逻辑开始 (关卡掉队记录) ---
        stats_dict['total_scanned'] += 1
        
        if latest['收盘'] < MIN_PRICE:
            stats_dict['fail_price'] += 1
            return None
        if latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            stats_dict['fail_turnover'] += 1
            return None
        
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        
        # 记录空间不足
        if potential < MIN_PROFIT_POTENTIAL:
            stats_dict['fail_potential'] += 1
        
        # 记录指标不达标
        is_oversold = latest['rsi6'] <= RSI6_MAX and latest['rsi14'] <= RSI14_MAX and latest['kdj_k'] <= KDJ_K_MAX
        if not is_oversold:
            stats_dict['fail_rsi_kdj'] += 1
        
        # 记录缩量不达标
        is_shrink_vol = MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO
        if not is_shrink_vol:
            stats_dict['fail_volume'] += 1

        is_small_body = abs(change) <= MAX_TODAY_CHANGE
        if not is_small_body:
            stats_dict['fail_shape'] += 1

        # --- 最终策略判定 ---
        strategy_tag = ""

        # 1. 极致精选 (严格执行你的原始所有条件)
        if (is_oversold and is_shrink_vol and is_small_body and potential >= MIN_PROFIT_POTENTIAL):
            if latest['kdj_gold']:
                strategy_tag = "1-极致共振金叉"
            else:
                strategy_tag = "2-极致缩量捡漏"

        # 2. 准入选逻辑 (放宽空间和量比，但指标底限不变)
        elif (is_oversold and 
              latest['vol_ratio'] <= 1.1 and         # 量比放宽到1.1
              potential >= 10.0 and                  # 空间放宽到10%
              abs(change) <= 2.5):                  # 波动放宽到2.5%
            strategy_tag = "3-准入选观察池"

        if strategy_tag:
            return {
                '类型': strategy_tag,
                '代码': stock_code,
                '名称': stock_name,
                '现价': round(latest['收盘'], 2),
                '量比': round(latest['vol_ratio'], 2),
                'RSI6/14': f"{round(latest['rsi6'],1)}/{round(latest['rsi14'],1)}",
                'KDJ状态': "金叉" if latest['kdj_gold'] else "底位",
                '距60日线': f"{round(potential, 1)}%",
                '今日涨跌': f"{round(change, 1)}%"
            }
    except:
        return None
    return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 极致精选+准入选扫描开始... (当前时间: {now_shanghai.strftime('%Y-%m-%d %H:%M')})")

    manager = Manager()
    stats_dict = manager.dict({
        'total_scanned': 0, 'fail_price': 0, 'fail_turnover': 0,
        'fail_potential': 0, 'fail_rsi_kdj': 0, 'fail_volume': 0, 'fail_shape': 0
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
    
    # --- 输出诊断报告 (功能完全保留) ---
    print("\n" + "="*50)
    print("📊 市场环境诊断报告 (未入选原因统计)")
    print("-" * 50)
    print(f"1. 扫描总数: {stats_dict['total_scanned']} 只")
    print(f"2. 股价或换手率不符: {stats_dict['fail_price'] + stats_dict['fail_turnover']} 只")
    print(f"3. 距60日线空间不足 {MIN_PROFIT_POTENTIAL}%: {stats_dict['fail_potential']} 只")
    print(f"4. RSI6/14未共振超跌: {stats_dict['fail_rsi_kdj']} 只")
    print(f"5. 成交量比未落在 {MIN_VOLUME_RATIO}-{MAX_VOLUME_RATIO}: {stats_dict['fail_volume']} 只")
    print(f"6. 非小阴小阳形态: {stats_dict['fail_shape']} 只")
    print("="*50)
        
    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values(by=['类型', '距60日线'], ascending=[True, False])
        print(f"\n🎯 选出结果 ({len(results)} 只):")
        print(df_result.to_string(index=False))
        
        os.makedirs("results", exist_ok=True)
        file_name = f"极致及准入选_{now_shanghai.strftime('%Y%m%d_%H%M')}.csv"
        df_result.to_csv(os.path.join("results", file_name), index=False, encoding='utf_8_sig')
    else:
        print("\n😱 诊断结果：全场无任何符合极致或准入选条件的标的，建议继续空仓休息。")

if __name__ == "__main__":
    main()
