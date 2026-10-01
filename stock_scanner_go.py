import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count, Manager
import numpy as np

# ==================== 2026“量价齐升”核心参数 (金标准) ===================
MIN_PRICE = 5.0              # 股价门槛：过滤垃圾低价股
MAX_AVG_TURNOVER_30 = 2.5    # 筹码锁定门槛：30日均换手低于2.5%

# --- 极致缩量与小阴小阳逻辑 ---
MIN_VOLUME_RATIO = 0.2       
MAX_VOLUME_RATIO = 0.85      # 极致缩量上限（针对潜伏型）
MAX_TODAY_CHANGE = 1.5       # 形态门槛：波动绝对值小于1.5%

# --- 极度超跌与多周期共振逻辑 ---
RSI6_MAX = 25                # RSI6极度超跌
RSI14_MAX = 35               # RSI14多周期确认
KDJ_K_MAX = 30               # KDJ处于底部区域
MIN_PROFIT_POTENTIAL = 15    # 反弹空间：距60日线至少15%空间
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
    """
    计算核心指标体系：含MA、RSI、KDJ、MACD及量能变化
    """
    df = df.reset_index(drop=True)
    close = df['收盘']
    delta = close.diff()
    
    # 1. RSI计算 (6日 & 14日)
    def get_rsi(period):
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    df['rsi6'] = get_rsi(6)
    df['rsi14'] = get_rsi(14)
    
    # 2. KDJ计算 (9,3,3)
    low_list = df['最低'].rolling(window=9).min()
    high_list = df['最高'].rolling(window=9).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=2).mean()
    df['kdj_gold'] = (df['kdj_k'] > df['kdj_d']) & (df['kdj_k'].shift(1) <= df['kdj_d'].shift(1))
    
    # 3. MACD计算 (12, 26, 9)
    df['ema12'] = close.ewm(span=12, adjust=False).mean()
    df['ema26'] = close.ewm(span=26, adjust=False).mean()
    df['diff'] = df['ema12'] - df['ema26']
    df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['diff'] - df['dea']) * 2
    # MACD金叉判断
    df['macd_gold'] = (df['diff'] > df['dea']) & (df['diff'].shift(1) <= df['dea'].shift(1))
    # MACD能量增强：绿柱缩短或翻红
    df['macd_improving'] = df['macd_hist'] > df['macd_hist'].shift(1)

    # 4. 均线与量能指标
    df['ma5'] = close.rolling(window=5).mean()
    df['ma60'] = close.rolling(window=60).mean()
    df['avg_turnover_30'] = df['换手率'].rolling(window=30).mean()
    df['vol_ma5'] = df['成交量'].shift(1).rolling(window=5).mean()
    df['vol_ratio'] = df['成交量'] / df['vol_ma5']
    df['vol_increase'] = df['成交量'] > df['成交量'].shift(1)  # 较昨日放量
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
        
        # --- 关卡式统计 (不精简，严格记录掉队原因) ---
        stats_dict['total_scanned'] += 1
        
        # 1. 基础门槛统计
        if latest['收盘'] < MIN_PRICE:
            stats_dict['fail_price'] += 1
            return None
        if latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            stats_dict['fail_turnover'] += 1
            return None
        
        # 2. 空间门槛统计
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        if potential < MIN_PROFIT_POTENTIAL:
            stats_dict['fail_potential'] += 1
        
        # 3. 指标超跌统计
        is_oversold = latest['rsi6'] <= RSI6_MAX and latest['rsi14'] <= RSI14_MAX and latest['kdj_k'] <= KDJ_K_MAX
        if not is_oversold:
            stats_dict['fail_rsi_kdj'] += 1
        
        # 4. 缩量逻辑统计
        is_shrink_vol = MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO
        if not is_shrink_vol:
            stats_dict['fail_volume'] += 1

        # 5. 形态门槛统计
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        is_small_body = abs(change) <= MAX_TODAY_CHANGE
        if not is_small_body:
            stats_dict['fail_shape'] += 1

        # --- 瞄准“量价齐升”与“共振”判定 ---
        strategy_tag = ""

        # 【0级：点火启动】解决时间成本。逻辑：超跌 + 站上5日线 + 较昨日放量 + MACD改善
        if is_oversold and latest['收盘'] > latest['ma5'] and latest['vol_increase'] and latest['vol_ratio'] > 0.6:
            if latest['macd_improving']:
                strategy_tag = "0-点火启动(即买即涨)"

        # 【1级：共振金叉】追求技术指标的一致性。逻辑：超跌 + KDJ金叉 + MACD改善
        if strategy_tag == "" and is_oversold and latest['kdj_gold'] and latest['macd_improving']:
            strategy_tag = "1-多指标共振金叉"

        # 【2级：极致潜伏】追求极致安全。逻辑：超跌 + 极致缩量 + 小阴小阳
        if strategy_tag == "" and is_oversold and is_shrink_vol and is_small_body and potential >= MIN_PROFIT_POTENTIAL:
            strategy_tag = "2-极致缩量潜伏"

        # 【3级：准入选观察】宽限条件，进入蓄势区
        elif strategy_tag == "" and is_oversold and latest['vol_ratio'] <= 1.1 and potential >= 10.0:
            strategy_tag = "3-准入选观察池"

        if strategy_tag:
            macd_status = "金叉" if latest['macd_gold'] else ("红柱" if latest['macd_hist'] > 0 else "绿柱缩短")
            return {
                '类型': strategy_tag,
                '代码': stock_code,
                '名称': stock_name,
                '现价': round(latest['收盘'], 2),
                '量比': round(latest['vol_ratio'], 2),
                'RSI6/14': f"{round(latest['rsi6'],1)}/{round(latest['rsi14'],1)}",
                'KDJ/MACD': f"{'金叉' if latest['kdj_gold'] else '底位'}/{macd_status}",
                '距60日线': f"{round(potential, 1)}%",
                '今日涨跌': f"{round(change, 1)}%"
            }
    except:
        return None
    return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 量价齐升版扫描开始... (当前时间: {now_shanghai.strftime('%Y-%m-%d %H:%M')})")

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
    
    # --- 输出诊断报告 (保留所有精细化统计) ---
    print("\n" + "="*50)
    print(f"📊 市场环境诊断报告 (未入选原因统计)")
    print("-" * 50)
    print(f"1. 扫描总数: {stats_dict['total_scanned']} 只")
    print(f"2. 股价低于 {MIN_PRICE}或换手率不符: {stats_dict['fail_price'] + stats_dict['fail_turnover']} 只")
    print(f"3. 距60日线空间不足 {MIN_PROFIT_POTENTIAL}%: {stats_dict['fail_potential']} 只")
    print(f"4. RSI/KDJ未达极致超跌: {stats_dict['fail_rsi_kdj']} 只")
    print(f"5. 成交量比不符合 {MIN_VOLUME_RATIO}-{MAX_VOLUME_RATIO}: {stats_dict['fail_volume']} 只")
    print(f"6. 非小阴小阳形态 (波动过大): {stats_dict['fail_shape']} 只")
    print("="*50)
        
    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values(by=['类型', '距60日线'], ascending=[True, False])
        print(f"\n🎯 选出结果 ({len(results)} 只):")
        print(df_result.to_string(index=False))
        
        os.makedirs("results", exist_ok=True)
        file_name = f"量价齐升版_{now_shanghai.strftime('%Y%m%d_%H%M')}.csv"
        df_result.to_csv(os.path.join("results", file_name), index=False, encoding='utf_8_sig')
    else:
        print("\n😱 诊断结果：未发现符合“量价齐升”或“超跌潜伏”的极品标的。")

if __name__ == "__main__":
    main()
