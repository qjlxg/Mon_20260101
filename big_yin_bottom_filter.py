import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# --- 配置参数 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results'

# 字段映射：请根据你的 CSV 实际列名进行调整
COL_MAP = {
    '日期': 'date', '开盘': 'open', '最高': 'high', 
    '最低': 'low', '收盘': 'close', '成交量': 'volume', '涨跌幅': 'pct_chg'
}

def check_big_yin_logic(df):
    """
    战法名称：大阴线黄金坑战法（精简版）
    包含形态：
    1. 牛回头：上升趋势中回踩10日线，缩量阴线。
    2. 物极必反：连续大跌后出现止跌信号（超跌反弹）。
    """
    if len(df) < 40: 
        return None
    
    # 提取数组以提高计算速度
    close = df['close'].values
    low = df['low'].values
    high = df['high'].values
    vol = df['volume'].values
    pct = df['pct_chg'].values
    open_p = df['open'].values

    # --- 基础过滤：排除高位股 (涨幅已超100%不碰，防追高) ---
    low_60 = df['low'].tail(60).min()
    if close[-1] > low_60 * 2.0: 
        return None

    # 计算均线
    ma10 = df['close'].rolling(10).mean().values

    # --- 形态一：支撑线上的“牛回头” ---
    # 逻辑：10日线上行，今日是大阴线(<-3%)，触碰10日线不破，且显著缩量
    if ma10[-1] > ma10[-2] and pct[-1] < -3.0:
        # 价格在10日均线附近（上下1%波动）
        if low[-1] <= ma10[-1] * 1.01 and close[-1] >= ma10[-1] * 0.99:
            avg_vol_5 = vol[-6:-1].mean()
            if vol[-1] < avg_vol_5 * 0.7:  # 缩量30%以上，代表抛压衰竭
                return "牛回头(回踩10MA)"

    # --- 形态二：连续大跌后的“物极必反” ---
    # 逻辑：最近4天累计跌幅 > 18%，且今日出现显著长下影线（恐慌盘出尽）
    last_4_pct = (close[-1] - close[-5]) / close[-5] * 100
    if last_4_pct < -18.0:
        # 计算下影线占比：下影线长度 / (最高-最低)
        # 防止除以0
        price_range = high[-1] - low[-1]
        if price_range > 0:
            shadow = (min(open_p[-1], close[-1]) - low[-1]) / price_range
            if shadow > 0.5:  # 下影线占全天波幅一半以上
                return "物极必反(超跌反弹)"

    return None

def process_stock(file_name):
    """单只股票处理逻辑"""
    code = file_name.split('.')[0]
    # 默认过滤创业板(30开头)，如需包含请删除此行
    if code.startswith('30'): 
        return None 
    
    file_path = os.path.join(DATA_DIR, file_name)
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns=COL_MAP)
        if df.empty or len(df) < 10: 
            return None
        
        # 统一处理百分比（去除 % 符号并转为 float）
        if 'pct_chg' in df.columns and df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
            
        signal = check_big_yin_logic(df)
        if signal:
            return {'code': code, 'type': signal}
    except Exception as e:
        # print(f"处理 {code} 时出错: {e}")
        return None
    return None

def main():
    # 1. 环境准备
    if not os.path.exists(NAMES_FILE):
        print(f"错误：找不到名称映射文件 {NAMES_FILE}")
        return
    
    if not os.path.exists(DATA_DIR):
        print(f"错误：找不到数据目录 {DATA_DIR}")
        return

    # 2. 加载名称并过滤 ST 股
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    # 过滤掉名称中带 ST 的股票
    valid_names_df = names_df[~names_df['name'].str.contains('ST|st', na=False)]
    valid_codes = set(valid_names_df['code'])

    # 3. 筛选待处理文件
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f.split('.')[0] in valid_codes]
    
    print(f"开始扫描 {len(files)} 只个股...")

    # 4. 多进程并行处理
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_stock, files))
    
    # 5. 过滤掉 None 结果
    found = [r for r in results if r is not None]
    
    # 6. 保存并输出结果
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    if not os.path.exists(month_dir):
        os.makedirs(month_dir)
        
    ts = now.strftime('%Y%m%d_%H%M%S')

    if found:
        res_df = pd.DataFrame(found)
        # 合并股票名称
        final_df = pd.merge(res_df, valid_names_df, on='code', how='left')
        file_path = os.path.join(month_dir, f'big_yin_bottom_{ts}.csv')
        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print("-" * 30)
        print(f"筛选完成！")
        print(f"发现机会：{len(final_df)} 只")
        print(f"结果已保存至：{file_path}")
        print("-" * 30)
    else:
        print("扫描完成，未发现符合条件的信号。")

if __name__ == '__main__':
    main()
