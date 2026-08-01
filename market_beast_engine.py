import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# --- 配置区 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
STRATEGY_MAP = {
    'macd_bottom': 'results/macd_bottom', 'duck_head': 'results/duck_head',
    'three_in_one': 'results/three_in_one', 'pregnancy_line': 'results/pregnancy_line',
    'single_yang': 'results/single_yang', 'limit_pullback': 'results/limit_pullback',
    'golden_pit': 'results/golden_pit', 'grass_fly': 'results/grass_fly',
    'limit_break': 'results/limit_break', 'double_plate': 'results/double_plate',
    'horse_back': 'results/horse_back', 'hot_money': 'results/hot_money',
    'wave_bottom': 'results/wave_bottom', 'no_loss': 'results/no_loss',
    'chase_rise': 'results/chase_rise', 'inst_swing': 'results/inst_swing'
}

class AlphaLogics:
    @staticmethod
    def get_indicators(df):
        df = df.copy()
        # 计算核心均线
        for m in [5, 10, 20, 34, 60, 120, 250]:
            df[f'ma{m}'] = df['close'].rolling(m).mean()
        # 计算MACD
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2
        return df

    # --- 终极逻辑：精细形态识别 ---

    @staticmethod
    def logic_macd_bottom(df):
        # 0轴下二次金叉（比普通金叉更精准）
        cond1 = df['diff'].iloc[-1] < 0 
        cond2 = df['diff'].iloc[-2] < df['dea'].iloc[-2] and df['diff'].iloc[-1] > df['dea'].iloc[-1]
        # 增加限制：红柱必须放量增长
        cond3 = df['macd'].iloc[-1] > df['macd'].iloc[-2]
        return cond1 and cond2 and cond3

    @staticmethod
    def logic_duck_head(df):
        # 老鸭头：MA5/10金叉，且股价在MA20附近缩量（鸭鼻孔）
        ma_up = df['ma5'].iloc[-1] > df['ma10'].iloc[-1] > df['ma60'].iloc[-1]
        vol_shrink = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]
        price_near = df['ma20'].iloc[-1] * 0.98 <= df['close'].iloc[-1] <= df['ma20'].iloc[-1] * 1.02
        return ma_up and vol_shrink and price_near

    @staticmethod
    def logic_three_in_one(df):
        # 巨阳+倍量+MACD金叉
        return df['pct_chg'].iloc[-1] > 5 and df['volume'].iloc[-1] > df['volume'].iloc[-2] * 1.8 and df['macd'].iloc[-1] > df['macd'].iloc[-2]

    @staticmethod
    def logic_pregnancy_line(df):
        # 底部孕线：K线实体变小，成交量极度萎缩
        is_inside = df['high'].iloc[-1] < df['high'].iloc[-2] and df['low'].iloc[-1] > df['low'].iloc[-2]
        vol_extreme = df['volume'].iloc[-1] < df['volume'].rolling(10).mean().iloc[-1] * 0.6
        return is_inside and vol_extreme

    @staticmethod
    def logic_single_yang(df):
        # 单阳不破：250日线以上，强阳后的平台整理
        if df['close'].iloc[-1] < df['ma250'].iloc[-1]: return False
        recent_yang = df.iloc[-10:-1][df['pct_chg'].iloc[-10:-1] > 6]
        if recent_yang.empty: return False
        yang_low = recent_yang.iloc[-1]['low']
        return df['low'].iloc[-10:].min() >= yang_low * 0.99

    @staticmethod
    def logic_limit_pullback(df):
        # 涨停回调：回调不破MA20
        has_limit = (df['pct_chg'].iloc[-10:-1] > 9.5).any()
        is_support = df['close'].iloc[-1] >= df['ma20'].iloc[-1]
        vol_down = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1]
        return has_limit and is_support and vol_down

    @staticmethod
    def logic_golden_pit(df):
        # 黄金坑：下跌后的首根反转放量阳线
        is_down = df['close'].iloc[-10] > df['close'].iloc[-3] * 1.08
        is_rebound = df['pct_chg'].iloc[-1] > 3 and df['volume'].iloc[-1] > df['volume'].iloc[-2]
        return is_down and is_rebound

    @staticmethod
    def logic_grass_fly(df):
        # 草上飞：成交量极其平稳，振幅极小
        vol_std = df['volume'].iloc[-10:].std() / df['volume'].iloc[-10:].mean() < 0.2
        price_flat = abs(df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1] < 0.01
        return vol_std and price_flat

    @staticmethod
    def logic_limit_break(df):
        # 涨停破位：破MA5后，今日大阳收复
        return (df['pct_chg'].iloc[-5:-1] > 9.5).any() and df['close'].iloc[-2] < df['ma5'].iloc[-2] and df['close'].iloc[-1] > df['ma5'].iloc[-1]

    @staticmethod
    def logic_double_plate(df):
        # 阴阳双板：涨停+回调+反包
        return df['pct_chg'].iloc[-3] > 9.5 and df['close'].iloc[-2] < df['open'].iloc[-2] and df['pct_chg'].iloc[-1] > 5

    @staticmethod
    def logic_horse_back(df):
        # 回马枪：10日线精准支撑，缩量
        return df['pct_chg'].iloc[-5:-1].max() > 7 and abs(df['low'].iloc[-1] - df['ma10'].iloc[-1]) / df['ma10'].iloc[-1] < 0.01

    @staticmethod
    def logic_hot_money(df):
        # 游资：倍量+突发突破
        return df['volume'].iloc[-1] > df['volume'].rolling(20).mean().iloc[-1] * 2.5 and df['pct_chg'].iloc[-1] > 4

    @staticmethod
    def logic_wave_bottom(df):
        # 波动：超跌（偏离度指标）
        bias = (df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1]
        return bias < -0.15 and df['pct_chg'].iloc[-1] > 0

    @staticmethod
    def logic_no_loss(df):
        # 牛散：年线支撑阳线
        return abs(df['low'].iloc[-1] - df['ma250'].iloc[-1]) / df['ma250'].iloc[-1] < 0.01 and df['close'].iloc[-1] > df['open'].iloc[-1]

    @staticmethod
    def logic_chase_rise(df):
        # 追涨：突破前高且MA20斜率向上
        is_break = df['close'].iloc[-1] > df['high'].iloc[-20:-1].max()
        ma_up = df['ma20'].iloc[-1] > df['ma20'].iloc[-2]
        return is_break and ma_up

    @staticmethod
    def logic_inst_swing(df):
        # 机构：MACD红柱连续3日增长，且均线多头
        return all(df['macd'].iloc[-3:] > 0) and df['macd'].iloc[-1] > df['macd'].iloc[-2] > df['macd'].iloc[-3]

# --- 执行区 ---
def run_all_strategies():
    name_map = {}
    if os.path.exists(NAMES_FILE):
        name_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        name_map = dict(zip(name_df['code'], name_df['name']))

    files = glob.glob(f"{DATA_DIR}/*.csv")
    date_str = datetime.now().strftime('%Y-%m-%d')
    all_results = {k: [] for k in STRATEGY_MAP.keys()}

    for f in files:
        try:
            df = pd.read_csv(f)
            if len(df) < 250: continue
            df = df.rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','涨跌幅':'pct_chg','换手率':'turnover'})
            code = os.path.basename(f).replace('.csv','')
            
            # 基础通用过滤
            curr_c = df['close'].iloc[-1]
            if not (5.0 <= curr_c <= 35.0): continue
            
            df = AlphaLogics.get_indicators(df)
            for s_key in STRATEGY_MAP.keys():
                if getattr(AlphaLogics, f"logic_{s_key}")(df):
                    all_results[s_key].append({'date': date_str, 'code': code, 'name': name_map.get(code, '未知'), 'price': curr_c})
        except: continue

    # 保存
    for s_key, path in STRATEGY_MAP.items():
        if not os.path.exists(path): os.makedirs(path, exist_ok=True)
        res_df = pd.DataFrame(all_results[s_key])
        if not res_df.empty:
            res_df.to_csv(f"{path}/{s_key}_{date_str}.csv", index=False, encoding='utf-8-sig')
            print(f"战法 {s_key} 完成，发现 {len(res_df)} 个目标")

if __name__ == "__main__":
    run_all_strategies()
