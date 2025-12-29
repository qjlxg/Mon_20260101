import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# --- 配置区保持不变 ---
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
        for m in [5, 10, 20, 34, 60, 120, 250]:
            df[f'ma{m}'] = df['close'].rolling(m).mean()
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2
        return df

    # --- 核心逻辑：精准化调整 ---

    @staticmethod
    def logic_macd_bottom(df):
        # 0轴下二次金叉更精准
        return df['diff'].iloc[-1] < 0 and df['diff'].iloc[-2] < df['dea'].iloc[-2] and df['diff'].iloc[-1] > df['dea'].iloc[-1]

    @staticmethod
    def logic_duck_head(df):
        # 老鸭头：MA5/10在MA60上方，且今日缩量回踩
        is_up = df['ma5'].iloc[-1] > df['ma60'].iloc[-1]
        is_back = df['low'].iloc[-1] <= df['ma20'].iloc[-1] * 1.015 and df['close'].iloc[-1] > df['ma20'].iloc[-1]
        is_shrink = df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1] # 必须缩量
        return is_up and is_back and is_shrink

    @staticmethod
    def logic_three_in_one(df):
        # 必须是放量大阳线且MACD红柱刚开始放大
        return df['pct_chg'].iloc[-1] > 5 and df['volume'].iloc[-1] > df['volume'].iloc[-2] * 1.8 and df['macd'].iloc[-1] > df['macd'].iloc[-2]

    @staticmethod
    def logic_pregnancy_line(df):
        # 底部孕线：必须在低位，且今日成交量极度萎缩
        is_low = df['close'].iloc[-1] < df['ma20'].iloc[-1]
        is_inside = df['high'].iloc[-1] < df['high'].iloc[-2] and df['low'].iloc[-1] > df['low'].iloc[-2]
        is_extreme_shrink = df['volume'].iloc[-1] < df['volume'].rolling(10).mean().iloc[-1] * 0.7
        return is_low and is_inside and is_extreme_shrink

    @staticmethod
    def logic_single_yang(df):
        # 单阳不破：250日线之上的强势股，回调不破大阳低点
        if df['close'].iloc[-1] < df['ma250'].iloc[-1]: return False
        recent = df.iloc[-10:-1]
        if not any(recent['pct_chg'] > 7): return False
        yang_low = recent[recent['pct_chg'] > 7].iloc[-1]['low']
        return df['low'].iloc[-10:].min() >= yang_low * 0.99

    @staticmethod
    def logic_limit_pullback(df):
        # 涨停回调：回调过程必须是缩量的，且不能破MA20
        recent_limit = any(df['pct_chg'].iloc[-10:-2] > 9.5)
        is_pullback = all(df['pct_chg'].iloc[-3:-1] < 2)
        is_support = df['close'].iloc[-1] > df['ma20'].iloc[-1]
        return recent_limit and is_pullback and is_support

    @staticmethod
    def logic_golden_pit(df):
        # 黄金坑：快速下跌后V型反转，站上MA5
        return df['pct_chg'].iloc[-5:-2].sum() < -8 and df['pct_chg'].iloc[-1] > 3 and df['close'].iloc[-1] > df['ma5'].iloc[-1]

    @staticmethod
    def logic_grass_fly(df):
        # 草上飞：成交量极其均匀，股价死贴MA60
        vol_stable = df['volume'].iloc[-10:].std() / df['volume'].iloc[-10:].mean() < 0.3
        price_near = abs(df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1] < 0.01
        return vol_stable and price_near

    @staticmethod
    def logic_limit_break(df):
        # 涨停破位：涨停后跌破MA5，今日强势收回
        has_limit = any(df['pct_chg'].iloc[-5:-1] > 9.5)
        is_break = df['close'].iloc[-2] < df['ma5'].iloc[-2]
        is_recover = df['close'].iloc[-1] > df['ma5'].iloc[-1]
        return has_limit and is_break and is_recover

    @staticmethod
    def logic_double_plate(df):
        # 阴阳双板：涨停 + 缩量回调1-2天 + 再大阳
        return df['pct_chg'].iloc[-3] > 9.5 and df['volume'].iloc[-2] < df['volume'].iloc[-3] and df['pct_chg'].iloc[-1] > 5

    @staticmethod
    def logic_horse_back(df):
        # 回马枪：10日线精准支撑
        return df['pct_chg'].iloc[-3:-1].max() > 7 and df['low'].iloc[-1] <= df['ma10'].iloc[-1] * 1.01 and df['close'].iloc[-1] > df['ma10'].iloc[-1]

    @staticmethod
    def logic_hot_money(df):
        # 游资：长期低位后的首个倍量板
        is_low = df['close'].iloc[-1] < df['ma120'].iloc[-1] * 1.2
        is_first_vol = df['volume'].iloc[-1] > df['volume'].rolling(20).mean().iloc[-1] * 2.5
        return is_low and is_first_vol and df['pct_chg'].iloc[-1] > 4

    @staticmethod
    def logic_wave_bottom(df):
        # 波动抄底：必须超跌，MACD绿柱开始缩短
        is_oversold = df['close'].iloc[-1] < df['ma60'].iloc[-1] * 0.85
        macd_shorter = df['macd'].iloc[-1] > df['macd'].iloc[-2] and df['macd'].iloc[-1] < 0
        return is_oversold and macd_shorter

    @staticmethod
    def logic_no_loss(df):
        # 牛散：年线支撑且今日阳线
        return abs(df['low'].iloc[-1] - df['ma250'].iloc[-1]) / df['ma250'].iloc[-1] < 0.015 and df['pct_chg'].iloc[-1] > 0

    @staticmethod
    def logic_chase_rise(df):
        # 追涨：放量突破20日高点
        is_break = df['close'].iloc[-1] > df['high'].iloc[-21:-1].max()
        is_vol = df['volume'].iloc[-1] > df['volume'].iloc[-2] * 1.2
        return is_break and is_vol

    @staticmethod
    def logic_inst_swing(df):
        # 机构波段：MACD在0轴上方持续走强，且MA20斜率向上
        ma20_up = df['ma20'].iloc[-1] > df['ma20'].iloc[-2]
        macd_strong = df['macd'].iloc[-1] > df['macd'].iloc[-2] > 0
        return ma20_up and macd_strong

# --- 运行逻辑完全不变 ---
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
            curr_close = df['close'].iloc[-1]
            if not (5.0 <= curr_close <= 35.0): continue # 回归35元上限
            
            df = AlphaLogics.get_indicators(df)
            for s_key in STRATEGY_MAP.keys():
                logic_func = getattr(AlphaLogics, f"logic_{s_key}")
                if logic_func(df):
                    all_results[s_key].append({'date': date_str, 'code': code, 'name': name_map.get(code, '未知'), 'price': curr_close})
        except: continue

    for s_key, path in STRATEGY_MAP.items():
        if not os.path.exists(path): os.makedirs(path, exist_ok=True)
        res_df = pd.DataFrame(all_results[s_key])
        if not res_df.empty:
            res_df.to_csv(f"{path}/{s_key}_{date_str}.csv", index=False, encoding='utf-8-sig')
            print(f"战法 {s_key} 完成，发现 {len(res_df)} 个目标")

if __name__ == "__main__":
    run_all_strategies()
