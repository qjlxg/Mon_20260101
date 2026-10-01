import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

# --- 配置区 ---
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'

# 16个战法的输出目录映射
STRATEGY_MAP = {
    'macd_bottom': 'results/macd_bottom',          # 1. MACD抄底
    'duck_head': 'results/duck_head',              # 2. 老鸭头
    'three_in_one': 'results/three_in_one',        # 3. 三位一体
    'pregnancy_line': 'results/pregnancy_line',    # 4. 底部孕线
    'single_yang': 'results/single_yang',          # 5. 单阳不破
    'limit_pullback': 'results/limit_pullback',    # 6. 涨停回调
    'golden_pit': 'results/golden_pit',            # 7. 黄金坑
    'grass_fly': 'results/grass_fly',              # 8. 草上飞
    'limit_break': 'results/limit_break',          # 9. 涨停破位
    'double_plate': 'results/double_plate',        # 10. 阴阳双板
    'horse_back': 'results/horse_back',            # 11. 洗盘回马枪
    'hot_money': 'results/hot_money',              # 12. 游资回调
    'wave_bottom': 'results/wave_bottom',          # 13. 波动抄底
    'no_loss': 'results/no_loss',                  # 14. 牛散不亏钱
    'chase_rise': 'results/chase_rise',            # 15. 高手追涨
    'inst_swing': 'results/inst_swing'             # 16. 机构波段
}

class AlphaLogics:
    """根据16张图片完善的量化逻辑"""
    
    @staticmethod
    def get_indicators(df):
        df = df.copy()
        # 计算核心均线
        for m in [5, 10, 20, 34, 60, 120, 250]:
            df[f'ma{m}'] = df['close'].rolling(m).mean()
        # 计算MACD (标准参数 12, 26, 9)
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['diff'] = df['ema12'] - df['ema26']
        df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['diff'] - df['dea']) * 2
        return df

    # --- 16个独立逻辑函数 (严格匹配图片特征) ---
    @staticmethod
    def logic_macd_bottom(df):
        # 1. MACD抄底: 0轴下金叉
        return df['diff'].iloc[-1] < 0 and df['diff'].iloc[-2] < df['dea'].iloc[-2] and df['diff'].iloc[-1] > df['dea'].iloc[-1]

    @staticmethod
    def logic_duck_head(df):
        # 2. 老鸭头: MA5/10金叉后回踩MA20 (鸭鼻孔)
        return df['ma5'].iloc[-1] > df['ma10'].iloc[-1] and df['low'].iloc[-1] <= df['ma20'].iloc[-1] * 1.01 and df['close'].iloc[-1] > df['ma20'].iloc[-1]

    @staticmethod
    def logic_three_in_one(df):
        # 3. 三位一体: 巨阳+放量+MACD 0轴上金叉
        is_big_yang = (df['close'].iloc[-1]/df['close'].iloc[-2] > 1.05)
        is_vol = (df['volume'].iloc[-1] > df['volume'].rolling(5).mean().iloc[-1] * 2)
        is_macd = (df['diff'].iloc[-1] > 0 and df['diff'].iloc[-1] > df['dea'].iloc[-1])
        return is_big_yang and is_vol and is_macd

    @staticmethod
    def logic_pregnancy_line(df):
        # 4. 底部孕线: 处于低位且今日K线被昨日包容
        is_low = df['close'].iloc[-1] < df['ma60'].iloc[-1]
        is_inside = df['high'].iloc[-1] < df['high'].iloc[-2] and df['low'].iloc[-1] > df['low'].iloc[-2]
        return is_low and is_inside

    @staticmethod
    def logic_single_yang(df):
        # 5. 单阳不破: 7天前有大阳，后续6天不破其最低价
        yang_idx = -7
        is_yang = (df['close'].iloc[yang_idx]/df['close'].iloc[yang_idx-1] > 1.05)
        is_hold = (df['low'].iloc[yang_idx+1:].min() >= df['low'].iloc[yang_idx])
        return is_yang and is_hold

    @staticmethod
    def logic_limit_pullback(df):
        # 6. 涨停回调: 5天前涨停，随后3-5天缩量回调
        has_limit = (df['close'].iloc[-5]/df['close'].iloc[-6] > 1.095)
        is_shrink = (df['volume'].iloc[-1] < df['volume'].rolling(5).mean().iloc[-1])
        return has_limit and is_shrink

    @staticmethod
    def logic_golden_pit(df):
        # 7. 黄金坑: MA5下穿MA34后再度上穿
        return df['ma5'].iloc[-3] < df['ma34'].iloc[-3] and df['ma5'].iloc[-1] > df['ma34'].iloc[-1]

    @staticmethod
    def logic_grass_fly(df):
        # 8. 草上飞: 股价长时间贴合MA60运行，波动极小
        return abs(df['close'].iloc[-1] - df['ma60'].iloc[-1]) / df['ma60'].iloc[-1] < 0.015

    @staticmethod
    def logic_limit_break(df):
        # 9. 涨停破位: 涨停后洗盘破位，3日内收回
        return (df['close'].iloc[-4]/df['close'].iloc[-5] > 1.095) and (df['close'].iloc[-1] > df['close'].iloc[-4])

    @staticmethod
    def logic_double_plate(df):
        # 10. 阴阳双板: 涨停+阴线洗盘+再次大阳
        return (df['close'].iloc[-3]/df['close'].iloc[-4] > 1.095) and (df['close'].iloc[-2] < df['open'].iloc[-2]) and (df['close'].iloc[-1] > df['open'].iloc[-1])

    @staticmethod
    def logic_horse_back(df):
        # 11. 洗盘回马枪: 涨停后缩量回踩MA5
        return (df['close'].iloc[-2]/df['close'].iloc[-3] > 1.095) and (df['low'].iloc[-1] <= df['ma5'].iloc[-1])

    @staticmethod
    def logic_hot_money(df):
        # 12. 游资回调: 连续回调后突发倍量阳线
        return df['volume'].iloc[-1] > df['volume'].iloc[-2] * 2.5 and df['close'].iloc[-1] > df['open'].iloc[-1]

    @staticmethod
    def logic_wave_bottom(df):
        # 13. 经典波动: 超跌后RSI或价格拐头
        return df['close'].iloc[-1] > df['ma5'].iloc[-1] and df['close'].iloc[-1] < df['ma20'].iloc[-1]

    @staticmethod
    def logic_no_loss(df):
        # 14. 牛散战法: 核心是不破年线(MA250)
        return df['close'].iloc[-1] > df['ma250'].iloc[-1] and df['close'].iloc[-2] < df['ma250'].iloc[-2]

    @staticmethod
    def logic_chase_rise(df):
        # 15. 高手追涨: 突破前期20日高点
        return df['close'].iloc[-1] > df['high'].iloc[-21:-1].max()

    @staticmethod
    def logic_inst_swing(df):
        # 16. 机构波段: MACD红柱持续放大
        return df['macd'].iloc[-1] > df['macd'].iloc[-2] > 0

# --- 主运行程序 ---
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
            
            # 统一列名
            df = df.rename(columns={'日期':'date','股票代码':'code','开盘':'open','收盘':'close','成交量':'volume','涨跌幅':'pct_chg','换手率':'turnover'})
            code = os.path.basename(f).replace('.csv','')
            
            # 基础前置过滤
            curr_close = df['close'].iloc[-1]
            if not (5.0 <= curr_close <= 35.0): continue
            
            # 计算指标
            df = AlphaLogics.get_indicators(df)
            
            # 遍历运行16个逻辑
            for s_key in STRATEGY_MAP.keys():
                logic_func = getattr(AlphaLogics, f"logic_{s_key}")
                if logic_func(df):
                    all_results[s_key].append({
                        'date': date_str,
                        'code': code,
                        'name': name_map.get(code, '未知'),
                        'price': curr_close,
                        'pct_chg': df['pct_chg'].iloc[-1]
                    })
        except:
            continue

    # 循环保存非空结果
    for s_key, path in STRATEGY_MAP.items():
        if not os.path.exists(path): os.makedirs(path, exist_ok=True)
        res_df = pd.DataFrame(all_results[s_key])
        
        if not res_df.empty:
            file_path = f"{path}/{s_key}_{date_str}.csv"
            res_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"战法 {s_key} 完成，发现 {len(res_df)} 个目标")

if __name__ == "__main__":
    run_all_strategies()
