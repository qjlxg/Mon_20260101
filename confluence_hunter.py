import pandas as pd
import os
import glob
from datetime import datetime

# 配置路径 - 保持不变
STRATEGIES = {
    'one_sun': 'results/one_sun',
    'macd_water': 'results/macd_water',
    'golden_pit': 'results/golden_pit',
    'duck_hunter': 'results/duck_hunter'
}

REPORT_PATH = 'results/confluence_report.csv'
HISTORY_DIR = 'history'
HISTORY_FILE = os.path.join(HISTORY_DIR, 'resonance_history.csv')

# 各战法的实战操作手册
OPERATIONS = {
    'one_sun': "【爆发位】一阳穿三线。次日看高开(1%-3%)，放量突破昨日最高价即是买点。",
    'macd_water': "【强势位】水上金叉。代表多头趋势延续。若股价贴近20日线可回吸。",
    'golden_pit': "【底部位】黄金坑企稳。适合底部轻仓潜伏，跌破坑底最低价止损。",
    'duck_hunter': "【波段位】老鸭头形态。鸭嘴张开是主升浪起点。止损设在鸭嘴下沿。"
}

def get_latest_file(folder):
    """获取文件夹内最新的CSV文件"""
    files = glob.glob(f"{folder}/*.csv")
    return max(files) if files else None

def main():
    all_picks = []
    
    # 1. 汇总今日各战法结果
    for name, path in STRATEGIES.items():
        latest = get_latest_file(path)
        if latest:
            try:
                df = pd.read_csv(latest)
                df['code'] = df['code'].astype(str).str.zfill(6)
                for _, row in df.iterrows():
                    all_picks.append({
                        'date': row.get('filter_date', datetime.now().strftime('%Y-%m-%d')),
                        'code': row['code'],
                        'name': row.get('name', '未知'),
                        'strategy': name,
                        'price': row.get('price', 0)
                    })
            except: continue

    if not all_picks:
        print("今日无选股结果。")
        return

    # 2. 生成今日共振报告
    df_all = pd.DataFrame(all_picks)
    # 合并同代码的战法
    today_report = df_all.groupby(['date', 'code', 'name']).agg({
        'strategy': lambda x: ','.join(x),
        'price': 'first'
    }).reset_index()
    
    today_report['resonance_count'] = today_report['strategy'].apply(lambda x: len(x.split(',')))
    
    # 注入操作建议
    def get_guide(strategies):
        guides = []
        for s in strategies.split(','):
            guides.append(f"[{s}]: {OPERATIONS.get(s, '')}")
        return " | ".join(guides)
    
    today_report['action_guide'] = today_report['strategy'].apply(get_guide)
    today_report = today_report.sort_values(by=['resonance_count', 'code'], ascending=[False, True])

    # 3. 战果统计 (复盘昨日)
    os.makedirs(HISTORY_DIR, exist_ok=True)
    performance_msg = "首次运行，暂无历史数据对比。"
    
    if os.path.exists(HISTORY_FILE):
        hist_df = pd.read_csv(HISTORY_FILE, dtype={'code': str})
        last_date = hist_df['date'].max()
        # 只有日期不同时才计算收益（避免同日运行覆盖）
        if last_date != today_report['date'].iloc[0]:
            last_picks = hist_df[hist_df['date'] == last_date].copy()
            # 拿今天所有票的最新价去对账
            merged = pd.merge(last_picks, today_report[['code', 'price']], on='code', suffixes=('_old', '_now'))
            if not merged.empty:
                merged['gain'] = ((merged['price_now'] - merged['price_old']) / merged['price_old'] * 100).round(2)
                avg_gain = merged['gain'].mean()
                win_rate = (len(merged[merged['gain'] > 0]) / len(merged)) * 100
                performance_msg = f"昨日精选今日平均涨幅: {avg_gain:.2f}% | 胜率: {win_rate:.1f}%"

    # 4. 更新历史总账 (存入 history/)
    if os.path.exists(HISTORY_FILE):
        full_history = pd.read_csv(HISTORY_FILE, dtype={'code': str})
        # 避免当天多次运行产生重复行
        full_history = full_history[full_history['date'] != today_report['date'].iloc[0]]
        full_history = pd.concat([full_history, today_report], ignore_index=True)
    else:
        full_history = today_report

    full_history.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

    # 5. 保存最新精选到 results/
    today_report.to_csv(REPORT_PATH, index=False, encoding='utf-8-sig')

    # 6. 强化版控制台输出
    print("\n" + "="*50)
    print(f"  大海捞鱼 - 复盘与选股报告 ({today_report['date'].iloc[0]})")
    print(f"  📈 {performance_msg}")
    print("="*50)

    lv3 = today_report[today_report['resonance_count'] >= 3]
    if not lv3.empty:
        print(f"💎 【今日鱼王 (3重共振+)】")
        for _, r in lv3.iterrows():
            print(f" >> {r['code']} | {r['name']} | 现价: {r['price']} | 战法: {r['strategy']}")
        print("-" * 30)
    
    lv2_count = len(today_report[today_report['resonance_count'] == 2])
    print(f"🔥 今日 2 重共振标的共: {lv2_count} 只")
    print(f"📂 完整报告: {REPORT_PATH}")
    print(f"📂 历史账本: {HISTORY_FILE}")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
