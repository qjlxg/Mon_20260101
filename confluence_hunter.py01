import pandas as pd
import os
import glob
from datetime import datetime

# 策略路径配置
STRATEGIES = {
    'one_sun': 'results/one_sun',
    'macd_water': 'results/macd_water',
    'golden_pit': 'results/golden_pit',
    'duck_hunter': 'results/duck_hunter'
}

REPORT_PATH = 'results/confluence_report.csv'
HISTORY_DIR = 'history'
HISTORY_FILE = os.path.join(HISTORY_DIR, 'resonance_history.csv')
STATS_FILE = os.path.join(HISTORY_DIR, 'overall_stats.txt')

# 操作指南
OPERATIONS = {
    'one_sun': "【爆发位】一阳穿三线。次日看高开(1%-3%)，放量突破昨日最高价即是买点。",
    'macd_water': "【强势位】水上金叉。代表多头趋势延续。若股价贴近20日线可回吸。",
    'golden_pit': "【底部位】黄金坑企稳。适合底部轻仓潜伏，跌破坑底最低价止损。",
    'duck_hunter': "【波段位】老鸭头形态。鸭嘴张开是主升浪起点。止损设在鸭嘴下沿。"
}

def get_latest_file(folder):
    files = glob.glob(f"{folder}/*.csv")
    return max(files) if files else None

def get_total_gain():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'r', encoding='utf-8') as f:
            try:
                content = f.read().strip()
                return float(content) if content else 0.0
            except:
                return 0.0
    return 0.0

def save_total_gain(gain):
    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        f.write(f"{gain:.2f}")

def generate_reports(df, total_gain, perf_msg, date_str):
    """生成中文版 Markdown 报告和每日独立 CSV 备份"""
    # 1. 准备显示数据
    cn_df = df.copy()
    columns_map = {
        'date': '日期', 'code': '股票代码', 'name': '股票名称',
        'strategy': '触发战法', 'price': '当前价格',
        'resonance_count': '共振强度', 'action_guide': '操作指南'
    }
    cn_df = cn_df.rename(columns=columns_map)
    display_cols = ['日期', '股票代码', '股票名称', '共振强度', '当前价格', '触发战法', '操作指南']
    # 强制按共振强度降序排列
    cn_df = cn_df[display_cols].sort_values(by='共振强度', ascending=False)

    # 2. 生成 Markdown 内容
    md_content = f"# 🌊 大海捞鱼 - 共振精选报告 ({date_str})\n\n"
    md_content += f"### 📈 战果复盘\n"
    md_content += f"- **最近表现**: {perf_msg}\n"
    md_content += f"- **系统累计总收益率**: `{total_gain:.2f}%` 🚀\n\n"
    md_content += f"### 💎 今日精选 (3重共振及以上优先)\n"
    md_content += cn_df.to_markdown(index=False)
    md_content += f"\n\n---\n*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"

    # 3. 保存文件
    # 根目录显示方式
    with open("显示方式.md", 'w', encoding='utf-8') as f:
        f.write(md_content)
    # history 目录下的日期备份 md
    with open(os.path.join(HISTORY_DIR, f"confluence_hunter_{date_str}.md"), 'w', encoding='utf-8') as f:
        f.write(md_content)
    # history 目录下的日期备份 csv
    df.to_csv(os.path.join(HISTORY_DIR, f"confluence_hunter_history_{date_str}.csv"), index=False, encoding='utf-8-sig')

def main():
    all_picks = []
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

    df_all = pd.DataFrame(all_picks)
    today_report = df_all.groupby(['date', 'code', 'name']).agg({
        'strategy': lambda x: ','.join(x),
        'price': 'first'
    }).reset_index()
    
    today_report['resonance_count'] = today_report['strategy'].apply(lambda x: len(x.split(',')))
    today_report['action_guide'] = today_report['strategy'].apply(
        lambda x: " | ".join([f"[{s}]: {OPERATIONS.get(s, '')}" for s in x.split(',')])
    )
    
    # 收益对账逻辑
    os.makedirs(HISTORY_DIR, exist_ok=True)
    perf_msg = "首次运行或今日无新对账数据。"
    total_gain = get_total_gain()
    
    if os.path.exists(HISTORY_FILE):
        hist_df = pd.read_csv(HISTORY_FILE, dtype={'code': str})
        if not hist_df.empty:
            last_date = hist_df['date'].max()
            if last_date != today_report['date'].iloc[0]:
                last_picks = hist_df[hist_df['date'] == last_date].copy()
                merged = pd.merge(last_picks, today_report[['code', 'price']], on='code', suffixes=('_old', '_now'))
                if not merged.empty:
                    daily_gain = ((merged['price_now'] - merged['price_old']) / merged['price_old'] * 100).mean()
                    total_gain += daily_gain
                    save_total_gain(total_gain)
                    perf_msg = f"昨日精选今日平均涨幅: {daily_gain:.2f}%"

    # 保存总账
    if os.path.exists(HISTORY_FILE):
        full_hist = pd.read_csv(HISTORY_FILE, dtype={'code': str})
        full_hist = pd.concat([full_hist[full_hist['date'] != today_report['date'].iloc[0]], today_report], ignore_index=True)
    else:
        full_hist = today_report
    full_hist.to_csv(HISTORY_FILE, index=False, encoding='utf-8-sig')

    # 生成 MD 视图和日期备份
    date_str = today_report['date'].iloc[0]
    generate_reports(today_report, total_gain, perf_msg, date_str)
    
    # 保持 results 下的最新报告
    today_report.to_csv(REPORT_PATH, index=False, encoding='utf-8-sig')
    print(f"✅ 处理完成: {date_str}")

if __name__ == "__main__":
    main()
