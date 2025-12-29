import pandas as pd
import os
import glob
from datetime import datetime

# --- 配置区 ---
RESULTS_DIR = 'results'
REPORT_DIR = 'reports'
# 战法名称翻译（对应你的 16 个文件夹名）
STRATEGY_NAMES = {
    'macd_bottom': 'MACD抄底',
    'duck_head': '老鸭头',
    'three_in_one': '三位一体',
    'pregnancy_line': '底部孕线',
    'single_yang': '单阳不破',
    'limit_pullback': '涨停回调',
    'golden_pit': '黄金坑',
    'grass_fly': '草上飞',
    'limit_break': '涨停破位',
    'double_plate': '阴阳双板',
    'horse_back': '洗盘回马枪',
    'hot_money': '游资回调',
    'wave_bottom': '波动抄底',
    'no_loss': '牛散不亏钱',
    'chase_rise': '高手追涨',
    'inst_swing': '机构波段'
}

def run_confluence_analysis():
    date_str = datetime.now().strftime('%Y-%m-%d')
    all_picks = []

    # 1. 扫描结果文件夹
    if not os.path.exists(RESULTS_DIR):
        print("未发现 results 目录，请先运行战法引擎。")
        return

    print(f"正在分析 {date_str} 的战法共振情况...")

    # 2. 读取每个战法产出的最新 CSV
    for folder_name, chinese_name in STRATEGY_NAMES.items():
        folder_path = os.path.join(RESULTS_DIR, folder_name)
        if not os.path.exists(folder_path):
            continue
            
        # 寻找当天的文件
        pattern = os.path.join(folder_path, f"{folder_name}_{date_str}.csv")
        files = glob.glob(pattern)
        
        for f in files:
            try:
                df = pd.read_csv(f, dtype={'code': str})
                if df.empty: continue
                # 记录每只股票属于哪个战法
                df['strategy'] = chinese_name
                all_picks.append(df)
            except Exception as e:
                print(f"读取 {f} 出错: {e}")

    if not all_picks:
        print("今日无任何战法选出股票。")
        return

    # 3. 合并所有结果
    full_df = pd.concat(all_picks, ignore_index=True)

    # 4. 计算共振强度 (Confluence Count)
    # 按代码和名称分组，统计出现了多少次
    confluence = full_df.groupby(['code', 'name']).agg({
        'strategy': lambda x: ' + '.join(list(x)),
        'price': 'last'
    }).reset_index()
    
    confluence['count'] = confluence['strategy'].apply(lambda x: len(x.split(' + ')))
    
    # 按共振次数降序排列
    confluence = confluence.sort_values(by='count', ascending=False)
    confluence.rename(columns={'strategy': '命中战法', 'count': '共振强度', 'price': '收盘价', 'code': '代码', 'name': '名称'}, inplace=True)

    # 5. 保存结果
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)

    # 保存 CSV
    csv_path = os.path.join(REPORT_DIR, f"confluence_{date_str}.csv")
    confluence.to_csv(csv_path, index=False, encoding='utf-8-sig')

    # 6. 生成 Markdown 复盘报告 (美化版)
    md_path = os.path.join(REPORT_DIR, f"report_{date_str}.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# 16战法共振复盘报告 ({date_str})\n\n")
        f.write(f"> 自动化系统已完成全市场扫描。今日共选出 **{len(confluence)}** 只目标股。\n\n")
        f.write("## 🏆 强共振候选 (2重及以上共振)\n\n")
        
        strong = confluence[confluence['共振强度'] >= 2]
        if not strong.empty:
            f.write(strong.to_markdown(index=False))
        else:
            f.write("今日暂无多重共振标的。")
            
        f.write("\n\n## 🔍 全量选股清单\n\n")
        f.write(confluence.to_markdown(index=False))

    print(f"✅ 分析完成！共振报告已生成至: {REPORT_DIR}")

if __name__ == "__main__":
    run_confluence_analysis()
