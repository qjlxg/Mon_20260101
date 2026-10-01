import pandas as pd
import os
import glob
from datetime import datetime

# --- 配置区 ---
RESULTS_DIR = 'results'
REPORT_DIR = 'reports'

# 16个战法目录与中文名称的对应关系
STRATEGY_MAP = {
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

def run_confluence_hunter():
    # 获取今天的日期字符串
    date_str = datetime.now().strftime('%Y-%m-%d')
    all_data = []

    print(f"开始汇总 {date_str} 的 16 战法筛选结果...")

    # 1. 遍历 16 个战法文件夹
    for s_key, s_name in STRATEGY_MAP.items():
        folder_path = os.path.join(RESULTS_DIR, s_key)
        if not os.path.exists(folder_path):
            continue
            
        # 查找当天的结果文件，例如：results/macd_bottom/macd_bottom_2025-12-29.csv
        file_pattern = os.path.join(folder_path, f"{s_key}_{date_str}.csv")
        target_files = glob.glob(file_pattern)
        
        for f in target_files:
            try:
                df = pd.read_csv(f, dtype={'code': str})
                if df.empty:
                    continue
                # 标记该股票所属的战法名称
                df['match_strategy'] = s_name
                all_data.append(df)
            except Exception as e:
                print(f"读取文件 {f} 出错: {e}")

    # 2. 如果没有选出任何股票，直接退出
    if not all_data:
        print(f"今日 ({date_str}) 16 个战法均未选出目标。")
        return

    # 3. 合并所有战法选出的股票
    full_df = pd.concat(all_data, ignore_index=True)

    # 4. 核心逻辑：统计每只股票出现的次数（共振强度）
    # 分组统计：代码、名称、价格
    summary = full_df.groupby(['code', 'name']).agg({
        'match_strategy': lambda x: ' + '.join(list(x)), # 合并所有命中的战法名
        'price': 'last' # 记录价格
    }).reset_index()

    # 计算共振次数
    summary['count'] = summary['match_strategy'].apply(lambda x: len(x.split(' + ')))
    
    # 按共振强度从高到低排序
    summary = summary.sort_values(by='count', ascending=False)
    
    # 重命名列名以便阅读
    summary.columns = ['股票代码', '股票名称', '命中战法汇总', '最新价', '共振强度']

    # 5. 保存汇总结果
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)

    # 保存为 CSV 方便下载
    csv_output = os.path.join(REPORT_DIR, f"confluence_{date_str}.csv")
    summary.to_csv(csv_output, index=False, encoding='utf-8-sig')

    # 6. 生成 Markdown 复盘报告
    md_output = os.path.join(REPORT_DIR, f"report_{date_str}.md")
    with open(md_output, 'w', encoding='utf-8') as f:
        f.write(f"# 16 战法共振复盘报告 ({date_str})\n\n")
        f.write(f"今日全市场扫描完成。共有 **{len(summary)}** 只股票入选战法池。\n\n")
        
        # 提取多重共振的股票（共振强度 >= 2）
        strong_signals = summary[summary['共振强度'] >= 2]
        
        f.write("## 🚀 强共振提醒 (多重战法指向)\n")
        if not strong_signals.empty:
            f.write(strong_signals.to_markdown(index=False))
        else:
            f.write("今日暂无双重及以上共振的标的。\n")
            
        f.write("\n\n## 📋 全量入选清单 (按强度排序)\n")
        f.write(summary.to_markdown(index=False))

    print(f"✅ 汇总完成！共振报告已生成至: {md_output}")

if __name__ == "__main__":
    run_confluence_hunter()
