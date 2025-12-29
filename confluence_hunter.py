import pandas as pd
import os
from datetime import datetime
import glob

# 配置文件夹路径
STRATEGIES = {
    'one_sun': 'results/one_sun',
    'macd_water': 'results/macd_water',
    'golden_pit': 'results/golden_pit',
    'duck_hunter': 'results/duck_hunter'
}

OUTPUT_FILE = 'results/confluence_report.csv'

# 各战法的实战操作手册（写入报告）
OPERATIONS = {
    'one_sun': "【爆发位】次日关注高开(1%-3%)。若放量突破昨日高点可进场，止损设在阳线1/2处。",
    'macd_water': "【强势位】DIF在水上。若股价回踩20日线企稳可低吸，MACD红柱缩短需减仓。",
    'golden_pit': "【底部位】属于左侧潜伏。若今日放量阳线确认坑底，可轻仓试错，跌破坑底止损。",
    'duck_hunter': "【波段位】极品形态。鸭嘴张开时买入，止损设在鸭嘴下沿，目标主升浪。"
}

def get_latest_file(folder):
    """获取文件夹内最新的CSV文件"""
    files = glob.glob(f"{folder}/*.csv")
    if not files:
        return None
    return max(files, key=os.path.getctime)

def main():
    confluence_data = []
    
    # 1. 汇总所有战法的最新结果
    for name, path in STRATEGIES.items():
        latest_file = get_latest_file(path)
        if latest_file:
            try:
                df = pd.read_csv(latest_file)
                if not df.empty:
                    # 统一代码格式
                    df['code'] = df['code'].astype(str).str.zfill(6)
                    for _, row in df.iterrows():
                        confluence_data.append({
                            'code': row['code'],
                            'name': row.get('name', '未知'),
                            'strategy': name
                        })
            except Exception as e:
                print(f"解析 {latest_file} 出错: {e}")

    if not confluence_data:
        print("今日无任何战法选出股票。")
        return

    # 2. 统计共振频率
    all_df = pd.DataFrame(confluence_data)
    
    # 按代码分组，合并战法名称
    report = all_df.groupby(['code', 'name'])['strategy'].apply(list).reset_index()
    report['resonance_count'] = report['strategy'].apply(len)
    
    # 3. 关联操作方法
    def attach_op(strategies):
        ops = []
        for s in strategies:
            ops.append(f"[{s}]: {OPERATIONS[s]}")
        return "\n".join(ops)

    report['action_guide'] = report['strategy'].apply(attach_op)
    report['strategy'] = report['strategy'].apply(lambda x: ",".join(x))

    # 4. 排序：共振次数越多越靠前
    report = report.sort_values(by='resonance_count', ascending=False)

    # 5. 保存结果
    os.makedirs('results', exist_ok=True)
    report.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    # 6. 控制台打印精华
    print(f"\n{'='*40}")
    print(f"  共振筛选完成 - {datetime.now().strftime('%Y-%m-%d')}")
    print(f"{'='*40}")
    top_picks = report[report['resonance_count'] > 1]
    if not top_picks.empty:
        print(f"🔥 发现 {len(top_picks)} 只多维共振股票（高胜率）：")
        for _, r in top_picks.iterrows():
            print(f"代码: {r['code']} | 名称: {r['name']} | 共振数: {r['resonance_count']}")
    else:
        print("今日暂无共振股票，建议关注单项最强的标的。")
    print(f"{'='*40}\n报告已存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
