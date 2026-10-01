import os
import akshare as ak
import pandas as pd

DATA_DIR = "stock_data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

RAW_LIST_PATH = os.path.join(DATA_DIR, "raw_stock_list.csv")
FILTERED_LIST_PATH = os.path.join(DATA_DIR, "filtered_stock_list.csv")

def main():
    print("正在获取 A 股实时名单...")
    # 获取全量行情
    df = ak.stock_zh_a_spot_em()
    df.to_csv(RAW_LIST_PATH, index=False, encoding='utf-8-sig')
    
    # --- 过滤逻辑升级 ---
    # 1. 排除 ST (包含 *ST)
    df = df[~df['名称'].str.contains("ST", na=False)]
    
    # 2. 排除 30 开头 (创业板)
    df = df[~df['代码'].astype(str).str.startswith("30")]
    
    # 3. 排除北交所 (代码以 8, 9, 4 开头)
    df = df[~df['代码'].astype(str).str.startswith(('8', '9', '4'))]
    
    # 4. 只要深沪 A 股 (逻辑上由上述排除后剩下 60, 00, 688 开头为主)
    # 5. 价格过滤: 5.0 <= 最新价 <= 20.0
    df = df[(df['最新价'] >= 5.0) & (df['最新价'] <= 20.0)]
    
    # 转换代码格式适配 yfinance (用于下载脚本读取)
    def format_code(c):
        c_str = str(c).zfill(6)
        return f"{c_str}.SS" if c_str.startswith('6') else f"{c_str}.SZ"
    
    df['yf_code'] = df['代码'].apply(format_code)
    
    # 保存精简名单
    df.to_csv(FILTERED_LIST_PATH, index=False, encoding='utf-8-sig')
    print(f"名单处理完成。")
    print(f"- 原始股数: {len(pd.read_csv(RAW_LIST_PATH))}")
    print(f"- 精简后股数: {len(df)}")
    print(f"精简名单已保存至: {FILTERED_LIST_PATH}")

if __name__ == "__main__":
    main()
