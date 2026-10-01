import os
import pandas as pd
import akshare as ak
import time
from datetime import datetime
import sys

# 配置路径
DATA_DIR = "stock_data"
FILTERED_LIST_PATH = os.path.join(DATA_DIR, "filtered_stock_list.csv")
CHECKPOINT_PATH = os.path.join(DATA_DIR, "checkpoint.txt") 

COLUMN_MAPPING = {
    "日期": "日期", "开盘": "开盘", "收盘": "收盘", "最高": "最高",
    "最低": "最低", "成交量": "成交量", "成交额": "成交额",
    "振幅": "振幅", "涨跌幅": "涨跌幅", "涨跌额": "涨跌额", "换手率": "换手率"
}
TARGET_COLUMNS = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']

def download_item(symbol_short):
    """处理单个股票的增量下载"""
    file_path = os.path.join(DATA_DIR, f"{symbol_short}.csv")
    try:
        existing_dates = set()
        start_date = "19900101"
        
        # 1. 检查本地数据，获取增量起始点
        if os.path.exists(file_path):
            try:
                existing_df = pd.read_csv(file_path)
                if not existing_df.empty:
                    existing_dates = set(existing_df['日期'].astype(str).tolist())
                    # 取最后一行日期，去除横杠作为接口起始时间
                    last_date = str(existing_df.iloc[-1]['日期']).replace("-", "")
                    start_date = last_date
            except Exception as e:
                print(f"读取旧文件失败 {symbol_short}, 重新全量下载: {e}")

        # 2. 调用 akshare 接口
        df = ak.stock_zh_a_hist(symbol=symbol_short, period="daily", start_date=start_date, adjust="")
        
        if df is not None and not df.empty:
            df = df.rename(columns=COLUMN_MAPPING)
            df['股票代码'] = symbol_short
            df['日期'] = df['日期'].astype(str)
            
            # 3. 严格去重：只保留本地不存在的日期
            df = df[~df['日期'].isin(existing_dates)]
            
            if not df.empty:
                # 格式化数据
                df['成交额'] = df['成交额'].round(1)
                for col in ['开盘', '收盘', '最高', '最低', '振幅', '涨跌幅', '涨跌额', '换手率']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                df['成交量'] = df['成交量'].astype(int)
                
                df = df[TARGET_COLUMNS]
                header = not os.path.exists(file_path)
                # 追加模式写入 CSV
                df.to_csv(file_path, mode='a', index=False, header=header, encoding='utf-8')
        
        time.sleep(0.2) # 接口保护频控
        return True
    except Exception as e:
        print(f"下载异常 {symbol_short}: {e}")
        return False

def main():
    # 确保目录存在
    if not os.path.exists(DATA_DIR): 
        os.makedirs(DATA_DIR)
    
    # 初始化断点文件（防止 Git 提交报错）
    if not os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, 'w') as f: f.write('0')

    if not os.path.exists(FILTERED_LIST_PATH):
        print("错误: 找不到名单文件 filtered_stock_list.csv")
        sys.exit(1)

    # 读取名单
    df_list = pd.read_csv(FILTERED_LIST_PATH)
    symbols = df_list['代码'].astype(str).str.zfill(6).tolist()

    # 读取进度断点
    try:
        with open(CHECKPOINT_PATH, 'r') as f:
            start_index = int(f.read().strip())
    except:
        start_index = 0

    print(f"📊 当前下载进度: {start_index}/{len(symbols)}")

    if start_index >= len(symbols):
        print("✅ 所有数据已下载完成，重置进度。")
        with open(CHECKPOINT_PATH, 'w') as f: f.write('0')
        return

    # 按顺序执行下载
    for i in range(start_index, len(symbols)):
        success = download_item(symbols[i])
        if success:
            # 每成功一只，实时更新断点
            with open(CHECKPOINT_PATH, 'w') as f:
                f.write(str(i + 1))
        else:
            # 失败则打印当前代码并退出，由 Workflow 触发重试
            print(f"🛑 任务中断于 index {i} (代码: {symbols[i]})")
            sys.exit(1)

    print("🎉 本轮下载任务顺利执行完毕。")

if __name__ == "__main__":
    main()
