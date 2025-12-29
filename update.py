import akshare as ak
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# --- 定义常量 ---
DATA_DIR = 'stock_data'
RESULTS_DIR = 'results_data_update' # 用于保存进度文件
PROGRESS_FILE = os.path.join(RESULTS_DIR, 'progress.txt')
LAST_RUN_DATE_FILE = os.path.join(RESULTS_DIR, 'last_run_date.txt')
MAX_WORKERS = 15 # 并发线程数

# 修改：定义股票列表文件路径为文本文件 (列表.txt)
STOCK_LIST_FILE = '列表.txt' 

# 创建结果保存目录和数据保存目录
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
    print(f"创建数据目录: {DATA_DIR}")

# 修改后的函数：从 TXT 文件（制表符分隔）获取股票列表
def get_stock_list():
    
    print(f"正在读取股票列表文件: {STOCK_LIST_FILE}...")
    
    # 检查文件是否存在
    if not os.path.exists(STOCK_LIST_FILE):
        print(f"致命错误：未找到股票列表文件: {STOCK_LIST_FILE}")
        return pd.DataFrame() 

    try:
        # 尝试读取 TXT 文件，使用制表符作为分隔符
        # dtype={'code': str} 确保代码列一开始就被视为字符串
        stock_info_df = pd.read_csv(STOCK_LIST_FILE, sep='\t', encoding='utf-8', dtype={'code': str})
    except Exception as e:
        print(f"致命错误：读取股票列表文件失败 (请检查文件是否存在、格式是否正确，并确保为制表符分隔): {e}")
        return pd.DataFrame()

    if stock_info_df.empty:
        print("警告: 股票列表文件为空。")
        return pd.DataFrame()
    
    # 确保列名为 '代码' 和 '名称'
    col_mapping = {
        'code': '代码', 'name': '名称', 
        'symbol': '代码'
    }
    
    # 进行列名映射和重命名，以兼容不同的列名
    for old, new in col_mapping.items():
        if old in stock_info_df.columns and new not in stock_info_df.columns:
            stock_info_df = stock_info_df.rename(columns={old: new}, inplace=False)
            
    if '代码' in stock_info_df.columns and '名称' in stock_info_df.columns:
        # 仅保留 '代码' 和 '名称' 列
        # 确保 '代码' 是字符串类型，移除可能的 .0 后缀 (例如 600000.0)
        stock_info_df['代码'] = stock_info_df['代码'].astype(str).str.replace(r'\.0$', '', regex=True)
        
        # ADDED: 强制填充前导零，确保代码始终为 6 位字符串 (FIX 1)
        stock_info_df['代码'] = stock_info_df['代码'].str.zfill(6)
        
        print(f"成功读取 {len(stock_info_df)} 只股票信息。")
        return stock_info_df[['代码', '名称']]
    else:
        print("警告: 无法在列表中找到 '代码' 和 '名称' (或其兼容名称如'code','name','symbol') 列。")
        return pd.DataFrame()
    
# 保存或增量更新单只股票的历史数据 (并发目标函数)
def save_and_update_stock_data(stock_code, stock_name, max_retries=5):
    """
    保存或增量更新单只股票的历史数据。
    返回 True 表示成功或已最新，False 表示失败。
    """
    # FIX 1 (副作用): 由于 get_stock_list 已修正，这里的 stock_code 已经是一个 6 位字符串。
    file_path = os.path.join(DATA_DIR, f"{stock_code}.csv")
    
    start_date_str = "19900101"
    existing_df = pd.DataFrame()
    
    # --- 尝试读取本地数据，确定增量更新的起始日期 ---
    if os.path.exists(file_path):
        try:
            # 使用 on_bad_lines='skip' 避免文件格式问题中断
            existing_df = pd.read_csv(file_path, parse_dates=['日期'], on_bad_lines='skip')
            existing_df.sort_values(by='日期', inplace=True)
            
            if not existing_df.empty and '日期' in existing_df.columns:
                last_date_obj = existing_df['日期'].iloc[-1]
                last_date_str = last_date_obj.strftime('%Y%m%d')
                today_str = datetime.now().strftime('%Y%m%d')
                
                # 如果数据已是最新 (今天)
                if last_date_str == today_str:
                    return (True, 0) # 0 表示跳过更新 (已是最新)

                # 设置增量更新的起始日期为本地数据的后一天
                start_date_obj = last_date_obj + timedelta(days=1)
                start_date_str = start_date_obj.strftime('%Y%m%d')

        except Exception as e:
            # 如果文件读取失败，将尝试全量下载
            existing_df = pd.DataFrame()
            
    # --- 循环尝试下载数据 ---
    for attempt in range(max_retries):
        try:
            current_end_date = datetime.now().strftime('%Y%m%d')
            
            # 使用 6 位代码进行 API 调用
            new_data_df = ak.stock_zh_a_hist(
                symbol=stock_code, 
                period="daily", 
                start_date=start_date_str, 
                end_date=current_end_date, 
                adjust="qfq"
            )
            
            if new_data_df.empty:
                return (True, 0) # 没有新数据，也算成功
            
            # FIX 2: 插入 6 位股票代码列到 DataFrame (修复 CSV 内容中的代码丢失前导零)
            # akshare 数据默认不含股票代码列，且股票代码在 CSV 中位于 '日期' 列之后 (索引 1)
            # 确保插入的列名与 CSV 文件中的列名一致
            if '股票代码' not in new_data_df.columns:
                 new_data_df.insert(1, '股票代码', stock_code) 

            new_data_df['日期'] = pd.to_datetime(new_data_df['日期'])
            new_data_df.sort_values(by='日期', inplace=True)
            
            records_count = 0
            
            if existing_df.empty:
                # 全量下载或文件损坏后重新下载
                new_data_df.to_csv(file_path, index=False, encoding='utf-8')
                records_count = len(new_data_df)
            else:
                # 增量追加数据
                new_data_df = new_data_df[new_data_df['日期'] > existing_df['日期'].iloc[-1]]
                
                if not new_data_df.empty:
                    # 注意：如果 new_data_df 在上面的 insert 步骤中没有 '股票代码' 列，这里会导致列错位。
                    # 由于我们已经强制插入了 '股票代码' 列 (FIX 2)，这里是安全的。
                    new_data_df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8')
                    records_count = len(new_data_df)
            
            return (True, records_count) # (成功状态, 更新的记录数)
            
        except Exception as e:
            # 遇到连接错误等，进行重试
            if attempt < max_retries - 1:
                # 避免并发输出干扰，只在日志中记录
                time.sleep(3) 
            else:
                return (False, 0) # 失败

    return (False, 0)

# 主函数
def main():
    
    # ----------------------------------------
    # 1. 检查和加载进度
    # ----------------------------------------
    start_index = 0
    today_str = datetime.now().strftime('%Y-%m-%d')
    BATCH_SIZE = 15 # 每批次处理的股票数量
    
    last_run_date = None
    if os.path.exists(LAST_RUN_DATE_FILE):
        with open(LAST_RUN_DATE_FILE, 'r') as f:
            try:
                last_run_date = f.read().strip()
            except:
                pass

    if last_run_date != today_str:
        # 非今日运行记录：重置进度
        print("检测到非今日运行记录或进度文件缺失，重置进度。")
        if os.path.exists(PROGRESS_FILE):
             os.remove(PROGRESS_FILE)
        start_index = 0
    else:
        # 今日运行记录：加载进度
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                try:
                    start_index = int(f.read().strip())
                    print(f"检测到今日运行进度，将从第 {start_index + 1} 只股票开始更新。")
                except ValueError:
                    os.remove(PROGRESS_FILE)
                    print("进度文件损坏，将从头开始分析。")
                    start_index = 0

    stock_list = get_stock_list()
    if stock_list.empty:
        print("致命错误：无法获取股票列表，程序退出。")
        return

    # ----------------------------------------
    # 2. 股票过滤 (ST, GEM, STAR)
    # ----------------------------------------
    # FIX 1 保证了 stock_list['代码'] 已经是 6 位字符串
    stock_list['代码'] = stock_list['代码'].astype(str)
    
    # 排除 ST/退市 股票
    stock_list = stock_list.drop(
        stock_list[stock_list['名称'].str.lower().str.contains(r'[s\*]t|退', na=False)].index
    )
    
    # 排除 创业板(300/301开头) 和 科创板(688开头) 股票
    stock_list = stock_list.drop(
        stock_list[
            stock_list['代码'].str.startswith('300') | 
            stock_list['代码'].str.startswith('301') |
            stock_list['代码'].str.startswith('688')
        ].index
    )
    
    total_stocks = len(stock_list)
    
    # ----------------------------------------
    # 3. 设置分段处理范围并检查是否完成
    # ----------------------------------------
    END_INDEX = min(start_index + BATCH_SIZE, total_stocks)
    
    if start_index >= total_stocks:
        print("所有股票已更新完毕。任务结束。")
        if os.path.exists(PROGRESS_FILE):
             os.remove(PROGRESS_FILE)
        # 记录本次运行的日期
        with open(LAST_RUN_DATE_FILE, 'w') as f:
            f.write(today_str)
        return

    print(f"共获取到 {total_stocks} 只股票 (排除ST、退市、创业板、科创板后)")
    print(f"本次任务范围: 更新 {start_index + 1} 到 {END_INDEX} 只股票。")
    print(f"使用 {MAX_WORKERS} 个线程并发处理...")
    
    current_batch = stock_list.iloc[start_index:END_INDEX]
    
    # ----------------------------------------
    # 4. 使用 ThreadPoolExecutor 进行并发处理
    # ----------------------------------------
    success_count = 0
    fail_count = 0
    records_updated = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {
            executor.submit(save_and_update_stock_data, row['代码'], row['名称']): (row['代码'], row['名称'])
            for _, row in current_batch.iterrows()
        }
        
        for future in tqdm(as_completed(future_to_stock), total=len(current_batch), desc="数据更新进度"):
            code, name = future_to_stock[future]
            try:
                is_success, count = future.result()
                if is_success:
                    success_count += 1
                    records_updated += count
                else:
                    fail_count += 1
                    print(f"\n[更新失败] 股票代码: {code}，名称: {name}。")
            except Exception as e:
                fail_count += 1
                print(f"\n[任务异常] 股票代码: {code}，名称: {name}，异常: {e}")

    # ----------------------------------------
    # 5. 进度保存和退出
    # ----------------------------------------
    print("\n--- 批次更新结果 ---")
    print(f"✅ 成功更新/已是最新股票数: {success_count} (共新增/更新 {records_updated} 条记录)")
    print(f"❌ 失败股票数: {fail_count}")

    next_start_index = END_INDEX
    
    # 记录本次运行的日期
    with open(LAST_RUN_DATE_FILE, 'w') as f:
        f.write(today_str)
    
    if next_start_index < total_stocks:
        with open(PROGRESS_FILE, 'w') as f:
            f.write(str(next_start_index))
        print(f"本次任务完成。进度已保存到 {PROGRESS_FILE}，下次将从第 {next_start_index + 1} 只股票继续。")
        # 退出码 99 通知工作流重启
        exit(99) 
    else:
        # 任务全部完成后的处理
        if os.path.exists(PROGRESS_FILE):
             os.remove(PROGRESS_FILE)
        print("所有股票已分析完毕。进度文件已清除。")
        # 退出码 0 通知工作流完成
        exit(0)

if __name__ == "__main__":
    main()
