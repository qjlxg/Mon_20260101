import os
import shutil
import glob

def sync_csv_files():
    # --- 关键修改：获取当前脚本所在的根目录 ---
    # 因为你运行的是 python main_repo/sync_stock_data.py
    # 此时 os.getcwd() 通常是 GitHub Actions 的工作根目录
    base_dir = os.getcwd()
    
    # 根据你的 YAML 配置：
    # 源文件在：根目录/source_repo/stock_data
    # 目标文件在：根目录/main_repo/stock_data
    source_dir = os.path.join(base_dir, 'source_repo/stock_data')
    target_dir = os.path.join(base_dir, 'main_repo/stock_data')

    print(f"当前工作目录: {base_dir}")
    print(f"源目录路径: {source_dir}")
    print(f"目标目录路径: {target_dir}")

    # 1. 检查源目录是否存在
    if not os.path.exists(source_dir):
        print(f"错误: 找不到源目录 {source_dir}。请检查 YAML 中的 path 设置。")
        return

    # 2. 确保目标目录存在
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"已创建目标目录: {target_dir}")

    # 3. 获取源目录下所有最新的 CSV
    csv_files = glob.glob(os.path.join(source_dir, '*.csv'))
    source_filenames = {os.path.basename(f) for f in csv_files}
    
    if not csv_files:
        print(f"警告: 在 {source_dir} 未找到任何 CSV 文件。")
        return

    # 4. 【镜像同步关键】清理目标目录中多余的旧文件
    # 如果某个文件在源目录没了，但在目标目录还有，就删掉它
    target_files_path = glob.glob(os.path.join(target_dir, '*.csv'))
    remove_count = 0
    for t_path in target_files_path:
        t_name = os.path.basename(t_path)
        if t_name not in source_filenames:
            os.remove(t_path)
            print(f"已清理旧 CSV (源目录已不存在): {t_name}")
            remove_count += 1

    # 5. 执行同步（覆盖更新）
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(target_dir, file_name)
        shutil.copy2(file_path, dest_path)

    print(f"--- 同步任务结束 ---")
    print(f"成功同步: {len(csv_files)} 个文件")
    print(f"清理冗余: {remove_count} 个文件")

if __name__ == "__main__":
    sync_csv_files()
