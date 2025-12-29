import os
import requests
from bs4 import BeautifulSoup
import easyocr
import datetime

# 初始化 OCR (中文和英文)
reader = easyocr.Reader(['ch_sim', 'en'])

channels = [
    'ChinaStock3000', 
    'Guanshuitan', 
    'gainiantuhua', 
    'hgclhyyb'
]

def get_channel_content(channel_name):
    print(f"正在抓取频道: {channel_name}")
    url = f"https://t.me/s/{channel_name}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 查找所有消息卡片
    messages = soup.find_all('div', class_='tgme_widget_message_wrap')
    
    content_md = f"## 来源: {channel_name}\n\n"
    
    # 只取最新的 5 条防止 GitHub Actions 超时
    for msg in messages[-5:]:
        # 1. 提取文字内容
        text_element = msg.find('div', class_='tgme_widget_message_text')
        text = text_element.get_text(separator="\n") if text_element else "无文字内容"
        
        # 2. 提取图片并 OCR
        photo_element = msg.find('a', class_='tgme_widget_message_photo_step')
        ocr_text = ""
        if photo_element:
            # 获取背景图 URL
            style = photo_element.get('style', '')
            if 'url(' in style:
                img_url = style.split("url('")[1].split("')")[0]
                try:
                    # 下载图片
                    img_data = requests.get(img_url).content
                    with open("temp.jpg", "wb") as f:
                        f.write(img_data)
                    # OCR 识别
                    results = reader.readtext("temp.jpg")
                    ocr_text = "\n> **[图片识别结果]**: " + " ".join([res[1] for res in results])
                except Exception as e:
                    ocr_text = f"\n> [图片识别失败]: {e}"

        content_md += f"{text}{ocr_text}\n\n---\n\n"
        
    return content_md

def main():
    # 获取上海时间
    shanghai_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    
    final_report = f"# Telegram 频道内容自动汇总\n\n更新时间 (北京时间): {shanghai_time}\n\n"
    
    for channel in channels:
        try:
            final_report += get_channel_content(channel)
        except Exception as e:
            final_report += f"## 来源: {channel}\n抓取失败: {e}\n\n---\n\n"

    # 写入 README
    with open("README.md", "w", encoding="utf-8") as f:
        f.write(final_report)
    
    # 存入历史记录目录
    if not os.path.exists("history"):
        os.makedirs("history")
    history_file = f"history/{shanghai_time[:10]}.md"
    with open(history_file, "a", encoding="utf-8") as f:
        f.write(f"\n\n{final_report}")

if __name__ == "__main__":
    main()
