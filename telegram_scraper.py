import os
import requests
from bs4 import BeautifulSoup
import easyocr
import datetime

# 初始化 OCR，增加识别容错
try:
    reader = easyocr.Reader(['ch_sim', 'en'], gpu=False)
except Exception as e:
    print(f"OCR 初始化警告: {e}")
    reader = None

channels = ['ChinaStock3000', 'Guanshuitan', 'gainiantuhua', 'hgclhyyb']

def get_channel_content(channel_name):
    print(f">>> 正在尝试抓取: {channel_name}")
    url = f"https://t.me/s/{channel_name}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        # 兼容性查找：Telegram 有时使用不同的 message wrapper
        messages = soup.find_all('div', class_=['tgme_widget_message_wrap', 'tgme_widget_message'])
        
        if not messages:
            return f"## 来源: {channel_name}\n> 暂未抓取到公开消息，可能是频道设置了隐私或结构变更。\n\n---\n\n"
            
    except Exception as e:
        return f"## 来源: {channel_name}\n抓取异常: {e}\n\n---\n\n"
    
    content_md = f"## 来源: {channel_name}\n\n"
    
    # 获取最新的 10 条，确保覆盖
    for msg in messages[-10:]:
        # 1. 文字提取 (增加对 inner 标签的深度搜索)
        text_element = msg.find('div', class_='tgme_widget_message_text')
        text = text_element.get_text(separator="\n").strip() if text_element else ""
        
        # 2. 图片提取与 OCR
        ocr_text = ""
        # 尝试多种可能的图片标签
        photo_element = msg.find('a', class_=['tgme_widget_message_photo_step', 'tgme_widget_message_photo'])
        if photo_element and reader:
            style = photo_element.get('style', '')
            img_url = None
            if 'url(' in style:
                img_url = style.split("url('")[1].split("')")[0]
            
            if img_url:
                try:
                    img_data = requests.get(img_url, timeout=10).content
                    with open("temp.jpg", "wb") as f:
                        f.write(img_data)
                    
                    result = reader.readtext("temp.jpg", detail=0)
                    if result:
                        ocr_text = "\n\n> **[图片识别文字]**：\n> " + "\n> ".join(result)
                except Exception as e:
                    print(f"图片处理跳过: {e}")

        if text or ocr_text:
            content_md += f"{text}{ocr_text}\n\n---\n\n"
            
    return content_md

def main():
    shanghai_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    final_report = f"# Telegram 频道自动汇总\n\n更新时间 (北京时间): {shanghai_time}\n\n"
    
    for channel in channels:
        final_report += get_channel_content(channel)

    # 写入 README.md
    with open("README.md", "w", encoding="utf-8") as f:
        f.write(final_report)
    
    # 同时在 history 目录存档（按日期）
    if not os.path.exists("history"):
        os.makedirs("history")
    history_path = f"history/{shanghai_time[:10]}.md"
    with open(history_path, "a", encoding="utf-8") as f:
        f.write(f"\n\n--- 抓取时间: {shanghai_time} ---\n\n" + final_report)

if __name__ == "__main__":
    main()
