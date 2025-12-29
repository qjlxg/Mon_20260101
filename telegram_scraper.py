import os
import requests
from bs4 import BeautifulSoup
import easyocr
import datetime
import time
import re

# 初始化 OCR
try:
    reader = easyocr.Reader(['ch_sim', 'en'], gpu=False)
except:
    reader = None

channels = ['ChinaStock3000', 'Guanshuitan', 'gainiantuhua', 'hgclhyyb']

def get_channel_content(channel_name):
    print(f"--- 抓取: {channel_name} ---")
    url = f"https://t.me/s/{channel_name}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        res = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        # 修复：只选择最外层的消息包装器，避免内层重复选择
        message_elements = soup.find_all('div', class_='tgme_widget_message', recursive=True)
        
        if not message_elements:
            return f"## 来源: {channel_name}\n> 未发现公开消息。\n\n---\n\n"
    except Exception as e:
        return f"## 来源: {channel_name}\n> 错误: {e}\n\n---\n\n"
    
    content_list = []
    seen_texts = set() # 简单去重

    # 取最后 12 条消息
    for msg in message_elements[-12:]:
        # 1. 提取文字
        text_area = msg.find('div', class_='tgme_widget_message_text')
        text = text_area.get_text(separator="\n").strip() if text_area else ""
        
        # 2. 提取图片 (专项修复背景图提取)
        ocr_text = ""
        photo_a = msg.find('a', class_='tgme_widget_message_photo_step')
        if photo_a and reader:
            style = photo_a.get('style', '')
            # 正则匹配 url('...') 里的链接
            img_match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
            if img_match:
                img_url = img_match.group(1)
                try:
                    img_data = requests.get(img_url, timeout=10).content
                    with open("temp.jpg", "wb") as f:
                        f.write(img_data)
                    # 执行 OCR
                    result = reader.readtext("temp.jpg", detail=0)
                    if result:
                        ocr_text = "\n\n> **[图片识别]**：\n> " + "\n> ".join(result)
                    os.remove("temp.jpg")
                except:
                    pass

        full_msg = f"{text}{ocr_text}".strip()
        if full_msg and full_msg not in seen_texts:
            content_list.append(f"{full_msg}\n\n---\n\n")
            seen_texts.add(full_msg)
            
    return f"## 来源: {channel_name}\n\n" + "".join(content_list)

def main():
    sh_tz = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(sh_tz)
    sh_time = now.strftime('%Y-%m-%d %H:%M:%S')
    
    final_output = f"# Telegram 内容汇总\n\n**北京时间: {sh_time}**\n\n"
    for c in channels:
        final_output += get_channel_content(c)
        time.sleep(1)

    # 更新 README
    with open("README.md", "w", encoding="utf-8") as f:
        f.write(final_output)
    
    # 归档
    os.makedirs("history", exist_ok=True)
    with open(f"history/{now.strftime('%Y-%m-%d')}.md", "a", encoding="utf-8") as f:
        f.write(f"\n\n### 抓取时间: {sh_time}\n\n" + final_output)

if __name__ == "__main__":
    main()
