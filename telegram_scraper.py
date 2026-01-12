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

channels = ['clsvip','zaobaocn','hgclhyyb','513180','FinanceNewsDaily','WorldSpotNews','BloombergZh','nowtv_news','kingkitay' ]

def get_channel_content(channel_name):
    print(f"--- 正在处理: {channel_name} ---")
    url = f"https://t.me/s/{channel_name}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        res = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(res.text, 'html.parser')
        # 寻找所有消息节点
        message_elements = soup.find_all('div', class_='tgme_widget_message')
        if not message_elements:
            return f"## 来源: {channel_name}\n> 未抓取到内容，可能存在访问频率限制。\n\n---\n\n"
    except Exception as e:
        return f"## 来源: {channel_name}\n> 抓取异常: {e}\n\n---\n\n"
    
    content_list = []
    seen_ids = set() # 用于去重

    # 处理最新的 10 条
    for msg in message_elements[-10:]:
        # 通过消息 ID 去重
        msg_id = msg.get('data-post')
        if msg_id in seen_ids: continue
        seen_ids.add(msg_id)

        # 1. 文字内容提取
        text_div = msg.find('div', class_=['tgme_widget_message_text', 'js-message_text'])
        text = text_div.get_text(separator="\n").strip() if text_div else ""
        
        # 2. 图片 OCR 提取 (针对 background-image 逻辑)
        ocr_text = ""
        photo_a = msg.find('a', class_='tgme_widget_message_photo_step')
        if photo_a and reader:
            style = photo_a.get('style', '')
            img_match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
            if img_match:
                img_url = img_match.group(1)
                try:
                    img_data = requests.get(img_url, timeout=10).content
                    with open("temp.jpg", "wb") as f:
                        f.write(img_data)
                    lines = reader.readtext("temp.jpg", detail=0)
                    if lines:
                        ocr_text = "\n\n> **[图片识别内容]**：\n> " + "\n> ".join(lines)
                    os.remove("temp.jpg")
                except:
                    pass

        if text or ocr_text:
            content_list.append(f"{text}{ocr_text}\n\n---\n\n")
            
    return f"## 来源: {channel_name}\n\n" + "".join(content_list)

def main():
    # 上海时间
    sh_tz = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(sh_tz)
    sh_time = now.strftime('%Y-%m-%d %H:%M:%S')
    
    final_output = f"# 汇总\n\n**最后更新: {sh_time}**\n\n"
    for c in channels:
        final_output += get_channel_content(c)
        time.sleep(1.5) # 稍微延迟防屏蔽

    # 更新 README.md
    with open("README.md", "w", encoding="utf-8") as f:
        f.write(final_output)
    
    # 存入 history
    os.makedirs("history", exist_ok=True)
    with open(f"history/{now.strftime('%Y-%m-%d')}.md", "a", encoding="utf-8") as f:
        f.write(f"\n\n### 抓取时点: {sh_time}\n\n" + final_output)

if __name__ == "__main__":
    main()
