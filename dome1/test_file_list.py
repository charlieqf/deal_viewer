#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试get_file_list函数"""

import requests
import json
import urllib3
urllib3.disable_warnings()

def test_search(product_name):
    print(f"搜索: {product_name}")
    
    # 获取token
    session = requests.Session()
    
    # 访问主页获取Cookie
    home_url = "https://www.chinamoney.com.cn/chinese/qwjsn/"
    home_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    session.get(home_url, headers=home_headers, verify=False, timeout=10)
    print(f"Cookie: {session.cookies.get_dict()}")
    
    # 获取token
    token_url = "https://www.chinamoney.com.cn/ags/ms/cm-s-notice-query/getToken"
    token_resp = session.post(token_url, verify=False, timeout=10)
    print(f"Token响应: {token_resp.status_code}")
    token_data = token_resp.json()
    print(f"Token数据: {token_data}")
    
    info_level = token_data["UT"].replace("\n", "")
    sign = token_data["sign"].replace("\n", "")
    
    # 搜索文档
    url = "https://www.chinamoney.com.cn/ses/rest/cm-u-notice-ses-cn/query"
    data = {
        "sort": "date",
        "text": product_name,
        "date": "all",
        "field": "title",
        "start": "",
        "end": "",
        "pageIndex": "1",
        "pageSize": "50",
        "public": "false",
        "infoLevel": info_level,
        "sign": sign,
        "channelIdStr": "2496, 2556, 2632, 2663, 2589, 2850, 3300,",
        "nodeLevel": "1",
    }
    
    headers = {
        "Accept": "*/*",
        "Content-Length": "0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    
    rs = session.post(url, params=data, headers=headers, verify=False)
    print(f"搜索响应: {rs.status_code}")
    
    result = rs.json()
    total = result["data"]["result"]["total"]
    pageItems = result["data"]["result"]["pageItems"]
    
    print(f"\n总数: {total}")
    print(f"pageItems数量: {len(pageItems)}")
    
    print("\n文档列表:")
    for i, item in enumerate(pageItems[:10]):  # 只显示前10个
        print(f"  {i+1}. {item.get('title', 'N/A')}")
        print(f"     paths: {item.get('paths', [])}")
        print()

if __name__ == "__main__":
    test_search("前联2025年度第五期华发优生活六号资产支持票据")
