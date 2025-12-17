import re

titles = [
    "保利商业保理有限公司2025年度第五期邦鑫一号资产支持票据",
    "深圳前海联易融商业保理有限公司2025年度第一期中悦一号资产支持票据",
    "广州市自来水有限公司2025年度2号第一期绿色资产支持票据(科创票据)"
]

regex = r"\d{2}.+?\d{3}"

print(f"Testing regex: {regex}")
for title in titles:
    match = re.findall(regex, title)
    print(f"Title: {title}")
    if match:
        print(f"  MATCH: {match}")
    else:
        print(f"  NO MATCH")
