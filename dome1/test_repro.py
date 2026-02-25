import pypinyin

def conversion(str_val):
    zhong = {
        "零": 0, "一": 1, "二": 2, "三": 3, "四": 4,
        "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
    }
    danwei = {"十": 10, "百": 100, "千": 1000, "万": 10000}
    num = 0
    if len(str_val) == 0: return 0
    if len(str_val) == 1:
        if str_val == "十": return 10
        return zhong.get(str_val, 0)
    
    temp = 0
    if str_val[0] == "十": num = 10
    
    for i in str_val:
        if i in zhong:
            temp = zhong[i]
        elif i == "十":
            temp = max(1, temp) * danwei[i]
            num += temp
            temp = 0
        elif i in danwei:
            temp = temp * danwei[i]
            num += temp
            temp = 0
    
    num += temp
    return num

product_name = "华驭第十七期汽车抵押贷款支持证券"
FCode = "AUTO"

s = ""
for i in pypinyin.pinyin(product_name, style=pypinyin.NORMAL):
    i = i[0].title()
    s += "".join(i)

s = s.split("Nian")[0]

try:
    sp_filename = product_name.split("第")[1]
    nper = sp_filename.split("期")[0]
    conversion_nper = conversion(nper)
    s_trust_code = s + "-" + str(conversion_nper)

    print(f"DEBUG: {s=}")
    print(f"DEBUG: {s_trust_code=}")

    # The problematic logic
    trust_code_1 = s_trust_code.split("2025")[0]
    trust_code_2 = s_trust_code.split("2025")[-1]

    print(f"DEBUG: {trust_code_1=}")
    print(f"DEBUG: {trust_code_2=}")

    trust_code = trust_code_1 + "_" + FCode + "2025" + trust_code_2
    print(f"RESULT: {trust_code=}")
    print(f"LENGTH: {len(trust_code)}")
except Exception as e:
    print(f"ERROR: {e}")
