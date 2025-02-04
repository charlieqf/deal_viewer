# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 09:38:28 2019

@author: HUAWEI
"""


# %%

def read_file():
    global path, path1, path2, path3, name_z
    path = r'E:\DataTeam\Products_log\更新时间TXT记录\更新受托报告.xlsx'
    if os.path.exists(path) == True:
        print('ok')
        data = open(path)
        df = pd.read_csv(data, header=None)
        df.columns = ['filename']
        name = []
        for i in df.filename:
            name.append(i[26:])
        l_name = len(name)

        name = '    受托报告文件:' + str(len(name)) + "个" + "\n        " + "\n        ".join(name)


    else:
        l_name = 0
        name = '    无受托报告更新'

    path1 = r'E:\DataTeamProducts_log\更新时间TXT记录\发行文件.xlsx'
    if os.path.exists(path1) == True:
        data1 = open(path1)
        df1 = pd.read_csv(data1, header=None)
        df1.columns = ['filename1']
        name1 = []
        for j in df1.filename1:
            name1.append(j[26:])
        l_name1 = len(name1)
        name1 = '\n\n    产品发行文件:' + str(len(name1)) + "个" + "\n        " + "\n        ".join(name1)

    else:
        name1 = '\n   无新产品披露'
        l_name1 = 0

    import pickle
    path2 = r'E:\DataTeam\Products_log\更新时间TXT记录\err_fxwj.pkl'

    if os.path.exists(path2):
        with open(path2, 'rb') as f:
            err_fxwj = pickle.load(f)
        gzProd, cmProd = err_fxwj
        l_name21, l_name22 = len(gzProd), len(cmProd)
        if gzProd:
            name21 = '\n\n    更新更正产品:' + str(l_name21) + "个" + "\n        " + "\n        ".join(gzProd)
        else:
            name21 = '\n   '
            l_name21 = 0

        if cmProd:
            name22 = '\n\n    code重名产品:' + str(l_name22) + "个" + "\n        " + "\n        ".join(cmProd)
        else:
            name22 = '\n   '
            l_name22 = 0
        os.remove(path2)
    else:
        name21 = '\n   '
        l_name21 = 0
        name22 = '\n   '
        l_name22 = 0

    path3 = r'E:\DataTeam\Products_log\更新时间TXT记录\err_stbg.pkl'

    if os.path.exists(path3):
        with open(path3, 'rb') as f:
            err_stbg = pickle.load(f)
        mailFiles, notFound = err_stbg
        l_name31, l_name32 = len(mailFiles), len(notFound)
        if mailFiles:
            name31 = '\n\n    更新版受托报告:' + str(l_name31) + "个" + "\n        " + "\n        ".join(mailFiles)
        else:
            name31 = '\n   '
            l_name31 = 0

        if notFound:
            name32 = '\n\n    无产品受托报告:' + str(l_name32) + "个" + "\n        " + "\n        ".join(notFound)
        else:
            name32 = '\n   '
            l_name32 = 0

        os.remove(path3)
    else:
        name31 = '\n   '
        l_name31 = 0
        name32 = '\n   '
        l_name32 = 0


    name_z = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
<u>{}</u>
<div style="color:blue">银行间ABS新增披露文件:  {} 个</div>
<pre>'''.format(time.strftime("%Y-%m-%d"),
                l_name + l_name1 + l_name21 + l_name22 + l_name31 + l_name32) + name + name1 + name21 + name22 + name31 + name32 + '''</pre>
</body>
</html>'''


def minl_send():
    if os.path.exists(path1) or os.path.exists(path) or os.path.exists(path2) or os.path.exists(path3):
        msg_from = 'fengyanyan@goldenstand.cn'  # 发送方邮箱
        passwd = 'Fyy2516302813'  # 填入发送方邮箱的授权码
        msg_to1 = ['fengyanyan@goldenstand.cn',
                   'qfeng@goldenstand.cn', 'zhangtongyao@gdsd.wecom.work']
        # msg_to1 = ['yixianfeng@goldenstand.cn']
        #

        for msg_to in msg_to1:

            subject = "银行间ABS披露情况"  # 主题
            content = str(name_z)

            msg = MIMEText(content, 'html', 'utf-8')
            # msg = MIMEText(content)
            msg['Subject'] = subject
            msg['From'] = msg_from
            msg['To'] = msg_to
            try:
                s = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
                s.login(msg_from, passwd)
                s.sendmail(msg_from, msg_to, msg.as_string())
                print("发送成功")
                s.quit()
            except Exception as e:
                print(e)
                print("发送失败")
            # finally:
            #     s.quit()

    else:
        print('...无文件更新-无法推送')


if __name__ == "__main__":
    import pandas as pd
    import os
    import smtplib
    from email.mime.text import MIMEText
    import time

    read_file()
    minl_send()

