# -*- coding: utf-8 -*-
"""
cn-abs.com 证券信息爬取测试脚本

================================================================================
抓取流程（模拟用户操作）
================================================================================

步骤1: 登录
    - 打开网站: https://www.cn-abs.com/index.html?page=home#/home
    - 使用账号登录:
        账号: 18085157187
        密码: Password01

备注：1. 打开https://www.cn-abs.com/#/home 点击右上角“登录”按钮（class="abs-btn btn-logion"），或者直接打开这个地址：https://account.cn-abs.com/account.html?return_url=%2Fconnect%2Fauthorize%2Fcallback%3Fclientid%3Djs_oauth%26client_id%3Djs_oauth%26redirecturi%3Dhttps%253A%252F%252Fwww.cn-abs.com%252Fsign.html%2523%252Fcallback%253F%26redirect_uri%3Dhttps%253A%252F%252Fwww.cn-abs.com%252Fsign.html%2523%252Fcallback%253F%26responsetype%3Did_token%2520token%26response_type%3Did_token%2520token%26scope%3Dopenid%2520profile%2520cnabs%2520quotes%2520organization%2520identity%2520apicenter%2520projects%2520products%2520assets%2520opendata%26state%3D128bdabe45db47b6ab30124c1e2aa336%26nonce%3Dddff580f02534ef79046c942c570b8f3#/login  应该可以直接到达登录页面

页面元素：
用户名输入框 class="ant-input ant-input-lg" 和密码输入框 class="ant-input ant-input-lg" 验证码 class="abs-captcha-code-img" 验证码输入框 class="ant-input ant-input-lg" 用户协议同意勾选按钮 class="ant-checkbox-input" 登录按钮 type="submit" class="ant-btn ant-btn-primary" 

有一个验证码的请求：
请求网址: https://account.cn-abs.com/api/global/captcha?type=Login&t=0.9213134580109255
请求方法: GET
状态代码: 200 OK
远程地址: 120.26.229.199:443
引荐来源网址政策: strict-origin-when-cross-origin
Connection: keep-alive
Content-Encoding: gzip
Content-Type: image/png
Date: Mon, 08 Dec 2025 04:26:34 GMT
Server: openresty
Set-Cookie: CNABS3_Vcode_Login=8e29aeca-67ff-4cc4-815c-0f176a294b07; path=/; httponly
Transfer-Encoding: chunked
Vary: Accept-Encoding
Accept: image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Connection: keep-alive
Host: account.cn-abs.com
Referer: https://account.cn-abs.com/account.html?return_url=%2Fconnect%2Fauthorize%2Fcallback%3Fclientid%3Djs_oauth%26client_id%3Djs_oauth%26redirecturi%3Dhttps%253A%252F%252Fwww.cn-abs.com%252Fsign.html%2523%252Fcallback%253F%26redirect_uri%3Dhttps%253A%252F%252Fwww.cn-abs.com%252Fsign.html%2523%252Fcallback%253F%26responsetype%3Did_token%2520token%26response_type%3Did_token%2520token%26scope%3Dopenid%2520profile%2520cnabs%2520quotes%2520organization%2520identity%2520apicenter%2520projects%2520products%2520assets%2520opendata%26state%3Dd5b0af220cc841e3b900d132cc3d2e7a%26nonce%3Ddd7929b70d044748a9625e7f20efb62d
sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Sec-Fetch-Dest: image
Sec-Fetch-Mode: no-cors
Sec-Fetch-Site: same-origin
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36

载荷：
type: Login
t: 0.45320279503762717
预览：能看到一张验证码图片

点击登录按钮后：

请求网址: https://account.cn-abs.com/api/account/login
请求方法: POST
状态代码: 200 OK
远程地址: 120.26.229.199:443
引荐来源网址政策: strict-origin-when-cross-origin
Access-Control-Allow-Credentials: true
Access-Control-Allow-Origin: https://account.cn-abs.com
Cache-Control: no-cache,no-store
Connection: keep-alive
Content-Length: 51
Content-Type: application/json; charset=utf-8
Date: Mon, 08 Dec 2025 04:29:22 GMT
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Pragma: no-cache
Server: openresty
Set-Cookie: CNABS3_Vcode_Login=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/
Set-Cookie: idsrv.session=ECBCE737023DCA0DBCA587004110BCFD; path=/; secure
Set-Cookie: .AspNetCore.Identity.Application=CfDJ8B_82XvvshNNn-prPfNHcuXhG3QazWblhBHteUjlMXJlAIVWvweF5nSKlmzIlRwOok4a6IVDC0IZKncve6yEVyKJMV-hk5PFQUJhXhiYH4tb5JmfcVOPsgqIAqPA8EdmBDehHJ0cpaWpF2ff67cq91B9kncxScGucBQ-os3GuqxguwZZxmWZ7l-4DCde2tCIb8cRxShPMYYF6S3K5OHIR9uaigGMyqWujBDtI4q8nrpz-Ts79RowL5pNsVgfcMEaS0j1RhIkzgxcwP05Tjy6eWpT8W6hpAElgzNgbo8WQTsbza2_AAV-71a0sEKn2waz-xLYiSm-80e3dmhmmKgCRNZm0mw84MSIm0vTftrRoB7PhyMGp-HlB86RjOFC7_NpCBvKa7IyWVTSX9S5wummBFFeR3jShB4KOK4UQ3JEVSMH6Mmt4xGLpO5jQSV9k6tUbTAm5WCsM1250bvtzIHaHnMaaZyEEnPpe86kaaktLj9JZefABpsKQJ-TmvL-fuf8LSorQiuLGSYLID-CXijgMQc6aU3ZVX8W0XR9ljwamIdvEKyRrNOOHkr1OZwoL04urUOqSgLBYeVYsNzZ8RabEGL791Vj-TtfQ1jn1POyJsnOSF7JmrXbQyKCn6zHEeBK9ZEFjyv2SB8lwTmH1xvZJN4d_M9iUxWD7whsQO8ZW51GQpZeLv8NFkx_N0sxyiyyqjeHjmL2F0nxOztQ-pbjM8VQ6xOjTBnC0R0SG3RAFxKtLYaw9N3yAxjnH7PuGHGQDnO8kDlgPSHxpLNlCXvj3anUS-sP7zDHHrXp-lQHZzVlo-UmxfHAHaQZYM_JgmBB_H-PBssOEgD-JXxkv84r3rkSWaASt-l2oZFPNJcuwo6YFHOcAyefeG7SZdw1DiA8MRWliOqgl8ovQuqXq_EXt3VryTr3F2kRhP4qFu2nW0Sw5VwxUcW7OBvDVyxAiImslLscftQUebxKTAg5y-S6I90M4KTarpjnyOWmrfV7QVCk-g2x5B8pT05C4eAY65VXER-w2-eDs6FsMnJg3bwgjM62f7X40_F_FCbRpFpzJIF3TjtqojkZLWXTZgJFG6zn49YeIO_c6B0roRuKKAWPQ9YKJqZwHBYjt5Lc3HiVuGf1hON3cx91gNo0gwNTCjy05SXh8wGVFGt08tSaAMZdL2QqbyX9h_YnYAudsI8OhFszZk3_lFAgSolIUAIH65Ro4w; path=/; secure; httponly
Vary: Origin
Accept: application/json
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Connection: keep-alive
Content-Length: 68
Content-Type: application/json
Cookie: CNABS3_Vcode_Login=1832b9a2-5f27-4a54-ac91-f2b17a2241cc
Host: account.cn-abs.com
Origin: https://account.cn-abs.com
Referer: https://account.cn-abs.com/account.html?return_url=%2Fconnect%2Fauthorize%2Fcallback%3Fclientid%3Djs_oauth%26client_id%3Djs_oauth%26redirecturi%3Dhttps%253A%252F%252Fwww.cn-abs.com%252Fsign.html%2523%252Fcallback%253F%26redirect_uri%3Dhttps%253A%252F%252Fwww.cn-abs.com%252Fsign.html%2523%252Fcallback%253F%26responsetype%3Did_token%2520token%26response_type%3Did_token%2520token%26scope%3Dopenid%2520profile%2520cnabs%2520quotes%2520organization%2520identity%2520apicenter%2520projects%2520products%2520assets%2520opendata%26state%3D2bddd322da43424fb2adebd011c3f783%26nonce%3D4c7fc75cccd344dbb3f1ccbaac8f4281
sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
Title: %E8%B5%84%E4%BA%A7%E8%AF%81%E5%88%B8%E5%8C%96%E5%88%86%E6%9E%90%E7%BD%91-ABS%E3%80%81REITs%E5%9C%A8%E7%BA%BF%E5%88%86%E6%9E%90%E5%B9%B3%E5%8F%B0
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36

载荷：
{user_name: "13985684486", password: "88520123Ww", captcha: "6017"}
captcha: "6017"
password: "88520123Ww"
user_name: "13985684486"

响应：无法加载响应数据: No resource with given identifier found

login请求之后有一个callback请求：
请求网址: https://account.cn-abs.com/connect/authorize/callback?clientid=js_oauth&client_id=js_oauth&redirecturi=https%3A%2F%2Fwww.cn-abs.com%2Fsign.html%23%2Fcallback%3F&redirect_uri=https%3A%2F%2Fwww.cn-abs.com%2Fsign.html%23%2Fcallback%3F&responsetype=id_token%20token&response_type=id_token%20token&scope=openid%20profile%20cnabs%20quotes%20organization%20identity%20apicenter%20projects%20products%20assets%20opendata&state=2bddd322da43424fb2adebd011c3f783&nonce=4c7fc75cccd344dbb3f1ccbaac8f4281
请求方法: GET
状态代码: 302 Found
远程地址: 120.26.229.199:443
引荐来源网址政策: strict-origin-when-cross-origin
Cache-Control: no-store, no-cache, max-age=0
Connection: keep-alive
Date: Mon, 08 Dec 2025 04:29:22 GMT
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Location: https://www.cn-abs.com/sign.html#/callback?id_token=eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc2NTkyRTczMTFGMjRFMjg4QjVCQ0Q4QTU1RjU2QTg2MzFFOUNBQTNSUzI1NiIsInR5cCI6IkpXVCIsIng1dCI6ImRsa3VjeEh5VGlpTFc4MktWZlZxaGpIcHlxTSJ9.eyJuYmYiOjE3NjUxNjgxNjIsImV4cCI6MTc2NTE2ODQ2MiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50LmNuLWFicy5jb20iLCJhdWQiOiJqc19vYXV0aCIsIm5vbmNlIjoiNGM3ZmM3NWNjY2QzNDRkYmIzZjFjY2JhYWM4ZjQyODEiLCJpYXQiOjE3NjUxNjgxNjIsImF0X2hhc2giOiJ6Y1RnVHAtd0tVZjB1cUFWREJLZVNRIiwic19oYXNoIjoiSjR5bVpNZ2JNVDVDUHBxWTh5TmhYZyIsInNpZCI6IkVDQkNFNzM3MDIzRENBMERCQ0E1ODcwMDQxMTBCQ0ZEIiwic3ViIjoiQ0M2QzdBNEUtMzA3My00NjM4LThGQkYtRUIyQjEwMDg4NEJDIiwiYXV0aF90aW1lIjoxNzY1MTY4MTYyLCJpZHAiOiJsb2NhbCIsImFtciI6WyJwd2QiXX0.crkAu4S8pCz2VSl4UeTkk0cH_LnuuSj32NWzAq6jEOrcF1QR-iJ8f1mwMr0aOT_Sl0Jo9nsvxiUNjdePdqC9AhcWG9fljYV7pjZJj8geN2o7esgjTGraecky_TJuwhfWbQvsckylFUWbz82q8kYA-1nHqC5Lj4FCG5XOFiPwzhsQyZBwTRgD8jji0_aQlG_oJcAs1mLk5Y8pZZrsF2nZEvhYg6Jf2SfNrghtIsK4ZGuX1VowJo6_sWDz6wx7-h1G8fa1gJtwHbTZaHgkTmFGp_AwcIYE6cQGUviwh0xj39ol9cYpQr7n9PvXbnJoh_mlmdNsKMWyB_y0J8U_q5Zqjqvf-Il-sfTMrjNW14AxtYR7gcbHL3HhZLsI_xcnEsAkLC2uzlXx-uvc_TfNGpwcV6CzOUhMwJfTWyU7VFAPL6RXwDEJVqVOAYGS9tES6JfV62sFK5YyyZBTgL1nQN3HQ3DxYz3hFl4-tJc6-xVxPKWMAm18-wnWchOaVQCXa-7LtXTzUJ4ubWbKLpAyVvdSCjiZIBBdZfaRfYSPQC96Nwny3SDV4RiMZXpgoA76LGLed01b9_1cSrAJkep5CbUEUjmF5XycVbBf1rOAclt4wLIh4rM9Dc82j8wAmW3nFungvo3Sf1UfLRKiC7HLU7ESE_eeJki1UNjmNJx6pGHgXSQ&access_token=eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc2NTkyRTczMTFGMjRFMjg4QjVCQ0Q4QTU1RjU2QTg2MzFFOUNBQTNSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImRsa3VjeEh5VGlpTFc4MktWZlZxaGpIcHlxTSJ9.eyJuYmYiOjE3NjUxNjgxNjIsImV4cCI6MTc2NTI1NDU2MiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50LmNuLWFicy5jb20iLCJhdWQiOlsiY25hYnMiLCJwcm9kdWN0cyIsImNhbGMiLCJxdW90ZXMiLCJvcmdhbml6YXRpb24iLCJpZGVudGl0eSIsImFwaWNlbnRlciIsInByb2plY3RzIiwiYXNzZXRzIiwib3BlbmRhdGEiXSwiY2xpZW50X2lkIjoianNfb2F1dGgiLCJzdWIiOiJDQzZDN0E0RS0zMDczLTQ2MzgtOEZCRi1FQjJCMTAwODg0QkMiLCJhdXRoX3RpbWUiOjE3NjUxNjgxNjIsImlkcCI6ImxvY2FsIiwibmFtZSI6IjEzOTg1Njg0NDg2Iiwic2lkIjoiRUNCQ0U3MzcwMjNEQ0EwREJDQTU4NzAwNDExMEJDRkQiLCJpYXQiOjE3NjUxNjgxNjIsInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJjbmFicyIsInF1b3RlcyIsIm9yZ2FuaXphdGlvbiIsImlkZW50aXR5IiwiYXBpY2VudGVyIiwicHJvamVjdHMiLCJwcm9kdWN0cyIsImFzc2V0cyIsIm9wZW5kYXRhIl0sImFtciI6WyJwd2QiXX0.D9t_cUhnWODhn38Dhns13K-1MHDtjgi02bVgxnoC2rcLdkTIkiMC2VYDPUVeg075ru1PzPBT76Nsby5TodpRXDPRzPNqHAchXLM5Z_Vc6vWonh9DQckbX6tngS737M0jyQTwj_kxC3YtwGBCqBkAZO4Mk7rS5O5pqJg2vOPCZqvG1-IheI7QFW5hULYCgMXEeEGVWo_n4oT0zbX6Iej6MxZnGttqzVieVSwSibl8wyc6qgZpY2bmKHRjBvdvu92NBose5rXO_0w7BDRdhYHMHc_oP7oo-JdOWwRiWH-pYiqxOGxlf_IDPIEzFf4FGjAj5xpp7ZqxV6ISRHJKMA0_x_AEFVnot4RfuIEtTKwjWr-8pnLaYaa_9Hu_YyPdMazQC88OjjzCcsyhpVfymkN32xCv1v9P0VhWwrKRD5dhg-B5VH3n_qpf3QHaMuF-Q6MCn1ujKL2jZpiCKoRMYMkEZ4th3frnUacV5tbK7Gcok60ttSOsYnOmViWCPZrxU6JJByFSIXi45hspoZzQxND9PpRoqRAjv6bU4m87Q0tFZDPZyw42WICagMgeXZ6r9GP_RZKLjmHw8JL5Dx5br9JwgLtnm94Fp5nqFcd5otLhMIDfnxcq24SBGTPhYTJF1n_OeYnSpfVPIkLS5OogvEiWANyH151ULtT2gnkek4ZzOtQ&token_type=Bearer&expires_in=86400&scope=openid%20profile%20cnabs%20quotes%20organization%20identity%20apicenter%20projects%20products%20assets%20opendata&state=2bddd322da43424fb2adebd011c3f783&session_state=wIO5N6zNT4JEZBp4hR360AGdaPDyRdSkaiiK67jmx9I.88F075C3B86E5FC3BE2B663DCB856AA0
Pragma: no-cache
Server: openresty
Set-Cookie: .AspNetCore.Identity.Application=CfDJ8B_82XvvshNNn-prPfNHcuUf3-QNKb3JZ85dDP61RGbjXVnUopQoh0EEOz4De2hhpzbFgLl9jeOmNPs4A0iy8SCdc86x09Gd5OhOmys8mU5ljYmTdaoHKLnoRm38fA8-s8nN3F4RAEnGQzfqOp8pqUpuvV-0mswOTcYkE-GK-Ms476x19bAaeI4R43ivxyXqPgVgER60VhO0rgsROxowefMaeWggUMqNiwDBjrmgnpbX2fTnE4yRtY0I3fj2xjTHDJv6BrjL3l0M3FYNcDqMQ0WXyS-HP5J0jUkEkbUB793NhGcTJr0Pfw8cV8lZpYA1htJLAnKrdIDfxAoAf7oBZQ2M0qRdWMrDFS0JVPT6QYvFYLlohd8GY7m0Oz8NtF-n-rFEdqc3ChcEHf_8euPhzc27HCKefeALLlDA1C1Qx1r-4Y4_sxGFzbYbaGhOHVZvkvxTliR7Ln4pCPIIWn8YhfHBMXkrTUfFQU9MKk_wjy_zMi4OfHK4aDocfzvtAVf6sw4tpS7g_HhlwomkCFNKiCnFozu03oEL1p5a934XIyKcLphRDSebD750Op_43ChZ8Br4hgZdixEx7XWUwMd31yWd7xeuYfaaLDWRgcOv-EuDm0d2hfEtzXtMJUQ8eUGY2J89271fdbcl9CeFnnXg8oVCfPr5UEL0Tl5nClzHvOkwao6sM8J72NL0jnNBj_wlkfuk1iz9c0cgn9Gu2GtcFK072d07kReHLTEJDnfh4CAubiT7l-OqCRVTcQ54HnusNHVe8keIfGeMxn3c9KvvkeNGNOh63QownJYzar4nMChuvhfCDF72EzNaMsWx8C4x3zf5nbwinU4Ec3gDxlAYI0hiOfdrjLIgPZ66_x4XEVJoeGb-U8-KEfpc4rE29Bfnc8NKWtnEM7ddddf5z2TZxjw1eMEqGZossCuYwTrYB4EHUu2hP1WKpXtYURptyMV_A_onxZ6ii2YT6SZYWsWc_wl9jEUd0j2sFcNgKjlUTJ1u6XjDg8gUPGrwMOSsfi-iOgB9qyC58akQaMPoczVTu9_M_Mx53Yo9z4siOMTdkKScrj5GPyQYoNAQ0Z7aC9XIMatv60dC0s_5hmOO9FWJk4pb82oPdIh7SeD3GLAxOl4nLch9SZS9cFE-dc3CbplrxDCG8Eo2njWb5VCyjWNrSVeR78pwA_c3iSuHnKbQe4zdHOmfW7IvwiFFDPBRMgBgg9XRrZkByj7A4LDVNP05FYA; path=/; secure; httponly
Transfer-Encoding: chunked

响应：无法加载响应数据


请求网址: https://www.cn-abs.com/apigateway/cnabs/account/login/oauth
请求方法: POST
状态代码: 200 OK

Access-Control-Allow-Credentials: true
Access-Control-Allow-Headers: *
Access-Control-Allow-Methods: GET, POST, PUT, DELETE
Access-Control-Allow-Origin: *
Cache-Control: private, s-maxage=0
Connection: keep-alive
Content-Length: 255
Content-Type: text/html; charset=utf-8
Date: Mon, 08 Dec 2025 05:37:31 GMT
Expires: -1
Pragma: no-cache
Server: openresty
Set-Cookie: CNABS_PassCode=13985684486; domain=cn-abs.com; expires=Mon, 15-Dec-2025 05:37:25 GMT; path=/
Set-Cookie: cnabs=2B0C37A02606A7F3FCA3205E2B2696D84B65779B42C4A679B862F6AA229E14A66058E551FE5DCB69C755C682ED41BB6B241DE6D9DACD7912CF9555A88A8F6EECC052B646CDB3446EF170796799A5B1DF04401246; domain=cn-abs.com; expires=Mon, 15-Dec-2025 05:37:30 GMT; path=/; HttpOnly; SameSite=Lax
Set-Cookie: ISNEW=; expires=Sun, 07-Dec-2025 05:37:31 GMT; path=/
Set-Cookie: cnabs_web=LW3rgXGydKHCUGia4258h3o1CPq1WXG2hoZii-btahMm23k6x2jcvz3pM7a5war-I6PbtWxKtUu2CRbjpL5DhxcCfg75wUEVQUx8N28c1sfxFpheSyoxImIkxMXUmjojY6zKN83DOdOBoI_0DA_OnVKe5b00G1ekLT7tbeFnpha-MhwG52KQ8fOe8j_5B6gpB6Tl1mw-OKie3ja51-uzpV6g58pBuLiNQlQxXQ0ADVLxlSI2CJjfSl4S9KNWbGbIWhQZYelaCsggG-aZed7Pe16SkL4Q2LKGw15xYWxqfG7nwpY21jt03AkCLIefgiUg5rkUrUNx0n7-eFPnwnllqth8sJQ; domain=cn-abs.com; path=/; HttpOnly
X-AspNet-Version: 4.0.30319
X-AspNetMvc-Version: 5.2

Accept: application/json
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc2NTkyRTczMTFGMjRFMjg4QjVCQ0Q4QTU1RjU2QTg2MzFFOUNBQTNSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImRsa3VjeEh5VGlpTFc4MktWZlZxaGpIcHlxTSJ9.eyJuYmYiOjE3NjUxNzIyNDksImV4cCI6MTc2NTI1ODY0OSwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50LmNuLWFicy5jb20iLCJhdWQiOlsiY25hYnMiLCJwcm9kdWN0cyIsImNhbGMiLCJxdW90ZXMiLCJvcmdhbml6YXRpb24iLCJpZGVudGl0eSIsImFwaWNlbnRlciIsInByb2plY3RzIiwiYXNzZXRzIiwib3BlbmRhdGEiXSwiY2xpZW50X2lkIjoianNfb2F1dGgiLCJzdWIiOiJDQzZDN0E0RS0zMDczLTQ2MzgtOEZCRi1FQjJCMTAwODg0QkMiLCJhdXRoX3RpbWUiOjE3NjUxNzIyNDksImlkcCI6ImxvY2FsIiwibmFtZSI6IjEzOTg1Njg0NDg2Iiwic2lkIjoiRUU1M0JGM0ZBOTU0NEZCMzI2ODY3MjUyQkNGQkUxMzEiLCJpYXQiOjE3NjUxNzIyNDksInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJjbmFicyIsInF1b3RlcyIsIm9yZ2FuaXphdGlvbiIsImlkZW50aXR5IiwiYXBpY2VudGVyIiwicHJvamVjdHMiLCJwcm9kdWN0cyIsImFzc2V0cyIsIm9wZW5kYXRhIl0sImFtciI6WyJwd2QiXX0.KL9kHFF1xm5UZ8NlnjcdFB5PCiabHVVWxjHqKvmS3vePKx2MhST5z05EuxM8UOFrf4JpuD9EC978UIxXXoMNx2ioju6muLWctd8xjH5zCXt5voRTsNKbQ6BbyN86qS4jpUR5eOvfpZKsxgWTEBSpv7heCRiZn98O7yv4N3t8bP2dkwPJ3bpOoZ7hUCF8iSplzpEIDlFGC3nQwZgaz_6GYvhYXAlc245RbqBdbaglw6jhYgQTv58LyT5ahJ-FLaXVbn4NG860wPnjxTCP0me8AJiL8edPBifYBR4UvCTpxjcHmNrYY92P4Mc7bikHDQde4p5cAgmaqz47reH-04jBikwKt0Bhvtcz2NzlFRs8qNpgwH3DhFFOPacuO78G5Mb8Ou5gZ6P4AtKOopEPwbI42H_R5bnmiN4Va4TpTIK4xmu0JtFmy24X6o5IqX-sofQ06-2_SB4ddon2jEQh93eY8GXmzJzZgMgN3wrGPqabLCulCg-u6J5-U4NH3KKcg5z4ladyU70J-C-P42tFeSq7fFK0x-wTnX-BWVW-UVdFDJEbQ7xwLugri6U86FYBVGLR5VzI0qaGG29DH_7CtRAgMS29-KZXit4TWrcJaVOdC24R1HMrtSWJqOqKUMi6HiWrhVqL4IgLacLbPG8_qIv1zADWHVR3jdpFfEeys01RB1I
Connection: keep-alive
Content-Length: 0
Content-Type: application/x-www-form-urlencoded;charset=UTF-8
Cookie: CNABS_PassCode=13985684486
Host: www.cn-abs.com
Origin: https://www.cn-abs.com
Referer: https://www.cn-abs.com/sign.html
sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
Title: %E8%B5%84%E4%BA%A7%E8%AF%81%E5%88%B8%E5%8C%96%E5%88%86%E6%9E%90%E7%BD%91-ABS%E3%80%81REITs%E5%9C%A8%E7%BA%BF%E5%88%86%E6%9E%90%E5%B9%B3%E5%8F%B0
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36

步骤2: 搜索产品
    - 找到页面右上角的搜索框（type="text" class="ant-input" ）
    - 输入产品名称，例如: "深圳弗迪融资租赁有限公司2025年度第一期绿色资产支持票据"
    - 回车

回车时在F12 Network中看到的请求：
1. check
请求网址: https://www.cn-abs.com/apigateway/cnabs/account/invite/business/check?url=%2Findex.html%2F%23%2Fmain%2Fsearch%3Fkeyword%3D%25E6%25B7%25B1%25E5%259C%25B3%25E5%25BC%2597%25E8%25BF%25AA%25E8%259E%258D%25E8%25B5%2584%25E7%25A7%259F%25E8%25B5%2581%25E6%259C%2589%25E9%2599%2590%25E5%2585%25AC%25E5%258F%25B82025%25E5%25B9%25B4%25E5%25BA%25A6%25E7%25AC%25AC%25E4%25B8%2580%25E6%259C%259F%25E7%25BB%25BF%25E8%2589%25B2%25E8%25B5%2584%25E4%25BA%25A7%25E6%2594%25AF%25E6%258C%2581%25E7%25A5%25A8%25E6%258D%25AE&title=%E6%90%9C%E7%B4%A2%E7%BB%93%E6%9E%9C
请求方法: POST
状态代码: 200 OK

2. count
请求网址: https://www.cn-abs.com/apigateway/cnabs/account/internal-message/unread/count
请求方法: POST

3. popup
请求网址: https://www.cn-abs.com/apigateway/cnabs/account/invite/popup
请求方法: POST

4. pager
请求网址: https://www.cn-abs.com/apigateway/cnabs/global/search/pager
请求方法: POST
状态代码: 200 OK
远程地址: 120.26.229.199:443
引荐来源网址政策: strict-origin-when-cross-origin
Access-Control-Allow-Credentials: true
Access-Control-Allow-Headers: DNT,X-Mx-ReqToken,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Authorization,title
Access-Control-Allow-Methods: POST
Access-Control-Allow-Origin: https://www.cn-abs.com
Connection: keep-alive
Content-Length: 1489
Content-Type: application/json; charset=utf-8
Date: Mon, 08 Dec 2025 03:39:50 GMT
Server: openresty
Vary: Origin

请求标头：
Accept: application/json
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc2NTkyRTczMTFGMjRFMjg4QjVCQ0Q4QTU1RjU2QTg2MzFFOUNBQTNSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImRsa3VjeEh5VGlpTFc4MktWZlZxaGpIcHlxTSJ9.eyJuYmYiOjE3NjUxNzA2NDIsImV4cCI6MTc2NTI1NzA0MiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50LmNuLWFicy5jb20iLCJhdWQiOlsiY25hYnMiLCJwcm9kdWN0cyIsImNhbGMiLCJxdW90ZXMiLCJvcmdhbml6YXRpb24iLCJpZGVudGl0eSIsImFwaWNlbnRlciIsInByb2plY3RzIiwiYXNzZXRzIiwib3BlbmRhdGEiXSwiY2xpZW50X2lkIjoianNfb2F1dGgiLCJzdWIiOiJDQzZDN0E0RS0zMDczLTQ2MzgtOEZCRi1FQjJCMTAwODg0QkMiLCJhdXRoX3RpbWUiOjE3NjUxNzA2MzEsImlkcCI6ImxvY2FsIiwibmFtZSI6IjEzOTg1Njg0NDg2Iiwic2lkIjoiN0ZFODVGQTc0MEU2Q0Q5MTY2MDI4NkRDM0E1MUM3N0EiLCJpYXQiOjE3NjUxNzA2NDIsInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJjbmFicyIsInF1b3RlcyIsIm9yZ2FuaXphdGlvbiIsImlkZW50aXR5IiwiYXBpY2VudGVyIiwicHJvamVjdHMiLCJwcm9kdWN0cyIsImFzc2V0cyIsIm9wZW5kYXRhIl0sImFtciI6WyJwd2QiXX0.Ayf1hF4bnjukafLLj8uuPxVKwdGnh38embUwne-KpgWJh9Cbp6LtogsMHhpah28C1q31DJ10fO3obxYVMQaGnPKPG3GrRfVOTxw_zkt98fexzGgUALLPAZVtBKV1zCz5Qu3UJx6v4wbFVGxzxgu8XLtS3Wt0HzJs8IDcYN9Guloeo2WZ3C6S69W0uLW5tDlkuC7sIj16uxIMcU2U_9RYv9_yM6yuxDQCVGw-uqa-yQlGiVIemfMrBHJfekFkNeG1XMs9AH9z-7Afd0c0JNTI3LPYHX9DjKA9aPRaEwcIygdQpAzVPuLi5CZihdRL4eJa-aelGm30tiD4-HK4rQRPENh8K4fhWv4cIVKZqxhKtfO_AuVQtaHEHD05SS3i-kkNbz7Dfg4yeB63KeHRs89kYXj8DJmXKtCxiMyP7L7NsLPwFWaR3wxVbAiUfEo6yyDBHhjf0T_0rpg45sGtbAdHPeC2I1npTPNkKhdEmziVw67Z049KciN2-9PKiLmqu-3eGSUnR3hNm8yPuOgY_oVNQyZ9yTqXZ1mTrI2jk-aNjb4s7LGpWasy7FbVE7paGP4_ABkrv2kNJjRjjnpviWtSUD2X-xa8NtuZPo_fffanIU0Ios3STsui2Emupu28pYshDXG-e2aNmnTSERsDFlwhHc4i1Zw9NbuS-Rz2vFBP4pY
Connection: keep-alive
Content-Length: 316
Content-Type: application/x-www-form-urlencoded;charset=UTF-8
Cookie: CNABS_PassCode=13985684486; CNABS_PassCode=13985684486; cnabs=C705C3B4DA4CD916B751F47A68621B4B73563ACA71FC49A70E34971E8456555C67F7D1A2B456A75665ABC0F663B76E440E93F476E08BDB1B5FB2514C86107726DFECA6D8DFBCAB31389360643139063F610610F1; cnabs_web=SgS6RPVPU3UNPt6_EEz9S9HzfyPsXVrUW82Gr4GDznJpiqs-qCwYJE_xN7A3j-5unlL5DBhk-8KGvaf01yNoNwEHyj6TLTekZdf-SdsChQN98Rd2bPhVHnMxRgolaRz9ij5plFRTcU9LZPDERi6eyPRt1GmNo64HgsDg5Dnu5dOfpl9FjU7Gj-2hBs29-7l2hY96V-p66olqPJAJnL_C4eYvvaPHljAqHZyfFACXZeSd33oVyd9Ufos0jnRNRENA2jeDcTF4sYI26W29V7VFPlJTmdcVB43zt_tieAZzwcZ4q91fvXFfj1kv9A5AWNNjbHKpc1hodVftskiHnqoY1kfBT3c
Host: www.cn-abs.com
Origin: https://www.cn-abs.com
Referer: https://www.cn-abs.com/index.html
sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
Title: %E6%90%9C%E7%B4%A2%E7%BB%93%E6%9E%9C
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36

载荷：
keyword: 深圳弗迪融资租赁有限公司2025年度第一期绿色资产支持票据
search_type: 0
timestamp: 2025-12-08T03:39:38.849Z
page_index: 1
page_size: 30

响应：
{"code":200,"data":{"OrganizationPager":{"page_size":30,"current_page":1,"total_pages":0,"total_items":0,"items":[]},"DealPager":{"page_size":30,"current_page":1,"total_pages":1,"total_items":1,"items":[{"id":21363,"short_name":"弗迪ABN2025-1","full_name":"深圳弗迪融资租赁有限公司2025年度第一期绿色资产支持票据","deal_type":"融资租赁","total_offering":6784000000.0,"originators":null,"url":"?deal_id=21363"}]},"SecurityPager":{"page_size":30,"current_page":1,"total_pages":1,"total_items":2,"items":[{"id":56474,"code":"082580900.IB","short_name":"25弗迪租赁ABN001次级(绿色)","security_type":"次级","expected_maturity_date":"2030-07-12","deal_short_name":"弗迪ABN2025-1","deal_full_name":"深圳弗迪融资租赁有限公司2025年度第一期绿色资产支持票据","product_type":"资产支持票据","current_coupon":0.04,"current_rating":"NR","wal":4.5942922374429225,"url":"?security_id=56474"},{"id":56473,"code":"082580899.IB","short_name":"25弗迪租赁ABN001优先(绿色)","security_type":"优先级","expected_maturity_date":"2028-07-14","deal_short_name":"弗迪ABN2025-1","deal_full_name":"深圳弗迪融资租赁有限公司2025年度第一期绿色资产支持票据","product_type":"资产支持票据","current_coupon":0.017,"current_rating":"AAA","wal":2.599771689497717,"url":"?security_id=56473"}]},"ExpertPager":{"page_size":30,"current_page":1,"total_pages":0,"total_items":0,"items":[]}},"message":null,"status":"ok"}

** 注意：以上4个请求的请求标头中都有Authorization和Cookie：
Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc2NTkyRTczMTFGMjRFMjg4QjVCQ0Q4QTU1RjU2QTg2MzFFOUNBQTNSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImRsa3VjeEh5VGlpTFc4MktWZlZxaGpIcHlxTSJ9.eyJuYmYiOjE3NjUxNjgxNjIsImV4cCI6MTc2NTI1NDU2MiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50LmNuLWFicy5jb20iLCJhdWQiOlsiY25hYnMiLCJwcm9kdWN0cyIsImNhbGMiLCJxdW90ZXMiLCJvcmdhbml6YXRpb24iLCJpZGVudGl0eSIsImFwaWNlbnRlciIsInByb2plY3RzIiwiYXNzZXRzIiwib3BlbmRhdGEiXSwiY2xpZW50X2lkIjoianNfb2F1dGgiLCJzdWIiOiJDQzZDN0E0RS0zMDczLTQ2MzgtOEZCRi1FQjJCMTAwODg0QkMiLCJhdXRoX3RpbWUiOjE3NjUxNjgxNjIsImlkcCI6ImxvY2FsIiwibmFtZSI6IjEzOTg1Njg0NDg2Iiwic2lkIjoiRUNCQ0U3MzcwMjNEQ0EwREJDQTU4NzAwNDExMEJDRkQiLCJpYXQiOjE3NjUxNjgxNjIsInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJjbmFicyIsInF1b3RlcyIsIm9yZ2FuaXphdGlvbiIsImlkZW50aXR5IiwiYXBpY2VudGVyIiwicHJvamVjdHMiLCJwcm9kdWN0cyIsImFzc2V0cyIsIm9wZW5kYXRhIl0sImFtciI6WyJwd2QiXX0.D9t_cUhnWODhn38Dhns13K-1MHDtjgi02bVgxnoC2rcLdkTIkiMC2VYDPUVeg075ru1PzPBT76Nsby5TodpRXDPRzPNqHAchXLM5Z_Vc6vWonh9DQckbX6tngS737M0jyQTwj_kxC3YtwGBCqBkAZO4Mk7rS5O5pqJg2vOPCZqvG1-IheI7QFW5hULYCgMXEeEGVWo_n4oT0zbX6Iej6MxZnGttqzVieVSwSibl8wyc6qgZpY2bmKHRjBvdvu92NBose5rXO_0w7BDRdhYHMHc_oP7oo-JdOWwRiWH-pYiqxOGxlf_IDPIEzFf4FGjAj5xpp7ZqxV6ISRHJKMA0_x_AEFVnot4RfuIEtTKwjWr-8pnLaYaa_9Hu_YyPdMazQC88OjjzCcsyhpVfymkN32xCv1v9P0VhWwrKRD5dhg-B5VH3n_qpf3QHaMuF-Q6MCn1ujKL2jZpiCKoRMYMkEZ4th3frnUacV5tbK7Gcok60ttSOsYnOmViWCPZrxU6JJByFSIXi45hspoZzQxND9PpRoqRAjv6bU4m87Q0tFZDPZyw42WICagMgeXZ6r9GP_RZKLjmHw8JL5Dx5br9JwgLtnm94Fp5nqFcd5otLhMIDfnxcq24SBGTPhYTJF1n_OeYnSpfVPIkLS5OogvEiWANyH151ULtT2gnkek4ZzOtQ

Cookie: CNABS_PassCode=13985684486; CNABS_PassCode=13985684486; cnabs=91F249F56B315DB31B43730D95BDF97C13F589D86E4B9FF49C60E55192AB7D89C0A83CF996C0FB1C678D38D54427644CA1D468EC0EDE9CC313E9DD65AEF10703B8CF481AAF97417DEAABD090AA61E03F23CAA2FA; cnabs_web=9weJBxdxUxW9rodsc5B5I3iQFl1hsulYLJMGxCwshKTLAnapoPhxsslyxJ5THBh3jsUS6zcKyB91egYgzkO4PuN0KFM3XTKAhIxqk9darxdEKgXnhZgfgwMKrF8aXN5w6ZTOtktRHtFnKGegu--NQHgRbjlD9a5R8E-vFcRqhQcSYV7bCoRJORNf16_32Dz9xzLUul8KTIL3FdIo7SSgHBqFI7kKtGPAH1adgpW0iP0CdSTPsxzdDx5J2WxRRyZ3E-vOJ5DxrWWeu2_OxQSizYunuuQDud1PHXqWTpJVLLRsxkvw4b1si5fFtlTxP_RV6KYERa8JIOc6xOitVur2VozBQzQ


步骤3: 进入产品页面
    - 在搜索结果页面，找到"产品"栏目
    - 点击第一条"产品全称"结果，进入产品详情页面

备注：
点击产品全称时，我观察到两个请求：

1. 请求网址: https://www.cn-abs.com/apigateway/cnabs/account/invite/business/check?url=%2Fproduct.html%2F%23%2Fdetail%2Fsecurity%3Fdeal_id%3D21363&title=%E4%BA%A7%E5%93%81%E8%AF%81%E5%88%B8
请求方法: POST
载荷：
url: /product.html/#/detail/security?deal_id=21363
title: 产品证券


2. 请求网址: https://www.cn-abs.com/apigateway/cnabs/deal/security-detail?deal_id=21363&page_index=1
请求方法: POST
载荷：
deal_id: 21363
page_index: 1

请求标头：
Accept: application/json
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc2NTkyRTczMTFGMjRFMjg4QjVCQ0Q4QTU1RjU2QTg2MzFFOUNBQTNSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6ImRsa3VjeEh5VGlpTFc4MktWZlZxaGpIcHlxTSJ9.eyJuYmYiOjE3NjUxNzA2NDIsImV4cCI6MTc2NTI1NzA0MiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50LmNuLWFicy5jb20iLCJhdWQiOlsiY25hYnMiLCJwcm9kdWN0cyIsImNhbGMiLCJxdW90ZXMiLCJvcmdhbml6YXRpb24iLCJpZGVudGl0eSIsImFwaWNlbnRlciIsInByb2plY3RzIiwiYXNzZXRzIiwib3BlbmRhdGEiXSwiY2xpZW50X2lkIjoianNfb2F1dGgiLCJzdWIiOiJDQzZDN0E0RS0zMDczLTQ2MzgtOEZCRi1FQjJCMTAwODg0QkMiLCJhdXRoX3RpbWUiOjE3NjUxNzA2MzEsImlkcCI6ImxvY2FsIiwibmFtZSI6IjEzOTg1Njg0NDg2Iiwic2lkIjoiN0ZFODVGQTc0MEU2Q0Q5MTY2MDI4NkRDM0E1MUM3N0EiLCJpYXQiOjE3NjUxNzA2NDIsInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJjbmFicyIsInF1b3RlcyIsIm9yZ2FuaXphdGlvbiIsImlkZW50aXR5IiwiYXBpY2VudGVyIiwicHJvamVjdHMiLCJwcm9kdWN0cyIsImFzc2V0cyIsIm9wZW5kYXRhIl0sImFtciI6WyJwd2QiXX0.Ayf1hF4bnjukafLLj8uuPxVKwdGnh38embUwne-KpgWJh9Cbp6LtogsMHhpah28C1q31DJ10fO3obxYVMQaGnPKPG3GrRfVOTxw_zkt98fexzGgUALLPAZVtBKV1zCz5Qu3UJx6v4wbFVGxzxgu8XLtS3Wt0HzJs8IDcYN9Guloeo2WZ3C6S69W0uLW5tDlkuC7sIj16uxIMcU2U_9RYv9_yM6yuxDQCVGw-uqa-yQlGiVIemfMrBHJfekFkNeG1XMs9AH9z-7Afd0c0JNTI3LPYHX9DjKA9aPRaEwcIygdQpAzVPuLi5CZihdRL4eJa-aelGm30tiD4-HK4rQRPENh8K4fhWv4cIVKZqxhKtfO_AuVQtaHEHD05SS3i-kkNbz7Dfg4yeB63KeHRs89kYXj8DJmXKtCxiMyP7L7NsLPwFWaR3wxVbAiUfEo6yyDBHhjf0T_0rpg45sGtbAdHPeC2I1npTPNkKhdEmziVw67Z049KciN2-9PKiLmqu-3eGSUnR3hNm8yPuOgY_oVNQyZ9yTqXZ1mTrI2jk-aNjb4s7LGpWasy7FbVE7paGP4_ABkrv2kNJjRjjnpviWtSUD2X-xa8NtuZPo_fffanIU0Ios3STsui2Emupu28pYshDXG-e2aNmnTSERsDFlwhHc4i1Zw9NbuS-Rz2vFBP4pY
Connection: keep-alive
Content-Length: 0
Content-Type: application/x-www-form-urlencoded;charset=UTF-8
Cookie: CNABS_PassCode=13985684486; CNABS_PassCode=13985684486; cnabs=C705C3B4DA4CD916B751F47A68621B4B73563ACA71FC49A70E34971E8456555C67F7D1A2B456A75665ABC0F663B76E440E93F476E08BDB1B5FB2514C86107726DFECA6D8DFBCAB31389360643139063F610610F1; cnabs_web=SgS6RPVPU3UNPt6_EEz9S9HzfyPsXVrUW82Gr4GDznJpiqs-qCwYJE_xN7A3j-5unlL5DBhk-8KGvaf01yNoNwEHyj6TLTekZdf-SdsChQN98Rd2bPhVHnMxRgolaRz9ij5plFRTcU9LZPDERi6eyPRt1GmNo64HgsDg5Dnu5dOfpl9FjU7Gj-2hBs29-7l2hY96V-p66olqPJAJnL_C4eYvvaPHljAqHZyfFACXZeSd33oVyd9Ufos0jnRNRENA2jeDcTF4sYI26W29V7VFPlJTmdcVB43zt_tieAZzwcZ4q91fvXFfj1kv9A5AWNNjbHKpc1hodVftskiHnqoY1kfBT3c
Host: www.cn-abs.com
Origin: https://www.cn-abs.com
Referer: https://www.cn-abs.com/product.html
sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
Title: %E4%BA%A7%E5%93%81%E8%AF%81%E5%88%B8
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36

响应：
{"status":"ok","code":200,"data":{"rating_column_name":"联合资信","ref_market_interest_type":"中期票据","note_list":[{"note_detail":{"deal_id":21363,"note_id":56473,"security_name":"25弗迪租赁ABN001优先(绿色)","security_code":"082580899.IB","description":"A","security_type":"优先级","notional":6127000000.0,"principal":6127000000.0,"coupon":0.017,"refer_coupon":0.023003933,"refer_interest":-0.0060039329999999995,"current_coupon":0.017,"current_refer_coupon":0.023003933,"current_refer_interest":-0.0060039329999999995,"repayment_of_principal":"过手","coupon_type":"固定","rating_pause":"AAA","is_fixed_coupon":true,"base_coupon":null,"base_interest":null,"base_coupon_description":"","issuing_coupon_range":"","principal_pct":0.90315448113207553,"current_rating_pause":"AAA","clean_price":null,"dirty_price":null,"notional_pct":0.90315448113207553,"is_equity":false,"cdr":null,"principal_coverage":null,"interest_coverage":null,"expected_maturity_date":"2028-07-14","settlement_date":null,"remaining_principal_amount":1.0,"expected_rate_securities":null,"frequency_chinese":"季付","initial_wal_legal":null,"current_wal":null,"expected_life":2.7417808219178084,"current_life":"2.5998","coupon_str":"1.7%","current_coupon_str":"1.7%","notional_str":"612,700.00","notional_pct_str":"90.32%","principal_str":"612,700.00","principal_pct_str":"90.32%","refer_interest_str":"-0.6%","current_refer_interest_str":"-0.6%"},"ratings":[{"rating_date":"初始评级","rating":"AAA","rating_agency":"联合资信","rating_agency_fullname":"联合资信评估有限公司","is_original_rating":true}],"payment_history":[]},{"note_detail":{"deal_id":21363,"note_id":56474,"security_name":"25弗迪租赁ABN001次级(绿色)","security_code":"082580900.IB","description":"Sub","security_type":"次级","notional":657000000.0,"principal":657000000.0,"coupon":0.04,"refer_coupon":null,"refer_interest":null,"current_coupon":0.04,"current_refer_coupon":null,"current_refer_interest":null,"repayment_of_principal":"过手","coupon_type":null,"rating_pause":"NR","is_fixed_coupon":true,"base_coupon":null,"base_interest":null,"base_coupon_description":"","issuing_coupon_range":"","principal_pct":0.096845518867924529,"current_rating_pause":"NR","clean_price":null,"dirty_price":null,"notional_pct":0.096845518867924529,"is_equity":true,"cdr":null,"principal_coverage":null,"interest_coverage":null,"expected_maturity_date":"2030-07-12","settlement_date":null,"remaining_principal_amount":1.0,"expected_rate_securities":null,"frequency_chinese":"季付","initial_wal_legal":null,"current_wal":null,"expected_life":4.7363013698630141,"current_life":"4.5943","coupon_str":"4%","current_coupon_str":"4%","notional_str":"65,700.00","notional_pct_str":"9.68%","principal_str":"65,700.00","principal_pct_str":"9.68%","refer_interest_str":"--","current_refer_interest_str":"--"},"ratings":[{"rating_date":"初始评级","rating":"NR","rating_agency":"联合资信","rating_agency_fullname":"联合资信评估有限公司","is_original_rating":true}],"payment_history":[]}]}}


步骤4: 获取证券信息
    - 在产品页面左侧导航栏，点击"产品证券"栏目
    - 右侧显示证券信息
    - 顶部有各证券的标签（如果有4个证券，就有4个标签）
    - 点击每个标签，获取下方"产品证券"表格中的数据

备注：似乎不用点击这些标签了，各证券数据似乎已经在https://www.cn-abs.com/apigateway/cnabs/deal/security-detail?deal_id=21363&page_index=1请求的返回数据中提供

================================================================================
需要抓取的字段（产品证券表格）
================================================================================

    | 字段名称         | 说明                    |
    |-----------------|------------------------|
    | 证券名称         | 证券的全称              |
    | 证券代码         | 证券的唯一标识码         |
    | 发行量           | 发行的总量              |
    | 还本方式         | 本金偿还方式            |
    | 类型            | 证券类型（优先级/次级等）  |
    | 发行利率         | 初始发行时的利率         |
    | 当前利率         | 当前执行的利率           |
    | 预计到期日       | 预计的到期日期           |
    | 联合资信(原始)
    | 联合资信(当前)

将抓取到的证券信息，加上“产品全称”字段（放在第0列），写入到一个excel文件
（如果某产品有3个证券，那么该产品在excel中有三行）

================================================================================
技术实现说明
================================================================================

由于无法直接访问网站，本脚本采用探索式方法：
1. 尝试多个常见的API路径格式
2. 记录所有请求和响应
3. 分析页面源码查找API线索
4. 根据实际响应调整爬取逻辑

运行本脚本后，请将日志发送给开发人员分析，以确定：
- 正确的登录接口和参数格式
- 搜索API的路径和参数
- 产品详情和证券数据的API路径

================================================================================
"""

import requests
import json
import time
import sys
import os
import pandas as pd
from datetime import datetime
from urllib.parse import quote, urlencode

# ==================== 配置 ====================
CNABS_USERNAME = "18085157187"
CNABS_PASSWORD = "Password01"
BASE_URL = "https://www.cn-abs.com"
ACCOUNT_URL = "https://account.cn-abs.com"

# API接口（根据实际观察）
API_SEARCH = f"{BASE_URL}/apigateway/cnabs/global/search/pager"
API_SECURITY_DETAIL = f"{BASE_URL}/apigateway/cnabs/deal/security-detail"

# 登录相关API（account.cn-abs.com）
LOGIN_PAGE_URL = "https://account.cn-abs.com/account.html"
# 验证码接口：GET请求，返回image/png，验证码key通过Set-Cookie: CNABS3_Vcode_Login返回
API_CAPTCHA = f"{ACCOUNT_URL}/api/global/captcha"
API_LOGIN = f"{ACCOUNT_URL}/api/account/login"  # 登录接口（需要观察）

# 输出文件
OUTPUT_EXCEL = "cnabs_securities.xlsx"
SUCCESS_LOG = "successful_products.log"

# 请求头（不设置固定Content-Type，让requests根据data/json参数自动设置）
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/index.html",
}


class CNABSClient:
    """cn-abs.com 客户端 - 基于实际API观察"""
    
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.token = None
        self.is_logged_in = False
    
    def _log(self, message, level="INFO"):
        """打印日志"""
        print(f"[{level}] {message}")
    
    def login_with_captcha(self):
        """
        登录流程（需要人工输入验证码）
        
        登录页面: https://account.cn-abs.com/account.html
        页面元素:
        - 用户名输入框: class="ant-input ant-input-lg"
        - 密码输入框: class="ant-input ant-input-lg"
        - 验证码图片: class="abs-captcha-code-img"
        - 验证码输入框: class="ant-input ant-input-lg"
        - 用户协议勾选: class="ant-checkbox-input"
        - 登录按钮: type="submit" class="ant-btn ant-btn-primary"
        """
        self._log("=" * 60)
        self._log("开始登录流程")
        self._log("=" * 60)
        
        # 步骤1: 访问登录页面，获取初始Cookie
        self._log("步骤1: 访问登录页面...")
        response, _ = self._request("GET", LOGIN_PAGE_URL)
        if not response or response.status_code != 200:
            self._log("✗ 无法访问登录页面", "ERROR")
            return False
        
        # 步骤2: 获取验证码图片
        self._log("步骤2: 获取验证码...")
        captcha_path, captcha_key = self._get_captcha()
        
        if not captcha_path:
            self._log("✗ 无法获取验证码", "ERROR")
            return False
        
        # 步骤3: 人工输入验证码
        print("\n" + "=" * 60)
        print(f"📷 验证码图片已保存到: {captcha_path}")
        print("请打开图片查看验证码，然后在此输入")
        print("=" * 60)
        captcha_code = input("请输入验证码: ").strip()
        
        if not captcha_code:
            self._log("✗ 未输入验证码", "ERROR")
            return False
        
        # 步骤4: 提交登录
        self._log("步骤4: 提交登录...")
        success = self._submit_login(captcha_code, captcha_key)
        
        if not success:
            self._log("✗ 登录失败", "ERROR")
            return False
        
        self._log("✓ 登录成功!")
        
        # 步骤5: 完成OAuth回调，同步登录状态到 www.cn-abs.com
        self._log("步骤5: 同步登录状态到主站...")
        if self._complete_oauth_callback():
            self._log("✓ 登录状态同步成功!")
            self.is_logged_in = True
            return True
        else:
            self._log("⚠️ OAuth回调失败，尝试继续...", "WARN")
            self.is_logged_in = True  # 仍然标记为已登录，尝试继续
            return True
    
    def _complete_oauth_callback(self):
        """
        完成OAuth回调，将登录状态同步到 www.cn-abs.com
        
        流程：
        1. 访问 /connect/authorize/callback 获取302重定向
        2. 从Location头中提取access_token
        3. 设置Authorization: Bearer xxx 请求头
        
        注意：state和nonce需要与登录页面URL中的一致
        """
        import uuid
        import re
        
        # 生成state和nonce（服务器会验证，但登录成功后应该接受新的）
        state = uuid.uuid4().hex
        nonce = uuid.uuid4().hex
        
        # OAuth授权URL（与浏览器观察到的一致）
        authorize_url = (
            f"{ACCOUNT_URL}/connect/authorize/callback"
            f"?clientid=js_oauth"
            f"&client_id=js_oauth"
            f"&redirecturi=https%3A%2F%2Fwww.cn-abs.com%2Fsign.html%23%2Fcallback%3F"
            f"&redirect_uri=https%3A%2F%2Fwww.cn-abs.com%2Fsign.html%23%2Fcallback%3F"
            f"&responsetype=id_token%20token"
            f"&response_type=id_token%20token"
            f"&scope=openid%20profile%20cnabs%20quotes%20organization%20identity%20apicenter%20projects%20products%20assets%20opendata"
            f"&state={state}"
            f"&nonce={nonce}"
        )
        
        self._log(f"访问OAuth授权URL...")
        
        try:
            # 访问授权URL，会返回302重定向到回调URL（带token）
            response = self.session.get(authorize_url, allow_redirects=False, timeout=30)
            self._log(f"响应状态: {response.status_code}")
            
            if response.status_code == 302:
                # 获取重定向URL
                redirect_url = response.headers.get("Location", "")
                self._log(f"重定向到: {redirect_url[:100]}...")
                
                # 从重定向URL中提取access_token
                if "access_token=" in redirect_url:
                    token_match = re.search(r'access_token=([^&]+)', redirect_url)
                    if token_match:
                        self.token = token_match.group(1)
                        self._log(f"✓ 获取到access_token (长度: {len(self.token)})")
                        
                        # 设置Authorization头
                        self.session.headers["Authorization"] = f"Bearer {self.token}"
                        
                        # 调用login/oauth API获取cnabs和cnabs_web Cookie
                        self._log("调用login/oauth获取Cookie...")
                        try:
                            oauth_url = f"{BASE_URL}/apigateway/cnabs/account/login/oauth"
                            oauth_headers = {
                                "Accept": "application/json",
                                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                                "Content-Length": "0",
                                "Origin": BASE_URL,
                                "Referer": f"{BASE_URL}/sign.html",
                            }
                            oauth_resp = self.session.post(oauth_url, headers=oauth_headers, data="", timeout=30)
                            self._log(f"login/oauth响应: {oauth_resp.status_code}")
                            
                            # 打印当前所有Cookie
                            cookies = [c.name for c in self.session.cookies]
                            self._log(f"当前Cookies: {cookies}")
                            
                            # 验证是否获取到cnabs Cookie
                            if 'cnabs' in cookies and 'cnabs_web' in cookies:
                                self._log("✓ 成功获取cnabs和cnabs_web Cookie!")
                            else:
                                self._log("⚠ 未获取到cnabs Cookie", "WARN")
                        except Exception as e:
                            self._log(f"login/oauth失败: {e}", "WARN")
                        
                        return True
                else:
                    self._log(f"重定向URL中没有access_token", "WARN")
            
            # 如果不是302，打印更多信息
            self._log(f"响应头: {dict(response.headers)}")
            if response.text:
                self._log(f"响应内容: {response.text[:500]}")
                        
        except Exception as e:
            self._log(f"OAuth回调失败: {e}", "ERROR")
        
        return False
    
    def _get_captcha(self):
        """
        获取验证码图片
        
        接口: GET https://account.cn-abs.com/api/global/captcha?type=Login&t=随机数
        返回: image/png 图片
        验证码Key: 通过 Set-Cookie: CNABS3_Vcode_Login=xxx 返回
        """
        import random
        
        # 构建验证码URL（带随机数防缓存）
        t = random.random()
        url = f"{API_CAPTCHA}?type=Login&t={t}"
        
        self._log(f"获取验证码: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                
                if "image" in content_type:
                    # 保存验证码图片
                    captcha_path = "captcha.png"
                    with open(captcha_path, "wb") as f:
                        f.write(response.content)
                    self._log(f"✓ 验证码图片已保存: {captcha_path}")
                    
                    # 验证码Key从Cookie获取: CNABS3_Vcode_Login
                    captcha_key = self.session.cookies.get("CNABS3_Vcode_Login")
                    self._log(f"验证码Key (CNABS3_Vcode_Login): {captcha_key}")
                    
                    return captcha_path, captcha_key
                else:
                    self._log(f"✗ 返回类型不是图片: {content_type}", "ERROR")
            else:
                self._log(f"✗ 请求失败: {response.status_code}", "ERROR")
                
        except Exception as e:
            self._log(f"获取验证码失败: {e}", "ERROR")
        
        return None, None
    
    def _submit_login(self, captcha_code, captcha_key=None):
        """
        提交登录请求
        
        接口: POST https://account.cn-abs.com/api/account/login
        载荷: {"user_name": "xxx", "password": "xxx", "captcha": "xxxx"}
        成功标志: Set-Cookie 包含 idsrv.session 和 .AspNetCore.Identity.Application
        """
        url = f"{ACCOUNT_URL}/api/account/login"
        
        login_data = {
            "user_name": self.username,
            "password": self.password,
            "captcha": captcha_code
        }
        
        self._log(f"提交登录: {url}")
        self._log(f"载荷: user_name={self.username}, captcha={captcha_code}")
        
        try:
            response = self.session.post(url, json=login_data, timeout=30)
            self._log(f"响应状态: {response.status_code}")
            
            # 打印响应内容（调试用）
            try:
                resp_data = response.json()
                self._log(f"响应内容: {resp_data}")
            except:
                self._log(f"响应文本: {response.text[:200]}")
            
            # 检查登录是否成功
            if self._check_login_response(response):
                return True
            else:
                self._log("✗ 登录验证失败", "ERROR")
                return False
                
        except Exception as e:
            self._log(f"登录请求失败: {e}", "ERROR")
            return False
    
    def _check_login_response(self, response):
        """
        检查登录响应是否成功
        
        成功标志: 
        1. Set-Cookie 包含 idsrv.session
        2. Set-Cookie 包含 .AspNetCore.Identity.Application
        """
        if not response or response.status_code != 200:
            return False
        
        # 检查是否有登录成功的Cookie
        cookies = self.session.cookies.get_dict()
        self._log(f"当前Cookies: {list(cookies.keys())}")
        
        # 检查关键Cookie
        has_idsrv = "idsrv.session" in cookies or "idsrv" in str(cookies)
        has_aspnet = ".AspNetCore.Identity.Application" in cookies
        
        if has_idsrv or has_aspnet:
            self._log("✓ 检测到登录成功Cookie")
            return True
        
        # 也检查响应JSON
        try:
            data = response.json()
            if data.get("code") in [0, 200, "0", "200"]:
                return True
            if data.get("success") == True:
                return True
        except:
            pass
        
        return False
    
    def _request(self, method, url, **kwargs):
        """发送请求并打印调试信息"""
        self._log(f"请求: {method} {url}")
        if 'json' in kwargs:
            self._log(f"请求体: {json.dumps(kwargs['json'], ensure_ascii=False)[:300]}")
        
        # 打印关键请求头
        auth_header = self.session.headers.get("Authorization", "无")
        self._log(f"Authorization: {auth_header[:80]}..." if len(auth_header) > 80 else f"Authorization: {auth_header}")
        
        try:
            response = self.session.request(method, url, timeout=30, **kwargs)
            self._log(f"响应状态: {response.status_code}")
            
            # 尝试解析JSON
            try:
                data = response.json()
                self._log(f"响应JSON: {json.dumps(data, ensure_ascii=False)[:500]}...")
                return response, data
            except:
                self._log(f"响应文本: {response.text[:300]}")
                return response, None
                
        except Exception as e:
            self._log(f"请求失败: {e}", "ERROR")
            return None, None
    
    def search_product(self, keyword):
        """
        搜索产品
        API: POST https://www.cn-abs.com/apigateway/cnabs/global/search/pager
        Content-Type: application/x-www-form-urlencoded (不是JSON!)
        """
        self._log("=" * 60)
        self._log(f"搜索产品: {keyword}")
        self._log("=" * 60)
        
        # 注意：浏览器使用的是 form-urlencoded 格式，不是 JSON
        payload = {
            "keyword": keyword,
            "search_type": 0,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "page_index": 1,
            "page_size": 30
        }
        
        # 使用 data= 而不是 json=，这样会自动使用 form-urlencoded
        response, data = self._request("POST", API_SEARCH, data=payload)
        
        if response and response.status_code == 200 and data:
            if data.get("code") == 200 and data.get("status") == "ok":
                self._log("✓ 搜索成功!")
                return data.get("data")
        
        self._log("✗ 搜索失败", "ERROR")
        return None
    
    def get_security_detail(self, deal_id, page_index=1):
        """
        获取产品证券详情
        API: POST https://www.cn-abs.com/apigateway/cnabs/deal/security-detail?deal_id=XXX&page_index=1
        
        关键：Content-Length: 0，参数在URL中，请求体为空！
        """
        self._log("=" * 60)
        self._log(f"获取证券详情: deal_id={deal_id}")
        self._log("=" * 60)
        
        # 先调用 check 接口（模拟浏览器行为）
        check_url = f"{BASE_URL}/apigateway/cnabs/account/invite/business/check"
        check_params = {
            "url": f"/product.html/#/detail/security?deal_id={deal_id}",
            "title": "产品证券"
        }
        try:
            self.session.post(check_url, params=check_params, timeout=10)
        except:
            pass  # 忽略check请求的错误
        
        # 获取证券详情 - 参数在URL中，请求体为空
        url = f"{API_SECURITY_DETAIL}?deal_id={deal_id}&page_index={page_index}"
        
        # 打印当前Cookie（调试）
        cookies = [c.name for c in self.session.cookies]
        self._log(f"当前Cookies: {cookies}")
        
        # POST请求，但请求体为空（Content-Length: 0）
        response, data = self._request("POST", url)
        
        if response and response.status_code == 200 and data:
            if data.get("code") == 200 and data.get("status") == "ok":
                self._log("✓ 获取证券详情成功!")
                return data.get("data")
        
        self._log("✗ 获取证券详情失败", "ERROR")
        return None
    
    def parse_security_info(self, security_data):
        """
        解析证券信息，提取需要的字段
        
        返回字段映射：
        - security_name -> 证券名称
        - security_code -> 证券代码
        - notional -> 发行量
        - repayment_of_principal -> 还本方式
        - security_type -> 类型
        - coupon -> 发行利率
        - current_coupon -> 当前利率
        - expected_maturity_date -> 预计到期日
        - rating_pause -> 评级（原始）
        - current_rating_pause -> 评级（当前）
        """
        if not security_data:
            return []
        
        results = []
        note_list = security_data.get("note_list", [])
        rating_agency = security_data.get("rating_column_name", "未知评级机构")
        
        for note in note_list:
            detail = note.get("note_detail", {})
            ratings = note.get("ratings", [])
            
            # 获取原始评级
            original_rating = "N/A"
            current_rating = detail.get("current_rating_pause", "N/A")
            for r in ratings:
                if r.get("is_original_rating"):
                    original_rating = r.get("rating", "N/A")
                    break
            
            security_info = {
                "证券名称": detail.get("security_name", ""),
                "证券代码": detail.get("security_code", ""),
                "发行量": detail.get("notional", 0),
                "发行量(万元)": detail.get("notional_str", ""),
                "还本方式": detail.get("repayment_of_principal", ""),
                "类型": detail.get("security_type", ""),
                "发行利率": detail.get("coupon", 0),
                "发行利率(%)": detail.get("coupon_str", ""),
                "当前利率": detail.get("current_coupon", 0),
                "当前利率(%)": detail.get("current_coupon_str", ""),
                "预计到期日": detail.get("expected_maturity_date", ""),
                f"{rating_agency}（原始）": original_rating,
                f"{rating_agency}（当前）": current_rating,
                # 额外字段
                "deal_id": detail.get("deal_id"),
                "note_id": detail.get("note_id"),
                "票息类型": detail.get("coupon_type", ""),
                "付息频率": detail.get("frequency_chinese", ""),
            }
            results.append(security_info)
        
        return results
    
    def fetch_product_securities(self, product_name):
        """
        完整流程：搜索产品 -> 获取deal_id -> 获取证券详情 -> 解析数据
        """
        self._log("=" * 60)
        self._log(f"开始获取产品证券信息: {product_name}")
        self._log("=" * 60)
        
        # 步骤1: 搜索产品
        search_result = self.search_product(product_name)
        if not search_result:
            return None
        
        # 从搜索结果中获取deal_id
        deal_pager = search_result.get("DealPager", {})
        items = deal_pager.get("items", [])
        
        if not items:
            self._log("✗ 未找到匹配的产品", "ERROR")
            return None
        
        # 取第一个匹配的产品
        product = items[0]
        deal_id = product.get("id")
        product_name_found = product.get("full_name")
        
        self._log(f"找到产品: {product_name_found} (deal_id={deal_id})")
        
        # 步骤2: 获取证券详情
        security_data = self.get_security_detail(deal_id)
        if not security_data:
            return None
        
        # 步骤3: 解析证券信息
        securities = self.parse_security_info(security_data)
        
        return {
            "product": product,
            "securities": securities,
            "raw_data": security_data
        }


def print_securities_table(securities):
    """打印证券信息表格"""
    if not securities:
        print("没有证券数据")
        return
    
    print("\n" + "=" * 100)
    print("产品证券信息")
    print("=" * 100)
    
    for i, sec in enumerate(securities, 1):
        print(f"\n--- 证券 {i} ---")
        for key, value in sec.items():
            if not key.startswith("deal_id") and not key.startswith("note_id"):
                print(f"  {key}: {value}")


def load_successful_products():
    """加载已成功处理的产品列表"""
    if os.path.exists(SUCCESS_LOG):
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_successful_product(product_name):
    """记录成功处理的产品（追加模式）"""
    with open(SUCCESS_LOG, "a", encoding="utf-8") as f:
        f.write(product_name + "\n")


def main(list_product_names, skip_login=False):
    """
    主函数 - 批量抓取产品证券信息
    
    流程：
    1. 登录（如果需要）
    2. 创建客户端（复用session）
    3. 遍历产品列表，依次抓取证券信息
    4. 每成功一个，立即记录到log文件（支持断点续传）
    5. 最后将所有结果写入Excel
    """
    print("=" * 80)
    print("cn-abs.com 证券信息批量抓取")
    print("=" * 80)
    print(f"待处理产品数: {len(list_product_names)}")
    print(f"输出文件: {OUTPUT_EXCEL}")
    print(f"成功日志: {SUCCESS_LOG}")
    print("=" * 80)
    
    # 创建客户端（循环外创建，复用session）
    client = CNABSClient(CNABS_USERNAME, CNABS_PASSWORD)
    
    # 登录流程
    if not skip_login:
        print("\n" + "=" * 80)
        print("登录选项:")
        print("1. 人工输入验证码登录")
        print("2. 跳过登录（先测试API是否公开）")
        print("=" * 80)
        choice = input("请选择 (1/2): ").strip()
        
        if choice == "1":
            if not client.login_with_captcha():
                print("\n⚠️ 登录失败！")
                retry = input("是否继续尝试不登录抓取？(y/n): ").strip().lower()
                if retry != 'y':
                    print("退出程序")
                    return
        else:
            print("\n跳过登录，直接尝试抓取...")
    
    # 初始化结果列表
    all_results = []
    success_count = 0
    fail_count = 0
    
    # 遍历产品列表
    for i, product_name in enumerate(list_product_names, 1):
        print(f"\n[{i}/{len(list_product_names)}] 处理: {product_name}")
        
        try:
            # 获取产品证券信息
            result = client.fetch_product_securities(product_name)
            
            if result and result.get("securities"):
                # 打印产品信息
                product = result["product"]
                print(f"  ✓ 找到产品: {product.get('short_name')}")
                print(f"    证券数量: {len(result['securities'])}")
                
                # 将每个证券添加到结果列表，并添加产品全称列
                for sec in result["securities"]:
                    row = {"产品全称": product.get("full_name", product_name)}
                    # 按指定顺序添加字段
                    row["证券名称"] = sec.get("证券名称", "")
                    row["证券代码"] = sec.get("证券代码", "")
                    row["发行量"] = sec.get("发行量", 0)
                    row["发行量(万元)"] = sec.get("发行量(万元)", "")
                    row["还本方式"] = sec.get("还本方式", "")
                    row["类型"] = sec.get("类型", "")
                    row["发行利率"] = sec.get("发行利率", 0)
                    row["发行利率(%)"] = sec.get("发行利率(%)", "")
                    row["当前利率"] = sec.get("当前利率", 0)
                    row["当前利率(%)"] = sec.get("当前利率(%)", "")
                    row["预计到期日"] = sec.get("预计到期日", "")
                    # 评级字段（动态获取）
                    for key in sec:
                        if "原始" in key or "当前" in key:
                            if key not in row:
                                row[key] = sec[key]
                    all_results.append(row)
                
                # 立即记录成功（支持断点续传）
                save_successful_product(product_name)
                success_count += 1
            else:
                print(f"  ✗ 未找到数据")
                fail_count += 1
                
        except Exception as e:
            print(f"  ✗ 处理失败: {e}")
            fail_count += 1
            continue  # 继续下一个，不中断
        
        # 请求间隔，避免过快
        time.sleep(0.5)
    
    # 保存结果到Excel（追加模式）
    print("\n" + "=" * 80)
    print("保存结果...")
    print("=" * 80)
    
    if all_results:
        df_new = pd.DataFrame(all_results)
        
        # 如果文件已存在，读取并追加
        if os.path.exists(OUTPUT_EXCEL):
            try:
                df_existing = pd.read_excel(OUTPUT_EXCEL, engine='openpyxl')
                df_results = pd.concat([df_existing, df_new], ignore_index=True)
                print(f"  已有数据: {len(df_existing)} 行")
                print(f"  新增数据: {len(df_new)} 行")
            except Exception as e:
                print(f"  ⚠️ 读取已有文件失败: {e}，将覆盖写入")
                df_results = df_new
        else:
            df_results = df_new
        
        df_results.to_excel(OUTPUT_EXCEL, index=False, engine='openpyxl')
        print(f"✓ 结果已保存到: {OUTPUT_EXCEL}")
        print(f"  总行数: {len(df_results)}")
    else:
        print("⚠️ 没有数据可保存")
    
    # 打印统计
    print("\n" + "=" * 80)
    print("处理完成!")
    print("=" * 80)
    print(f"成功: {success_count}")
    print(f"失败: {fail_count}")
    print(f"总计: {len(list_product_names)}")


if __name__ == "__main__":
    # 使用说明
    if len(sys.argv) < 2:
        print("使用方法: python cnabs_bonds_test.py <excel文件路径>")
        print("Excel文件要求: 第一个sheet的A列包含所有需要查询的产品全称")
        print("\n示例: python cnabs_bonds_test.py products.xlsx")
        sys.exit(1)
    
    excel_file_path = sys.argv[1]
    
    # 检查文件是否存在
    if not os.path.exists(excel_file_path):
        print(f"错误: 文件不存在 - {excel_file_path}")
        sys.exit(1)
    
    # 读取Excel文件，获取A列的产品名称
    print(f"读取Excel文件: {excel_file_path}")
    df = pd.read_excel(excel_file_path, sheet_name=0, header=None)
    all_product_names = df.iloc[:, 0].dropna().astype(str).tolist()
    print(f"Excel中共有 {len(all_product_names)} 个产品")
    
    # 读取已成功的产品，排除它们
    successful_products = load_successful_products()
    if successful_products:
        print(f"已成功处理 {len(successful_products)} 个产品，将跳过")
    
    list_product_names = [p for p in all_product_names if p not in successful_products]
    print(f"本次需要处理 {len(list_product_names)} 个产品")
    
    if not list_product_names:
        print("所有产品都已处理完成!")
        sys.exit(0)
    
    # 开始处理
    main(list_product_names)
