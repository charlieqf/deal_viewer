import requests

# Smartproxy credentials and proxy URL
username = 'splci64blr'
password = '6j0z1hrwFM4LbdheZ_'
proxy_address = 'gate.smartproxy.com:10001'

# Set up the proxy URL
proxy = f"http://{username}:{password}@{proxy_address}"

# Test URL to check proxy configuration
test_url = 'https://www.chinabond.com.cn/xxpl/ywzc_fxyfxdh/fxyfxdh_zqzl/zqzl_zjzzczj/'

proxies = {
    'http': proxy,
    'https': proxy,
}

# Send a GET request using the proxy
response = requests.get(test_url, proxies=proxies)

# Print the response
print(response.status_code)
print(response.text)
