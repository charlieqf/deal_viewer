from ftplib import FTP
# FTP2 server details 192.168.1.211
FTP2_HOST = '113.125.202.171'
FTP2_PORT = 21121
FTP2_USER = 'gsuser'
FTP2_PASS = 'Password01'
FTP2_HOME_DIR = '.'

ftp2 = FTP()
ftp2.connect(FTP2_HOST, FTP2_PORT)
ftp2.login(FTP2_USER, FTP2_PASS)


def check_utf8_support(ftp):
    features = ftp.sendcmd('FEAT')
    if 'UTF8' in features:
        print("UTF-8 is supported by the FTP server.")
    else:
        print("UTF-8 is not supported by the FTP server.")

def enable_utf8(ftp):
    response = ftp.sendcmd('OPTS UTF8 ON')
    if '200' in response:
        print("UTF-8 encoding enabled on the FTP server.")
    else:
        print("Failed to enable UTF-8 encoding on the FTP server.")


check_utf8_support(ftp2)
enable_utf8(ftp2)

ftp2.quit()
