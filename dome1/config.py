"""Configuration settings for the ABN document processor."""
import os
from typing import NamedTuple

class FTPConfig(NamedTuple):
    host: str
    port: int
    user: str
    password: str
    home_dir: str

# Load FTP credentials from environment variables or use defaults for development
FTP1_CONFIG = FTPConfig(
    host=os.getenv('FTP1_HOST', '113.125.202.171'),
    port=int(os.getenv('FTP1_PORT', '11421')),
    user=os.getenv('FTP1_USER', 'dv'),
    password=os.getenv('FTP1_PASS', '246qweASD@'),
    home_dir=os.getenv('FTP1_HOME_DIR', '/')
)

FTP2_CONFIG = FTPConfig(
    host=os.getenv('FTP2_HOST', '113.125.202.171'),
    port=int(os.getenv('FTP2_PORT', '21121')),
    user=os.getenv('FTP2_USER', 'gsuser'),
    password=os.getenv('FTP2_PASS', 'Password01'),
    home_dir=os.getenv('FTP2_HOME_DIR', '.')
)

# File paths
PATHS = {
    'UPDATE_LOG': '/Products_log/更新时间TXT记录/ABN更新时间sms.txt',
    'UPDATE_LOG_YYBG': '/Products_log/更新时间TXT记录/ABN更新时间sms_yybg.txt',
    'FTP_FOLDER': '/Products_log/银行间债券市场更新数据',
    'INCREMENT_FOLDER': '/增量文档',
    'DV_FOLDER': '/DealViewer/TrustAssociatedDoc',
    'DV_CREDIT_RATING': 'ProductCreditRatingFiles',
    'DV_RELEASE_INSTRUCTION': 'ProductReleaseInstructions',
    'DV_TRUSTEE_REPORT': 'TrusteeReport'
}

# Cache settings
CACHE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "abn_file_cache")
