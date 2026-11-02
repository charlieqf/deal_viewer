"""Explicit private proxy configuration, shared by HTTP and browser transports."""
import json
import os
from pathlib import Path
from urllib.parse import urlsplit, quote


def proxy_config():
    path=os.environ.get('ISSUER_ACQUISITION_PROXY_FILE')
    if not path:return None
    config=json.loads(Path(path).read_text(encoding='utf-8'))
    if not config.get('enabled',False):return None
    server=config.get('server','');url=urlsplit(server)
    if url.scheme not in ('http','https') or not url.hostname or not url.port or url.username or url.password or url.path not in ('','/') or url.query or url.fragment:
        raise ValueError('invalid_proxy_server')
    username=config.get('username','');password=config.get('password','')
    if not isinstance(username,str) or not isinstance(password,str) or bool(username)!=bool(password):
        raise ValueError('invalid_proxy_auth')
    return {'server':server.rstrip('/'),'username':username,'password':password}


def requests_proxies(config):
    if not config:return {}
    url=urlsplit(config['server'])
    auth=quote(config['username'],safe='')+':'+quote(config['password'],safe='')+'@' if config['username'] else ''
    value=url.scheme+'://'+auth+url.netloc
    return {'http':value,'https':value}
