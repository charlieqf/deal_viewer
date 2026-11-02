"""Public SFTP acceptance; credentials read privately, never emitted."""
import base64
import hashlib
import io
import json
from pathlib import Path
import socket
import stat
import time
import paramiko

ROOT=Path(__file__).resolve().parents[2]
OUT=ROOT/'tmp/issuer-ratings/document-sftp'

def main():
    config=json.loads((Path.home()/'.codex/private/dealviewer-document-sftp.json').read_text(encoding='utf-8'))
    expected=json.loads((OUT/'manifest.json').read_text(encoding='utf-8'))['files']
    transport=paramiko.Transport((config['host'],config['port']))
    transport.get_security_options().key_types=('ssh-ed25519',)
    transport.start_client(timeout=30)
    actual=base64.b64encode(hashlib.sha256(transport.get_remote_server_key().asbytes()).digest()).decode().rstrip('=')
    assert actual==config['host_key_ed25519_sha256'],'Unexpected server host key'
    transport.auth_password(config['username'],config['password'])
    sftp=paramiko.SFTPClient.from_transport(transport)
    assert sftp.normalize('.')=='/documents'
    assert sftp.listdir('/')==['documents']
    checks=['password_login','pinned_host_key','chroot_home']
    def denied(name,action):
        try:action()
        except (OSError,paramiko.SSHException):checks.append(name);return
        raise AssertionError('Forbidden operation unexpectedly allowed: '+name)
    for name in ['/etc/passwd','/../../etc/passwd','/data/dealviewer-issuer-production/private/web.json','/proc/self/environ']:
        denied('outside_path_denied:'+name,lambda name=name:sftp.stat(name))
    denied('upload_denied',lambda:sftp.putfo(io.BytesIO(b'probe'),'/documents/.write-test'))
    denied('mkdir_denied',lambda:sftp.mkdir('/documents/.mkdir-test'))
    denied('delete_denied',lambda:sftp.remove('/documents/.permission-check.txt'))
    denied('rename_denied',lambda:sftp.rename('/documents/.permission-check.txt','/documents/.renamed-test'))
    denied('chmod_denied',lambda:sftp.chmod('/documents/.permission-check.txt',0o666))
    denied('symlink_denied',lambda:sftp.symlink('/etc/passwd','/documents/.symlink-test'))
    # Server-side forwarding policy is exercised over this real authenticated connection.
    denied('local_forward_denied',lambda:transport.open_channel('direct-tcpip',('127.0.0.1',22),('127.0.0.1',12345),timeout=10))
    denied('remote_forward_denied',lambda:transport.request_port_forward('127.0.0.1',0))
    sftp.close()
    time.sleep(.3)
    tty=transport.open_session(timeout=10)
    denied('pty_denied',lambda:tty.get_pty());tty.close()
    command=transport.open_session(timeout=10);command.settimeout(5);command.exec_command('id')
    output=b'';started=time.monotonic()
    while time.monotonic()-started<8:
        if command.recv_ready():output+=command.recv(4096)
        if command.recv_stderr_ready():output+=command.recv_stderr(4096)
        if command.exit_status_ready():break
        time.sleep(.1)
    assert b'uid=' not in output and b'gid=' not in output
    assert b'sftp connections only' in output.lower(),repr(output)
    command.close();checks.append('shell_command_denied')
    time.sleep(.3)
    sftp=paramiko.SFTPClient.from_transport(transport)
    folders={e.filename for e in sftp.listdir_attr('/documents') if stat.S_ISDIR(e.st_mode)}
    assert folders=={p.split('/')[0] for p in expected}
    first=next(iter(expected));directory='/documents/'+first.rsplit('/',1)[0]
    samples=sorted([(directory+'/'+e.filename,e.st_size) for e in sftp.listdir_attr(directory) if e.filename.endswith('.pdf')],key=lambda x:x[1])[:3]
    assert len(samples)==3
    for path,size in samples:
        with sftp.open(path,'rb') as stream:
            stream.prefetch(file_size=size,max_concurrent_requests=8)
            data=stream.read()
        assert data.startswith(b'%PDF-') and len(data)==size
        assert hashlib.sha256(data).hexdigest()+'.pdf'==expected[path[len('/documents/'):]]
    checks+=['all_issuer_folders_match_catalog','three_downloads_sha256_match']
    sftp.close();transport.close()
    result={'username':config['username'],'catalog_documents':len(expected),'issuer_folders':len(folders),
            'download_samples':len(samples),'checks':checks,'password_in_output':False}
    (OUT/'acceptance.json').write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result))

if __name__=='__main__':main()
