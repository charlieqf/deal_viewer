"""Durable acquisition journal, separate from canonical business records."""
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'acquisition.sqlite', timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS runs(
              id INTEGER PRIMARY KEY, started TEXT NOT NULL, finished TEXT,
              status TEXT NOT NULL, summary TEXT);
            CREATE TABLE IF NOT EXISTS searches(
              run_id INTEGER NOT NULL REFERENCES runs(id), issuer_id INTEGER NOT NULL,
              kind TEXT NOT NULL, query TEXT NOT NULL, status TEXT NOT NULL,
              total INTEGER, seen INTEGER NOT NULL DEFAULT 0, error TEXT,
              PRIMARY KEY(run_id, issuer_id, kind, query));
            CREATE TABLE IF NOT EXISTS documents(
              id INTEGER PRIMARY KEY, issuer_id INTEGER NOT NULL, kind TEXT NOT NULL,
              source_id TEXT NOT NULL, url TEXT NOT NULL, title TEXT NOT NULL,
              published TEXT, digest TEXT NOT NULL, bytes INTEGER NOT NULL,
              pages INTEGER NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'downloaded', error TEXT,
              UNIQUE(issuer_id,kind,url,digest));
            CREATE TABLE IF NOT EXISTS events(
              id INTEGER PRIMARY KEY, at TEXT NOT NULL, document_id INTEGER,
              event TEXT NOT NULL, detail TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS download_failures(
              issuer_id INTEGER NOT NULL, kind TEXT NOT NULL, url TEXT NOT NULL,
              error TEXT NOT NULL, last_seen TEXT NOT NULL,
              PRIMARY KEY(issuer_id,kind,url));
            CREATE TABLE IF NOT EXISTS download_failure_details(
              issuer_id INTEGER NOT NULL, kind TEXT NOT NULL, url TEXT NOT NULL,
              source_id TEXT NOT NULL, title TEXT NOT NULL, published TEXT,
              PRIMARY KEY(issuer_id,kind,url));
        ''')

    @contextmanager
    def lock(self):
        # OS releases the lock on process termination; journal is restartable.
        file = (self.root / 'worker.lock').open('a+b')
        try:
            import os
            if os.name == 'nt':
                import msvcrt
                file.seek(0); file.write(b'0'); file.flush(); file.seek(0)
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            file.close()

    def start(self):
        with self.db:
            self.db.execute("UPDATE runs SET status='interrupted',finished=? WHERE status='running'", (now(),))
            return self.db.execute("INSERT INTO runs(started,status) VALUES(?,'running')", (now(),)).lastrowid

    def finish(self, run, status, summary):
        with self.db:
            self.db.execute('UPDATE runs SET finished=?,status=?,summary=? WHERE id=?',
                            (now(), status, json.dumps(summary, ensure_ascii=False), run))

    def search(self, run, issuer, kind, query, status, total=None, seen=0, error=None):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO searches VALUES(?,?,?,?,?,?,?,?)',
                            (run, issuer, kind, query, status, total, seen, error))

    def archive(self, issuer, kind, item, content):
        import pymupdf
        if not content.startswith(b'%PDF-'):
            raise ValueError('response_is_not_pdf')
        with pymupdf.open(stream=content, filetype='pdf') as pdf:
            if pdf.is_encrypted or not pdf.page_count or pdf.is_repaired:
                raise ValueError('encrypted_empty_or_repaired_pdf')
            count = pdf.page_count
            for page in pdf:
                page.get_text()  # Force page tree parsing before marking valid.
        digest = hashlib.sha256(content).hexdigest()
        target = self.root / 'archive' / (digest + '.pdf')
        target.parent.mkdir(exist_ok=True)
        if target.exists():
            if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                raise ValueError('archive_hash_mismatch')
        else:
            temp = target.with_suffix('.part'); temp.write_bytes(content); temp.replace(target)
        stamp = now()
        with self.db:
            old = self.db.execute('SELECT id FROM documents WHERE issuer_id=? AND kind=? AND url=? AND digest=?',
                                  (issuer, kind, item['url'], digest)).fetchone()
            if old:
                self.db.execute('UPDATE documents SET last_seen=? WHERE id=?', (stamp, old['id']))
                return old['id'], False
            cursor = self.db.execute('''INSERT INTO documents
                (issuer_id,kind,source_id,url,title,published,digest,bytes,pages,first_seen,last_seen)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                (issuer, kind, str(item['source_id']), item['url'], item['title'], item.get('published'),
                 digest, len(content), count, stamp, stamp))
            self.db.execute('INSERT INTO events(at,document_id,event,detail) VALUES(?,?,?,?)',
                            (stamp, cursor.lastrowid, 'downloaded', json.dumps({'sha256': digest, 'source': 'chinamoney'})))
            return cursor.lastrowid, True
