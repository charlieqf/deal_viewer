"""Operator import for other-market/international PDFs; review uses the existing page/key.

This archives supplied bytes, not a remote URL fetch. It does not mark any
periodic source check successful and does not create an event before review.
"""
import argparse,hashlib,os
from pathlib import Path
from urllib.parse import urlparse
from pilot_runtime import guarded_connection


def main():
    p=argparse.ArgumentParser();p.add_argument('--issuer-id',type=int,required=True)
    p.add_argument('--file',type=Path,required=True);p.add_argument('--source-url',required=True)
    p.add_argument('--title',required=True);p.add_argument('--archive',type=Path,default=Path('/data/dealviewer-issuer-production/archive'))
    a=p.parse_args();url=urlparse(a.source_url)
    if url.scheme!='https' or not url.hostname or url.username or url.password:raise ValueError('invalid_source_url')
    data=a.file.read_bytes()
    if not data.startswith(b'%PDF-'):raise ValueError('not_pdf')
    import pymupdf
    with pymupdf.open(stream=data,filetype='pdf') as pdf:
        if pdf.is_repaired or pdf.is_encrypted or not pdf.page_count:raise ValueError('invalid_pdf')
        pages=pdf.page_count
        for page in pdf:page.get_text()
    digest=hashlib.sha256(data).digest();target=a.archive/(digest.hex()+'.pdf')
    cn=guarded_connection()
    try:
        if not cn.execute('SELECT IssuerId FROM DV.Issuer WHERE IssuerId=?',a.issuer_id).fetchval():raise ValueError('issuer_not_registered')
        if not target.exists():
            temp=target.with_suffix('.manual-part');temp.write_bytes(data);temp.chmod(0o640);temp.replace(target)
            if hasattr(os,'geteuid') and os.geteuid()==0:os.chown(target,1200,1201)
        if hashlib.sha256(target.read_bytes()).digest()!=digest:raise ValueError('archive_hash_mismatch')
        file=cn.execute('SELECT FileId FROM DV.IssuerFile WHERE Sha256=?',digest).fetchval()
        if not file:
            file=cn.execute("INSERT DV.IssuerFile(Sha256,StorageKey,ByteSize,PageCount,ValidationStatus,StorageBackendCode,ArchiveStatus) OUTPUT inserted.FileId VALUES(?,?,?,?,'verified','r760_filesystem','archived')",digest,target.name,len(data),pages).fetchval()
        doc=cn.execute('SELECT DocumentId FROM DV.IssuerDocument WHERE IssuerId=? AND FileId=?',a.issuer_id,file).fetchval()
        if not doc:doc=cn.execute("INSERT DV.IssuerDocument(IssuerId,FileId,Title,DocumentType) OUTPUT inserted.DocumentId VALUES(?,?,?,'risk_manual')",a.issuer_id,file,a.title).fetchval()
        locator=hashlib.sha256(a.source_url.encode()).digest()
        if not cn.execute("SELECT SourceId FROM DV.IssuerDocumentSource WHERE DocumentId=? AND SourceSystem='manual_risk' AND LocatorHash=?",doc,locator).fetchval():
            cn.execute("INSERT DV.IssuerDocumentSource(DocumentId,SourceSystem,SourceLocator,LocatorHash,LocatorNormalizationVersion,OriginalFileName) VALUES(?,'manual_risk',?,?,'1',?)",doc,a.source_url,locator,a.title)
        cn.commit();print('archived_document_id='+str(doc))
    except BaseException:cn.rollback();raise
    finally:cn.close()

if __name__=='__main__':main()
