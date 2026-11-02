"""Apply immutable incremental migrations only to the guarded production DB."""
import hashlib
import json
import re
from pathlib import Path
from pilot_runtime import guarded_connection
from risk_repository import sync_mappings,rows,log_change
from risk_rules import normalize_name
from acquisition.inventory import export


def main():
    root=Path(__file__).resolve().parents[1];cn=guarded_connection()
    try:
        assert cn.execute('SELECT DB_NAME()').fetchval()=='PortfolioManagement'
        cn.execute("SET XACT_ABORT ON; DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource='issuer-risk-migration',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000; IF @r<0 THROW 51000,'Migration locked',1;")
        for number,name in [('008','production_migration_008_risk.sql'),('009','production_migration_009_product_mapping.sql')]:
            raw=(root/name).read_bytes();digest=hashlib.sha256(raw).hexdigest();marker='IssuerRiskMigration'+number+'Hash'
            prior=cn.execute('SELECT CONVERT(varchar(64),value) FROM sys.extended_properties WHERE class=0 AND name=?',marker).fetchval()
            if prior and prior!=digest:raise RuntimeError('migration_hash_changed')
            if not prior:
                for batch in re.split(r'(?m)^GO\s*$',raw.decode('utf-8')):
                    if batch.strip():cn.execute(batch)
                cn.execute('EXEC sys.sp_addextendedproperty @name=?,@value=?',marker,digest)
        # Known role correction is grounded in the supplied Fengyao original.
        legal='丰田汽车金融（中国）有限公司'
        issuers=rows(cn,'SELECT IssuerId,LegalName FROM DV.Issuer')
        toyota=next((r['IssuerId'] for r in issuers if normalize_name(r['LegalName'])==normalize_name(legal)),None)
        if toyota is None:
            toyota=cn.execute("INSERT DV.Issuer(LegalName,DisplayName,InstitutionType) OUTPUT inserted.IssuerId VALUES(?,N'丰田汽车金融','other')",legal).fetchval()
            log_change(cn,'issuer',toyota,None,{'LegalName':legal},{'reviewer':'V2.3实施','reason':'丰耀2025-1原文确认的发起人；https://www.chinamoney.com.cn/dqs/cm-s-notice-query/fileDownLoad.do?mode=open&contentId=3380026&priority=0','request_id':'risk23-register-toyota','operation':'register'})
        inventory=[dict(id=r[0],name=r[1],confirmed_aliases=[]) for r in cn.execute('SELECT IssuerId,LegalName FROM DV.Issuer')]
        by_id={i['id']:i for i in inventory}
        for r in cn.execute("SELECT IssuerId,AliasName FROM DV.IssuerAlias WHERE ReviewStatus='confirmed' AND (ValidTo IS NULL OR ValidTo>=CONVERT(date,DATEADD(hour,8,SYSUTCDATETIME()))) AND (ValidFrom IS NULL OR ValidFrom<=CONVERT(date,DATEADD(hour,8,SYSUTCDATETIME())))"):
            if r[0] in by_id:by_id[r[0]]['confirmed_aliases'].append(r[1])
        count=sync_mappings(cn,inventory)
        prior=rows(cn,'SELECT * FROM DV.ProductIssuerMapping WHERE ProductId=32641')
        if prior and prior[0]['MappingSource']!='manual':
            basis='丰耀2025-1发行说明书：发起人为丰田汽车金融，受托人为上海国际信托。原件contentId=3380026；角色字段颠倒按原文纠正。'
            cn.execute("UPDATE DV.ProductIssuerMapping SET IssuerId=?,MappingSource='manual',Basis=?,UpdatedAt=SYSUTCDATETIME() WHERE ProductId=32641",toyota,basis)
            log_change(cn,'product_mapping',32641,prior[0],{'IssuerId':toyota,'MappingSource':'manual'},dict(reviewer='V2.3实施',reason=basis,request_id='risk23-fengyao-32641'))
        cn.commit()
        print(json.dumps({'migrated':['008','009'],'products':count,'known_role_corrected':bool(prior)}))
    except BaseException:cn.rollback();raise
    finally:cn.close()

if __name__=='__main__':main()
