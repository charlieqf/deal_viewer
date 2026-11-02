"""Refresh the monitored issuer inventory from the canonical database before each run."""
import argparse
from pathlib import Path
from pilot_runtime import guarded_connection, atomic_json


def export(path):
    cn = guarded_connection()
    try:
        issuers = [dict(id=int(r[0]),name=r[1],display_name=r[2],institution_type=r[3],
                        monitoring_status=r[4],unified_social_credit_code=r[5],confirmed_aliases=[]) for r in cn.execute(
            'SELECT IssuerId,LegalName,DisplayName,InstitutionType,MonitoringStatus,UnifiedSocialCreditCode FROM DV.Issuer ORDER BY IssuerId')]
        by_id={r['id']:r for r in issuers}
        for row in cn.execute("SELECT IssuerId,AliasName FROM DV.IssuerAlias WHERE ReviewStatus='confirmed' AND (ValidTo IS NULL OR ValidTo>=CONVERT(date,SYSUTCDATETIME())) AND (ValidFrom IS NULL OR ValidFrom<=CONVERT(date,SYSUTCDATETIME()))"):
            if row[0] in by_id: by_id[row[0]]['confirmed_aliases'].append(row[1])
        atomic_json(path,issuers)
    finally: cn.close()


if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);export(p.parse_args().out)
