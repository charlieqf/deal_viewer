"""Read back business and task counts for a bounded acquisition canary."""
import argparse
import json
from pathlib import Path
from pilot_runtime import guarded_connection, atomic_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--issuer-id',type=int,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();cn=guarded_connection()
    try:
        result={}
        for table in ('IssuerDocument','IssuerRatingCandidate','IssuerFinancialRecord','IssuerMetricObservation','IssuerProcessingTask'):
            result[table]=cn.execute('SELECT COUNT(*) FROM DV.'+table+' WHERE IssuerId=?',a.issuer_id).fetchval()
        result['IssuerProcessingAttempt']=cn.execute('SELECT COUNT(*) FROM DV.IssuerProcessingAttempt a JOIN DV.IssuerProcessingTask t ON t.TaskId=a.TaskId WHERE t.IssuerId=?',a.issuer_id).fetchval()
        result['rating_pending']=cn.execute("SELECT COUNT(*) FROM DV.IssuerRatingCandidate WHERE IssuerId=? AND ReviewStatus='pending'",a.issuer_id).fetchval()
        atomic_json(a.out,result);print(json.dumps(result))
    finally:cn.close()


if __name__=='__main__':main()
