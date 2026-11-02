"""Apply only the additive rating migration on the guarded production database."""
import hashlib
from pathlib import Path
from pilot_runtime import guarded_connection


def main():
    data=(Path(__file__).resolve().parents[1]/'production_migration_005_rating.sql').read_bytes()
    digest=hashlib.sha256(data).hexdigest();cn=guarded_connection()
    try:
        cn.execute("DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource='issuer-rating-migration',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000; IF @r<0 THROW 51000,'Migration lock unavailable',1;")
        previous=cn.execute("SELECT CONVERT(varchar(64),value) FROM sys.extended_properties WHERE class=0 AND name='IssuerRatingMigration005Hash'").fetchval()
        if previous and previous!=digest:raise RuntimeError('rating_migration_hash_changed')
        if not previous:
            cn.execute(data.decode('utf-8'))
            cn.execute("EXEC sys.sp_addextendedproperty @name=N'IssuerRatingMigration005Hash',@value=?",digest)
        cn.commit()
        print('rating_migration_verified')
    except BaseException:cn.rollback();raise
    finally:cn.close()


if __name__=='__main__':main()
