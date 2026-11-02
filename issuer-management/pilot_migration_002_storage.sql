IF COL_LENGTH('DV.IssuerFile','StorageBackendCode') IS NULL
BEGIN
 ALTER TABLE DV.IssuerFile ADD StorageBackendCode varchar(32) NOT NULL CONSTRAINT DF_IssuerFile_Backend DEFAULT 'r760_filesystem';
 ALTER TABLE DV.IssuerFile ADD ArchiveStatus varchar(24) NOT NULL CONSTRAINT DF_IssuerFile_Archive DEFAULT 'archived';
 ALTER TABLE DV.IssuerFile ALTER COLUMN ArchivedAt datetime2(3) NULL;
END;
GO
IF NOT EXISTS(SELECT 1 FROM sys.check_constraints WHERE name='CK_IssuerFile_Archive' AND parent_object_id=OBJECT_ID('DV.IssuerFile'))
BEGIN
 ALTER TABLE DV.IssuerFile ADD CONSTRAINT CK_IssuerFile_Archive CHECK(
  (ArchiveStatus='registered_local' AND StorageBackendCode='operator_local' AND ArchivedAt IS NULL) OR
  (ArchiveStatus='archived' AND StorageBackendCode='r760_filesystem' AND ArchivedAt IS NOT NULL));
END;
GO
