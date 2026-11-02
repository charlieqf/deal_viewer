-- V2.3; additive migration, SQL Server 2014. Caller owns guarded transaction.
IF OBJECT_ID('DV.ProductIssuerMapping','U') IS NULL
BEGIN
 CREATE TABLE DV.ProductIssuerMapping(
  ProductId bigint PRIMARY KEY, IssuerId bigint NULL REFERENCES DV.Issuer,
  OriginalProductSubject nvarchar(512) NULL,
  MappingSource varchar(16) NOT NULL CHECK(MappingSource IN ('auto','manual')),
  Basis nvarchar(2000) NOT NULL, UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  RowVersion rowversion
 );
 CREATE INDEX IX_ProductIssuerMapping_Issuer ON DV.ProductIssuerMapping(IssuerId);
END;
IF OBJECT_ID('DV.RiskEvent','U') IS NULL
BEGIN
 CREATE TABLE DV.RiskEvent(
  RiskEventId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL REFERENCES DV.Issuer,
  Category varchar(16) NOT NULL CHECK(Category IN ('penalty','rating','default')),
  EventKey binary(32) NOT NULL UNIQUE,
  EventDate date NULL, PublishedDate date NULL,
  DiscoveredAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  SourceUrl nvarchar(2000) NOT NULL, OriginalHash binary(32) NOT NULL,
  DocumentId bigint NULL, RatingCandidateId bigint NULL REFERENCES DV.IssuerRatingCandidate,
  FieldsJson nvarchar(max) NOT NULL, RawExtractionJson nvarchar(max) NOT NULL,
  EvidenceJson nvarchar(max) NOT NULL,
  Status varchar(24) NOT NULL CHECK(Status IN ('pending','confirmed','excluded','below_threshold','other_debtor')),
  Notes nvarchar(2000) NOT NULL DEFAULT N'', UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  RowVersion rowversion,
  FOREIGN KEY(IssuerId,DocumentId) REFERENCES DV.IssuerDocument(IssuerId,DocumentId),
  CHECK(Status<>'confirmed' OR EventDate IS NOT NULL)
 );
 CREATE INDEX IX_RiskEvent_IssuerDate ON DV.RiskEvent(IssuerId,EventDate,Category,Status);
END;
IF OBJECT_ID('DV.RiskCheckLog','U') IS NULL
BEGIN
 CREATE TABLE DV.RiskCheckLog(
  RiskCheckLogId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL REFERENCES DV.Issuer,
  Source varchar(64) NOT NULL, CheckKey binary(32) NOT NULL UNIQUE,
  CheckedAt datetime2(3) NOT NULL, QueryStart date NULL, QueryEnd date NULL,
  Status varchar(24) NOT NULL CHECK(Status IN ('success','failed','not_connected')),
  FailureReason nvarchar(2000) NULL, ArchiveReference nvarchar(2000) NULL,
  PendingDocuments int NOT NULL DEFAULT 0,
  ImportedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  CHECK(QueryStart IS NULL OR QueryEnd IS NULL OR QueryEnd>=QueryStart)
 );
 CREATE INDEX IX_RiskCheckLog_Source ON DV.RiskCheckLog(IssuerId,Source,CheckedAt);
END;
IF COL_LENGTH('DV.IssuerRatingCandidate','PreviousGrade') IS NULL
 ALTER TABLE DV.IssuerRatingCandidate ADD PreviousGrade nvarchar(64) NULL;
IF COL_LENGTH('DV.IssuerRatingCandidate','ChangeDirection') IS NULL
 ALTER TABLE DV.IssuerRatingCandidate ADD ChangeDirection nvarchar(8) NOT NULL
 CONSTRAINT DF_RatingDirection DEFAULT N'未知'
 CONSTRAINT CK_RatingDirection CHECK(ChangeDirection IN (N'上调',N'下调',N'维持',N'未知'));
DECLARE @drop nvarchar(max)=N'';
SELECT @drop=@drop+N'ALTER TABLE DV.IssuerRatingCandidate DROP CONSTRAINT '+QUOTENAME(name)+N';'
 FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID('DV.IssuerRatingCandidate')
 AND definition LIKE '%RatingDate%' AND definition LIKE '%20240101%';
IF LEN(@drop)>0 EXEC sp_executesql @drop;
GO
IF OBJECT_ID('DV.vw_IssuerRiskSummary','V') IS NULL EXEC(N'CREATE VIEW DV.vw_IssuerRiskSummary AS SELECT 1 Placeholder');
GO
ALTER VIEW DV.vw_IssuerRiskSummary AS
WITH business AS (SELECT CONVERT(date,DATEADD(hour,8,SYSUTCDATETIME())) BusinessDate),
deductions AS (
 SELECT i.IssuerId,b.BusinessDate,
 MAX(CASE WHEN e.Category='penalty' THEN 5 ELSE 0 END) PenaltyDeduction,
 MAX(CASE WHEN e.Category='rating' THEN 10 ELSE 0 END) RatingDeduction,
 MAX(CASE WHEN e.Category='default' THEN 15 ELSE 0 END) DefaultDeduction
 FROM DV.Issuer i CROSS JOIN business b LEFT JOIN DV.RiskEvent e ON e.IssuerId=i.IssuerId
 AND e.Status='confirmed' AND e.EventDate BETWEEN DATEADD(month,-24,b.BusinessDate) AND b.BusinessDate
 GROUP BY i.IssuerId,b.BusinessDate)
SELECT *,DATEADD(month,-24,BusinessDate) WindowStart,
 CASE WHEN PenaltyDeduction+RatingDeduction+DefaultDeduction>=15 THEN 0
 ELSE 15-PenaltyDeduction-RatingDeduction-DefaultDeduction END SuggestedScore FROM deductions;
GO
IF DATABASE_PRINCIPAL_ID('DealViewerIssuerWebReader') IS NOT NULL
BEGIN
 GRANT SELECT ON DV.ProductIssuerMapping TO DealViewerIssuerWebReader;
 GRANT SELECT ON DV.RiskEvent TO DealViewerIssuerWebReader;
 GRANT SELECT ON DV.RiskCheckLog TO DealViewerIssuerWebReader;
 GRANT SELECT ON DV.vw_IssuerRiskSummary TO DealViewerIssuerWebReader;
END;
