-- SQL Server 2014. Executed only in a separately named, guarded pilot database.
CREATE SCHEMA DV;
GO
CREATE TABLE DV.Issuer (
 IssuerId bigint IDENTITY PRIMARY KEY,
 LegalName nvarchar(512) NOT NULL, DisplayName nvarchar(512) NOT NULL,
 InstitutionType varchar(32) NOT NULL, UnifiedSocialCreditCode varchar(18) NULL,
 MonitoringStatus varchar(16) NOT NULL DEFAULT 'active',
 CreatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(), RowVersion rowversion,
 CHECK (UnifiedSocialCreditCode IS NULL OR LEN(UnifiedSocialCreditCode)=18)
);
CREATE UNIQUE INDEX UX_Issuer_Code ON DV.Issuer(UnifiedSocialCreditCode) WHERE UnifiedSocialCreditCode IS NOT NULL;
CREATE TABLE DV.IssuerAlias (
 AliasId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL REFERENCES DV.Issuer,
 AliasName nvarchar(512) NOT NULL, NormalizedName nvarchar(256) COLLATE Latin1_General_BIN2 NOT NULL,
 NormalizationVersion varchar(32) NOT NULL, SourceSystem varchar(32) NOT NULL,
 ValidFrom date NULL, ValidTo date NULL, ReviewStatus varchar(16) NOT NULL,
 CHECK (ValidFrom IS NULL OR ValidTo IS NULL OR ValidTo>=ValidFrom),
 UNIQUE (IssuerId,SourceSystem,NormalizationVersion,NormalizedName,ValidFrom,ValidTo)
);
CREATE TABLE DV.IssuerFile (
 FileId bigint IDENTITY PRIMARY KEY, Sha256 binary(32) NOT NULL UNIQUE,
 StorageKey nvarchar(512) NOT NULL, ByteSize bigint NOT NULL CHECK(ByteSize>0),
 PageCount int NOT NULL CHECK(PageCount>0), ValidationStatus varchar(16) NOT NULL,
 ArchivedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(), CHECK(ValidationStatus='verified')
);
CREATE TABLE DV.IssuerDocument (
 DocumentId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL REFERENCES DV.Issuer,
 FileId bigint NOT NULL REFERENCES DV.IssuerFile,
 Title nvarchar(512) NOT NULL, DocumentType varchar(32) NOT NULL,
 MainPeriodStart date NULL, MainPeriodEnd date NULL, PublishedDate date NULL,
 DocumentStatus varchar(16) NOT NULL DEFAULT 'active', RowVersion rowversion,
 UNIQUE(IssuerId,FileId), UNIQUE(IssuerId,DocumentId),
 CHECK(DocumentStatus IN ('active','withdrawn')),
 CHECK(MainPeriodStart IS NULL OR MainPeriodEnd IS NULL OR MainPeriodEnd>=MainPeriodStart)
);
CREATE TABLE DV.IssuerDocumentSource (
 SourceId bigint IDENTITY PRIMARY KEY, DocumentId bigint NOT NULL REFERENCES DV.IssuerDocument,
 SourceSystem varchar(32) NOT NULL, SourceLocator nvarchar(max) NOT NULL,
 LocatorHash binary(32) NOT NULL, LocatorNormalizationVersion varchar(32) NOT NULL,
 OriginalFileName nvarchar(512) NOT NULL, SourcePublisher nvarchar(256) NULL,
 FirstSeenAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 LastSeenAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(), ImportRootTaskId bigint NULL,
 UNIQUE(DocumentId,SourceSystem,LocatorHash)
);
CREATE TABLE DV.IssuerMetricDefinition (
 MetricCode varchar(64) NOT NULL PRIMARY KEY, DisplayName nvarchar(64) NOT NULL,
 ValueKind varchar(16) NOT NULL, PeriodKind varchar(16) NOT NULL,
 UnitCode varchar(16) NOT NULL, DefinitionVersion varchar(16) NOT NULL,
 CHECK(ValueKind IN ('amount','percent')), CHECK(PeriodKind IN ('instant','duration'))
);
CREATE TABLE DV.IssuerProcessingTask (
 TaskId bigint IDENTITY PRIMARY KEY, RootTaskId bigint NULL REFERENCES DV.IssuerProcessingTask(TaskId),
 ParentTaskId bigint NULL REFERENCES DV.IssuerProcessingTask(TaskId),
 IssuerId bigint NULL REFERENCES DV.Issuer, DocumentId bigint NULL REFERENCES DV.IssuerDocument,
 Stage varchar(32) NOT NULL, InputManifestKey nvarchar(512) NOT NULL,
 InputHash binary(32) NOT NULL, IdempotencyKey binary(32) NOT NULL UNIQUE,
 PipelineManifestHash binary(32) NOT NULL, Generation int NOT NULL DEFAULT 0,
 Status varchar(24) NOT NULL, LeaseOwner varchar(64) NULL, LeaseEpoch bigint NOT NULL DEFAULT 0,
 LeaseExpiresAt datetime2(3) NULL, CheckpointKey nvarchar(512) NULL,
 NextAttemptAt datetime2(3) NULL, OutcomeCode varchar(64) NULL,
 CreatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(), FinishedAt datetime2(3) NULL,
 RowVersion rowversion,
 CHECK(Status IN ('queued','running','waiting_quota','needs_review','succeeded','failed','cancelled')),
 CHECK(LeaseEpoch>=0), CHECK(Generation>=0)
);
CREATE INDEX IX_IssuerTask_Queue ON DV.IssuerProcessingTask(Status,NextAttemptAt,TaskId);
CREATE INDEX IX_IssuerTask_Lease ON DV.IssuerProcessingTask(Status,LeaseExpiresAt);
ALTER TABLE DV.IssuerDocumentSource ADD FOREIGN KEY(ImportRootTaskId) REFERENCES DV.IssuerProcessingTask(TaskId);
CREATE TABLE DV.IssuerProcessingAttempt (
 AttemptId bigint IDENTITY PRIMARY KEY, TaskId bigint NOT NULL REFERENCES DV.IssuerProcessingTask,
 AttemptNo int NOT NULL CHECK(AttemptNo>0), LeaseEpoch bigint NOT NULL,
 Engine varchar(64) NOT NULL, Model varchar(64) NULL, AccountScope varchar(64) NULL,
 PromptVersion varchar(64) NULL, SchemaVersion varchar(64) NULL,
 InputHash binary(32) NOT NULL, InputStorageKey nvarchar(512) NOT NULL,
 ResultStorageKey nvarchar(512) NULL, ResultHash binary(32) NULL,
 GatewayRequestId varchar(128) NULL, InputTokens bigint NULL, OutputTokens bigint NULL,
 ReservedTokens bigint NOT NULL DEFAULT 0, UsageStatus varchar(16) NOT NULL DEFAULT 'unknown',
 Status varchar(24) NOT NULL, StartedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 FinishedAt datetime2(3) NULL, ErrorCode varchar(128) NULL,
 UNIQUE(TaskId,AttemptNo), CHECK(InputTokens IS NULL OR InputTokens>=0),
 CHECK(OutputTokens IS NULL OR OutputTokens>=0), CHECK(ReservedTokens>=0)
);
CREATE TABLE DV.IssuerMetricObservation (
 ObservationId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL REFERENCES DV.Issuer,
 MetricCode varchar(64) NOT NULL REFERENCES DV.IssuerMetricDefinition,
 PeriodStart date NOT NULL, PeriodEnd date NOT NULL, PeriodBasis varchar(32) NOT NULL,
 EntityScopeCode varchar(32) NOT NULL, AccountingBasisCode varchar(32) NOT NULL,
 CurrencyCode varchar(16) NOT NULL, MeasurementBasisCode varchar(32) NOT NULL,
 NumericValue decimal(28,6) NOT NULL, UnitCode varchar(16) NOT NULL,
 OriginalLabel nvarchar(512) NOT NULL, OriginalScopeLabel nvarchar(512) NOT NULL,
 NormalizationVersion varchar(32) NOT NULL,
 AttemptId bigint NULL REFERENCES DV.IssuerProcessingAttempt, CandidateOrdinal int NULL,
 Origin varchar(16) NOT NULL, CreatedBy nvarchar(128) NOT NULL,
 CreatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(), CorrectionOfObservationId bigint NULL,
 UNIQUE(IssuerId,ObservationId), UNIQUE(IssuerId,MetricCode,ObservationId),
 FOREIGN KEY(IssuerId,MetricCode,CorrectionOfObservationId) REFERENCES DV.IssuerMetricObservation(IssuerId,MetricCode,ObservationId),
 CHECK(PeriodEnd>=PeriodStart), CHECK(PeriodBasis IN ('instant','year_to_date','single_quarter','unknown')),
 CHECK(PeriodBasis<>'instant' OR PeriodStart=PeriodEnd),
 CHECK((Origin='machine' AND AttemptId IS NOT NULL AND CandidateOrdinal IS NOT NULL) OR (Origin='reviewer' AND AttemptId IS NULL AND CandidateOrdinal IS NULL))
);
CREATE UNIQUE INDEX UX_IssuerObservation_Attempt ON DV.IssuerMetricObservation(AttemptId,CandidateOrdinal) WHERE AttemptId IS NOT NULL;
CREATE INDEX IX_IssuerObservation_Search ON DV.IssuerMetricObservation(IssuerId,PeriodEnd,MetricCode);
CREATE TABLE DV.IssuerObservationEvidence (
 EvidenceId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL,
 ObservationId bigint NOT NULL, DocumentId bigint NOT NULL, PdfPage int NOT NULL CHECK(PdfPage>0),
 PrintedPageLabel nvarchar(64) NULL, RegionJson nvarchar(max) NULL,
 RawText nvarchar(max) NOT NULL, RawValue nvarchar(128) NOT NULL, RawUnit nvarchar(128) NOT NULL,
 ColumnHeader nvarchar(512) NOT NULL, Method varchar(32) NOT NULL,
 FOREIGN KEY(IssuerId,ObservationId) REFERENCES DV.IssuerMetricObservation(IssuerId,ObservationId),
 FOREIGN KEY(IssuerId,DocumentId) REFERENCES DV.IssuerDocument(IssuerId,DocumentId)
);
CREATE INDEX IX_IssuerEvidence_Observation ON DV.IssuerObservationEvidence(ObservationId);
CREATE TABLE DV.IssuerFinancialSeries (
 SeriesId bigint IDENTITY PRIMARY KEY, IssuerId bigint NOT NULL REFERENCES DV.Issuer,
 PeriodStart date NOT NULL, PeriodEnd date NOT NULL, FlowBasis varchar(32) NOT NULL,
 PresentationScopeCode varchar(32) NOT NULL, AccountingBasisCode varchar(32) NOT NULL,
 CurrencyCode varchar(16) NOT NULL, SelectionProfileCode varchar(64) NOT NULL,
 CurrentRecordId bigint NULL, NextVersionNo int NOT NULL DEFAULT 1, RowVersion rowversion,
 UNIQUE(IssuerId,SeriesId),
 UNIQUE(IssuerId,PeriodStart,PeriodEnd,FlowBasis,PresentationScopeCode,AccountingBasisCode,CurrencyCode,SelectionProfileCode),
 CHECK(PeriodEnd>=PeriodStart), CHECK(NextVersionNo>0), CHECK(FlowBasis IN ('year_to_date','single_quarter'))
);
CREATE TABLE DV.IssuerFinancialRecord (
 FinancialRecordId bigint IDENTITY PRIMARY KEY, SeriesId bigint NOT NULL, IssuerId bigint NOT NULL,
 VersionNo int NOT NULL CHECK(VersionNo>0), RecordStatus varchar(16) NOT NULL DEFAULT 'draft',
 SelectionPolicyVersion varchar(64) NOT NULL, SelectionPolicyHash binary(32) NOT NULL,
 ExpectedCurrentRecordId bigint NULL, RestatesRecordId bigint NULL, PrimaryDocumentId bigint NULL,
 PublishedDateSnapshot date NULL, ReviewedBy nvarchar(128) NULL, ReviewedAt datetime2(3) NULL,
 PublishedAt datetime2(3) NULL, UpdatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(), RowVersion rowversion,
 UNIQUE(SeriesId,VersionNo), UNIQUE(SeriesId,FinancialRecordId), UNIQUE(IssuerId,FinancialRecordId),
 FOREIGN KEY(IssuerId,SeriesId) REFERENCES DV.IssuerFinancialSeries(IssuerId,SeriesId),
 FOREIGN KEY(IssuerId,PrimaryDocumentId) REFERENCES DV.IssuerDocument(IssuerId,DocumentId),
 FOREIGN KEY(SeriesId,ExpectedCurrentRecordId) REFERENCES DV.IssuerFinancialRecord(SeriesId,FinancialRecordId),
 FOREIGN KEY(SeriesId,RestatesRecordId) REFERENCES DV.IssuerFinancialRecord(SeriesId,FinancialRecordId),
 CHECK(RecordStatus IN ('draft','published','withdrawn'))
);
ALTER TABLE DV.IssuerFinancialSeries ADD FOREIGN KEY(SeriesId,CurrentRecordId) REFERENCES DV.IssuerFinancialRecord(SeriesId,FinancialRecordId);
CREATE TABLE DV.IssuerFinancialMetric (
 FinancialMetricId bigint IDENTITY PRIMARY KEY, FinancialRecordId bigint NOT NULL, IssuerId bigint NOT NULL,
 MetricCode varchar(64) NOT NULL REFERENCES DV.IssuerMetricDefinition, ObservationId bigint NULL,
 ValueStatus varchar(24) NOT NULL, MissingReason nvarchar(max) NULL,
 ReviewStatus varchar(16) NOT NULL, ReviewNote nvarchar(max) NULL,
 ReviewedBy nvarchar(128) NULL, ReviewedAt datetime2(3) NULL,
 UNIQUE(FinancialRecordId,MetricCode), UNIQUE(IssuerId,FinancialMetricId),
 FOREIGN KEY(IssuerId,FinancialRecordId) REFERENCES DV.IssuerFinancialRecord(IssuerId,FinancialRecordId),
 FOREIGN KEY(IssuerId,MetricCode,ObservationId) REFERENCES DV.IssuerMetricObservation(IssuerId,MetricCode,ObservationId),
 CHECK(ValueStatus IN ('reported','not_disclosed','not_applicable','unresolved','conflict')),
 CHECK((ValueStatus='reported' AND ObservationId IS NOT NULL) OR (ValueStatus<>'reported' AND ObservationId IS NULL)),
 CHECK(ValueStatus NOT IN ('not_disclosed','not_applicable') OR (MissingReason IS NOT NULL AND LEN(MissingReason)>0)),
 CHECK(ReviewStatus IN ('pending','verified','corrected'))
);
CREATE TABLE DV.IssuerMetricDocumentReview (
 FinancialMetricId bigint NOT NULL, IssuerId bigint NOT NULL, DocumentId bigint NOT NULL,
 PageStart int NOT NULL, PageEnd int NOT NULL, CheckStatus varchar(16) NOT NULL,
 SearchOutcome varchar(24) NOT NULL, ReviewSummary nvarchar(max) NOT NULL,
 AttemptId bigint NULL REFERENCES DV.IssuerProcessingAttempt,
 ReviewedBy nvarchar(128) NOT NULL, ReviewedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 PRIMARY KEY(FinancialMetricId,DocumentId,PageStart,PageEnd),
 FOREIGN KEY(IssuerId,FinancialMetricId) REFERENCES DV.IssuerFinancialMetric(IssuerId,FinancialMetricId),
 FOREIGN KEY(IssuerId,DocumentId) REFERENCES DV.IssuerDocument(IssuerId,DocumentId),
 CHECK(PageStart>=1 AND PageEnd>=PageStart), CHECK(CheckStatus IN ('complete','incomplete'))
);
CREATE TABLE DV.IssuerChangeLog (
 ChangeId bigint IDENTITY PRIMARY KEY, EntityType varchar(64) NOT NULL, EntityId bigint NOT NULL,
 Action varchar(32) NOT NULL, BeforeJson nvarchar(max) NULL, AfterJson nvarchar(max) NULL,
 Reason nvarchar(max) NOT NULL, ActorId nvarchar(128) NOT NULL, RequestId varchar(128) NULL,
 RequestHash binary(32) NULL, CreatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE UNIQUE INDEX UX_IssuerChange_Request ON DV.IssuerChangeLog(EntityType,EntityId,Action,RequestId) WHERE RequestId IS NOT NULL;
INSERT DV.IssuerMetricDefinition VALUES
 ('total_assets',N'总资产','amount','instant','yuan','1'),
 ('capital_adequacy_ratio',N'资本充足率','percent','instant','percent','1'),
 ('nonperforming_loan_ratio',N'不良贷款率','percent','instant','percent','1'),
 ('provision_coverage_ratio',N'拨备覆盖率','percent','instant','percent','1'),
 ('operating_revenue',N'营业收入','amount','duration','yuan','1'),
 ('total_profit',N'利润总额','amount','duration','yuan','1');
GO
CREATE VIEW DV.vw_IssuerFinancialPublished AS
SELECT s.IssuerId,s.SeriesId,r.FinancialRecordId,r.VersionNo,s.PeriodStart,s.PeriodEnd,
 s.FlowBasis,s.PresentationScopeCode,s.AccountingBasisCode,s.CurrencyCode,s.SelectionProfileCode,
 MAX(CASE WHEN m.MetricCode='total_assets' THEN o.NumericValue END) TotalAssets,
 MAX(CASE WHEN m.MetricCode='capital_adequacy_ratio' THEN o.NumericValue END) CapitalAdequacyRatio,
 MAX(CASE WHEN m.MetricCode='nonperforming_loan_ratio' THEN o.NumericValue END) NonperformingLoanRatio,
 MAX(CASE WHEN m.MetricCode='provision_coverage_ratio' THEN o.NumericValue END) ProvisionCoverageRatio,
 MAX(CASE WHEN m.MetricCode='operating_revenue' THEN o.NumericValue END) OperatingRevenue,
 MAX(CASE WHEN m.MetricCode='total_profit' THEN o.NumericValue END) TotalProfit
FROM DV.IssuerFinancialSeries s
JOIN DV.IssuerFinancialRecord r ON r.FinancialRecordId=s.CurrentRecordId AND r.RecordStatus='published'
JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId
LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId
GROUP BY s.IssuerId,s.SeriesId,r.FinancialRecordId,r.VersionNo,s.PeriodStart,s.PeriodEnd,
 s.FlowBasis,s.PresentationScopeCode,s.AccountingBasisCode,s.CurrencyCode,s.SelectionProfileCode;
GO
CREATE TRIGGER DV.tr_IssuerObservation_Immutable ON DV.IssuerMetricObservation INSTEAD OF UPDATE,DELETE AS
BEGIN; THROW 51000,'Observation rows are immutable; create a correction.',1; END;
GO
CREATE TRIGGER DV.tr_IssuerEvidence_Immutable ON DV.IssuerObservationEvidence INSTEAD OF UPDATE,DELETE AS
BEGIN; THROW 51000,'Evidence rows are immutable.',1; END;
GO
CREATE TRIGGER DV.tr_IssuerLog_Immutable ON DV.IssuerChangeLog INSTEAD OF UPDATE,DELETE AS
BEGIN; THROW 51000,'Audit rows are append only.',1; END;
GO
CREATE TRIGGER DV.tr_IssuerMetric_Frozen ON DV.IssuerFinancialMetric AFTER INSERT,UPDATE,DELETE AS
BEGIN
 SET NOCOUNT ON;
 IF EXISTS(SELECT 1 FROM DV.IssuerFinancialRecord r WHERE r.RecordStatus<>'draft' AND r.FinancialRecordId IN
   (SELECT FinancialRecordId FROM inserted UNION SELECT FinancialRecordId FROM deleted))
 BEGIN; THROW 51001,'Published or withdrawn record is frozen.',1; END;
 UPDATE r SET UpdatedAt=SYSUTCDATETIME() FROM DV.IssuerFinancialRecord r WHERE r.FinancialRecordId IN
   (SELECT FinancialRecordId FROM inserted UNION SELECT FinancialRecordId FROM deleted);
END;
GO
CREATE TRIGGER DV.tr_IssuerReview_Frozen ON DV.IssuerMetricDocumentReview AFTER INSERT,UPDATE,DELETE AS
BEGIN
 SET NOCOUNT ON;
 IF EXISTS(SELECT 1 FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId
 WHERE r.RecordStatus<>'draft' AND m.FinancialMetricId IN (SELECT FinancialMetricId FROM inserted UNION SELECT FinancialMetricId FROM deleted))
 BEGIN; THROW 51001,'Published or withdrawn review is frozen.',1; END;
 UPDATE r SET UpdatedAt=SYSUTCDATETIME() FROM DV.IssuerFinancialRecord r WHERE r.FinancialRecordId IN
 (SELECT m.FinancialRecordId FROM DV.IssuerFinancialMetric m WHERE m.FinancialMetricId IN
 (SELECT FinancialMetricId FROM inserted UNION SELECT FinancialMetricId FROM deleted));
END;
GO
CREATE TRIGGER DV.tr_IssuerEvidence_Page ON DV.IssuerObservationEvidence AFTER INSERT AS
BEGIN
 IF EXISTS(SELECT 1 FROM inserted e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId
 JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE e.PdfPage>f.PageCount OR d.DocumentStatus<>'active')
 BEGIN; THROW 51002,'Evidence page outside the active source document.',1; END;
 IF EXISTS(SELECT 1 FROM inserted e JOIN DV.IssuerFinancialMetric m ON m.ObservationId=e.ObservationId
 JOIN DV.IssuerFinancialRecord r ON r.FinancialRecordId=m.FinancialRecordId WHERE r.RecordStatus<>'draft')
 BEGIN; THROW 51002,'Cannot append evidence to a published observation.',1; END;
END;
GO
CREATE TRIGGER DV.tr_IssuerReview_Page ON DV.IssuerMetricDocumentReview AFTER INSERT,UPDATE AS
BEGIN
 IF EXISTS(SELECT 1 FROM inserted e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId
 JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE e.PageEnd>f.PageCount OR d.DocumentStatus<>'active')
 BEGIN; THROW 51002,'Review page outside the active source document.',1; END;
END;
GO
CREATE PROCEDURE DV.usp_IssuerPublishFinancialRecord
 @RecordId bigint, @ExpectedRecordVersion binary(8), @ExpectedSeriesVersion binary(8),
 @ExpectedCurrentId bigint=NULL, @Actor nvarchar(128), @RequestId varchar(128), @RequestHash binary(32),
 @Reason nvarchar(max)
AS
BEGIN
 SET NOCOUNT ON; SET XACT_ABORT ON;
 BEGIN TRY
  BEGIN TRANSACTION;
  DECLARE @SeriesId bigint,@CurrentId bigint,@SeriesVersion binary(8),@RecordVersion binary(8),@Status varchar(16);
  SELECT @SeriesId=SeriesId FROM DV.IssuerFinancialRecord WHERE FinancialRecordId=@RecordId;
  IF @SeriesId IS NULL BEGIN; THROW 51003,'Unknown record.',1; END;
  SELECT @CurrentId=CurrentRecordId,@SeriesVersion=RowVersion FROM DV.IssuerFinancialSeries WITH(UPDLOCK,HOLDLOCK) WHERE SeriesId=@SeriesId;
  IF EXISTS(SELECT 1 FROM DV.IssuerChangeLog WHERE EntityType='record' AND EntityId=@RecordId AND Action='publish' AND RequestId=@RequestId)
  BEGIN
   IF NOT EXISTS(SELECT 1 FROM DV.IssuerChangeLog WHERE EntityType='record' AND EntityId=@RecordId AND Action='publish' AND RequestId=@RequestId AND RequestHash=@RequestHash)
   BEGIN; THROW 51004,'Idempotency request body mismatch.',1; END;
   COMMIT; SELECT @RecordId FinancialRecordId,'replayed' Outcome; RETURN;
  END;
  SELECT @RecordVersion=RowVersion,@Status=RecordStatus FROM DV.IssuerFinancialRecord WITH(UPDLOCK,HOLDLOCK) WHERE FinancialRecordId=@RecordId;
  IF @SeriesVersion<>@ExpectedSeriesVersion OR @RecordVersion<>@ExpectedRecordVersion OR ISNULL(@CurrentId,-1)<>ISNULL(@ExpectedCurrentId,-1)
  BEGIN; THROW 51005,'Concurrent modification; refresh and compare.',1; END;
  IF @Status<>'draft' BEGIN; THROW 51006,'Only draft records can be published.',1; END;
  IF (SELECT COUNT(*) FROM DV.IssuerFinancialMetric WHERE FinancialRecordId=@RecordId)<>6
  BEGIN; THROW 51007,'All six required metrics must exist.',1; END;
  IF EXISTS(SELECT 1 FROM DV.IssuerFinancialMetric WHERE FinancialRecordId=@RecordId AND
   (ValueStatus NOT IN ('reported','not_disclosed','not_applicable') OR ReviewStatus NOT IN ('verified','corrected') OR ReviewedBy IS NULL OR ReviewedAt IS NULL))
  BEGIN; THROW 51007,'Unreviewed, unresolved or conflicting metric.',1; END;
  IF NOT EXISTS(SELECT 1 FROM DV.IssuerFinancialRecord r JOIN DV.IssuerDocument d ON d.DocumentId=r.PrimaryDocumentId
    WHERE r.FinancialRecordId=@RecordId AND d.DocumentStatus='active')
  BEGIN; THROW 51007,'An active primary source is required.',1; END;
  IF EXISTS(SELECT 1 FROM DV.IssuerFinancialMetric m WHERE m.FinancialRecordId=@RecordId AND m.ValueStatus='reported'
   AND NOT EXISTS(SELECT 1 FROM DV.IssuerObservationEvidence e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId WHERE e.ObservationId=m.ObservationId AND d.DocumentStatus='active'))
  BEGIN; THROW 51007,'Reported metric requires source evidence.',1; END;
  IF EXISTS(SELECT 1 FROM DV.IssuerFinancialMetric m WHERE m.FinancialRecordId=@RecordId AND m.ValueStatus='not_disclosed'
   AND (NOT EXISTS(SELECT 1 FROM DV.IssuerMetricDocumentReview v WHERE v.FinancialMetricId=m.FinancialMetricId AND v.CheckStatus='complete' AND v.SearchOutcome='not_found')
    OR EXISTS(SELECT 1 FROM DV.IssuerMetricDocumentReview v WHERE v.FinancialMetricId=m.FinancialMetricId AND v.CheckStatus='incomplete')))
  BEGIN; THROW 51007,'Missing metric needs completed document review.',1; END;
  IF EXISTS(SELECT 1 FROM DV.IssuerFinancialMetric m JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId
   JOIN DV.IssuerMetricDefinition d ON d.MetricCode=m.MetricCode JOIN DV.IssuerFinancialSeries s ON s.SeriesId=@SeriesId
   WHERE m.FinancialRecordId=@RecordId AND
   (o.PeriodEnd<>s.PeriodEnd OR o.UnitCode<>d.UnitCode OR o.EntityScopeCode='unknown'
    OR (d.PeriodKind='instant' AND (o.PeriodBasis<>'instant' OR o.PeriodStart<>s.PeriodEnd))
    OR (d.PeriodKind='duration' AND (o.PeriodBasis<>s.FlowBasis OR o.PeriodStart<>s.PeriodStart))
    OR (d.ValueKind='amount' AND (o.CurrencyCode<>s.CurrencyCode OR o.AccountingBasisCode<>s.AccountingBasisCode))
    OR (d.ValueKind='percent' AND o.CurrencyCode<>'not_applicable')
    OR (s.PresentationScopeCode='consolidated' AND o.EntityScopeCode NOT IN ('accounting_consolidated','regulatory_consolidated'))
    OR (s.PresentationScopeCode='standalone' AND o.EntityScopeCode<>'standalone')))
  BEGIN; THROW 51007,'Period, unit, currency or entity scope mismatch.',1; END;
  UPDATE DV.IssuerFinancialRecord SET RecordStatus='published',ReviewedBy=@Actor,ReviewedAt=SYSUTCDATETIME(),PublishedAt=SYSUTCDATETIME() WHERE FinancialRecordId=@RecordId;
  UPDATE DV.IssuerFinancialSeries SET CurrentRecordId=@RecordId WHERE SeriesId=@SeriesId;
  INSERT DV.IssuerChangeLog(EntityType,EntityId,Action,BeforeJson,AfterJson,Reason,ActorId,RequestId,RequestHash)
   VALUES('record',@RecordId,'publish',CONVERT(nvarchar(32),@CurrentId),CONVERT(nvarchar(32),@RecordId),@Reason,@Actor,@RequestId,@RequestHash);
  COMMIT; SELECT @RecordId FinancialRecordId,'published' Outcome;
 END TRY
 BEGIN CATCH
  IF @@TRANCOUNT>0 ROLLBACK;
  THROW;
 END CATCH;
END;
GO
