-- Production additions. No changes to Trust or existing crawler tables.
ALTER TABLE DV.IssuerFile DROP CONSTRAINT CK_IssuerFile_Archive;
ALTER TABLE DV.IssuerFile ADD CONSTRAINT CK_IssuerFile_Archive CHECK (
 (ArchiveStatus='registered_local' AND StorageBackendCode='operator_local' AND ArchivedAt IS NULL) OR
 (ArchiveStatus='archived' AND StorageBackendCode IN ('r760_filesystem','dealviewer_filesystem') AND ArchivedAt IS NOT NULL));
CREATE INDEX IX_IssuerDocument_Period ON DV.IssuerDocument(IssuerId,MainPeriodEnd);
CREATE INDEX IX_IssuerObservation_Period ON DV.IssuerMetricObservation(IssuerId,PeriodEnd,MetricCode);
CREATE INDEX IX_IssuerTask_Root ON DV.IssuerProcessingTask(RootTaskId,TaskId);
GO
