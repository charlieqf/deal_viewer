IF COL_LENGTH('DV.IssuerMetricObservation','MeasurementBasisLabel') IS NULL
 ALTER TABLE DV.IssuerMetricObservation ADD MeasurementBasisLabel nvarchar(256) NOT NULL CONSTRAINT DF_IssuerObservation_MethodLabel DEFAULT N'';
IF COL_LENGTH('DV.IssuerObservationEvidence','EvidenceRole') IS NULL
 ALTER TABLE DV.IssuerObservationEvidence ADD EvidenceRole varchar(24) NOT NULL CONSTRAINT DF_IssuerEvidence_Role DEFAULT 'value';
GO
IF NOT EXISTS(SELECT 1 FROM sys.check_constraints WHERE name='CK_IssuerEvidence_Role' AND parent_object_id=OBJECT_ID('DV.IssuerObservationEvidence'))
 ALTER TABLE DV.IssuerObservationEvidence ADD CONSTRAINT CK_IssuerEvidence_Role CHECK(EvidenceRole IN ('value','context'));
GO
