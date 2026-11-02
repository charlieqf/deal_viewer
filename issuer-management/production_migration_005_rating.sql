-- Additive issuer rating candidates. No automatic business publication.
IF OBJECT_ID('DV.IssuerRatingCandidate','U') IS NULL
BEGIN
 CREATE TABLE DV.IssuerRatingCandidate(
  RatingCandidateId bigint IDENTITY PRIMARY KEY,
  IssuerId bigint NOT NULL REFERENCES DV.Issuer,
  DocumentId bigint NOT NULL,
  AttemptId bigint NOT NULL REFERENCES DV.IssuerProcessingAttempt,
  CandidateOrdinal int NOT NULL,
  Agency nvarchar(256) NOT NULL,
  RatingDate date NOT NULL,
  Grade varchar(16) NULL,
  Outlook nvarchar(64) NULL,
  RatingAction varchar(32) NOT NULL,
  EvidenceJson nvarchar(max) NOT NULL,
  ReviewStatus varchar(16) NOT NULL DEFAULT 'pending',
  CreatedAt datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
  FOREIGN KEY(IssuerId,DocumentId) REFERENCES DV.IssuerDocument(IssuerId,DocumentId),
  UNIQUE(AttemptId,CandidateOrdinal),
  CHECK(ReviewStatus='pending'),
  CHECK(RatingDate>='20240101'),
  CHECK(RatingAction IN ('initial','follow_up','outlook_change','withdrawn')),
  CHECK((RatingAction='withdrawn' AND Grade IS NULL) OR (RatingAction<>'withdrawn' AND Grade IS NOT NULL))
 );
 CREATE INDEX IX_IssuerRatingCandidate_History ON DV.IssuerRatingCandidate(IssuerId,Agency,RatingDate);
END;
IF DATABASE_PRINCIPAL_ID('DealViewerIssuerWebReader') IS NOT NULL
 GRANT SELECT ON OBJECT::DV.IssuerRatingCandidate TO DealViewerIssuerWebReader;
