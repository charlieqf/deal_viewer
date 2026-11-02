-- Source product view remains unchanged; all consumer joins use persisted IDs.
IF OBJECT_ID('DV.vw_IssuerNplProduct','V') IS NULL EXEC(N'CREATE VIEW DV.vw_IssuerNplProduct AS SELECT 1 Placeholder');
GO
ALTER VIEW DV.vw_IssuerNplProduct AS
SELECT DISTINCT m.IssuerId,p.TrustId,p.TrustCode,p.TrustNameShort,p.TrustName,
p.ProductSubject,p.StatusTrust,p.TrustStatus,p.TrustStartDate
FROM DV.ProductIssuerMapping m JOIN DV.tbl_view_Products p ON p.TrustId=m.ProductId
WHERE m.IssuerId IS NOT NULL AND p.MarketPlaceName=N'银行间债券市场'
AND p.MarketCategoryName=N'信贷资产证券化' AND p.AssetTypeName=N'不良资产重组';
GO
IF DATABASE_PRINCIPAL_ID('DealViewerIssuerWebReader') IS NOT NULL
 GRANT SELECT ON DV.vw_IssuerNplProduct TO DealViewerIssuerWebReader;
IF DATABASE_PRINCIPAL_ID('DealViewerIssuerReviewer') IS NOT NULL
BEGIN
 GRANT SELECT,INSERT,UPDATE ON DV.RiskEvent TO DealViewerIssuerReviewer;
 GRANT SELECT,INSERT,UPDATE ON DV.ProductIssuerMapping TO DealViewerIssuerReviewer;
 GRANT SELECT ON DV.IssuerRatingCandidate TO DealViewerIssuerReviewer;
END;
