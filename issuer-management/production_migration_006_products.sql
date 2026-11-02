-- Additive, read-only product association using the confirmed business scope.
SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF DB_NAME()<>'PortfolioManagement' THROW 51000,'Wrong database',1;
IF OBJECT_ID('DV.vw_IssuerNplProduct') IS NULL
 EXEC(N'CREATE VIEW DV.vw_IssuerNplProduct AS
 SELECT DISTINCT i.IssuerId,p.TrustId,p.TrustCode,p.TrustNameShort,p.TrustName,
 p.ProductSubject,p.StatusTrust,p.TrustStatus,p.TrustStartDate
 FROM DV.Issuer i JOIN DV.tbl_view_Products p ON p.ProductSubject=i.LegalName
 WHERE p.MarketPlaceName=N''银行间债券市场''
 AND p.MarketCategoryName=N''信贷资产证券化'' AND p.AssetTypeName=N''不良资产重组''');
GRANT SELECT ON OBJECT::DV.vw_IssuerNplProduct TO DealViewerIssuerWebReader;
COMMIT;
