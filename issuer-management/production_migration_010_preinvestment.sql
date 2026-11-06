-- Extend the existing ledger without changing the post-investment score (SQL Server 2014).
DECLARE @sql nvarchar(max)=N'';
SELECT @sql=@sql+N'ALTER TABLE DV.RiskEvent DROP CONSTRAINT '+QUOTENAME(name)+N';'
 FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID('DV.RiskEvent') AND definition LIKE '%Category%';
IF LEN(@sql)>0 EXEC sp_executesql @sql;
ALTER TABLE DV.RiskEvent WITH CHECK ADD CONSTRAINT CK_RiskEvent_Category
 CHECK(Category IN ('penalty','rating','default','abs_suspension','servicer_failure','maturity_unpaid'));
GO
IF OBJECT_ID('DV.vw_IssuerPreInvestmentRiskSummary','V') IS NULL
 EXEC(N'CREATE VIEW DV.vw_IssuerPreInvestmentRiskSummary AS SELECT 1 Placeholder');
GO
ALTER VIEW DV.vw_IssuerPreInvestmentRiskSummary AS
WITH business AS(SELECT CONVERT(date,DATEADD(hour,8,SYSUTCDATETIME())) BusinessDate),
items AS(
 SELECT i.IssuerId,b.BusinessDate,
 MAX(CASE WHEN e.Category='abs_suspension' THEN 5 ELSE 0 END) SuspensionDeduction,
 MAX(CASE WHEN e.Category='servicer_failure' THEN 5 ELSE 0 END) ServicerDeduction,
 MAX(CASE WHEN e.Category='maturity_unpaid' THEN 5 ELSE 0 END) MaturityDeduction
 FROM DV.Issuer i CROSS JOIN business b LEFT JOIN DV.RiskEvent e ON e.IssuerId=i.IssuerId
 AND e.Status='confirmed' AND e.EventDate BETWEEN DATEADD(month,-24,b.BusinessDate) AND b.BusinessDate
 GROUP BY i.IssuerId,b.BusinessDate)
SELECT *,DATEADD(month,-24,BusinessDate) WindowStart,
 CASE WHEN SuspensionDeduction+ServicerDeduction+MaturityDeduction>0 THEN 5 ELSE 0 END SuggestedDeduction
 FROM items;
GO
IF DATABASE_PRINCIPAL_ID('DealViewerIssuerWebReader') IS NOT NULL
 GRANT SELECT ON DV.vw_IssuerPreInvestmentRiskSummary TO DealViewerIssuerWebReader;
