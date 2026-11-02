-- Add the two existing profit metrics to the published projection.
-- Run against the guarded production database in a transaction.
-- Amounts retain the stored unit (yuan); scope remains PresentationScopeCode.
ALTER VIEW DV.vw_IssuerFinancialPublished AS
SELECT s.IssuerId,s.SeriesId,r.FinancialRecordId,r.VersionNo,s.PeriodStart,s.PeriodEnd,
 s.FlowBasis,s.PresentationScopeCode,s.AccountingBasisCode,s.CurrencyCode,s.SelectionProfileCode,
 MAX(CASE WHEN m.MetricCode='total_assets' THEN o.NumericValue END) TotalAssets,
 MAX(CASE WHEN m.MetricCode='capital_adequacy_ratio' THEN o.NumericValue END) CapitalAdequacyRatio,
 MAX(CASE WHEN m.MetricCode='nonperforming_loan_ratio' THEN o.NumericValue END) NonperformingLoanRatio,
 MAX(CASE WHEN m.MetricCode='provision_coverage_ratio' THEN o.NumericValue END) ProvisionCoverageRatio,
 MAX(CASE WHEN m.MetricCode='operating_revenue' THEN o.NumericValue END) OperatingRevenue,
 MAX(CASE WHEN m.MetricCode='total_profit' THEN o.NumericValue END) TotalProfit,
 MAX(CASE WHEN m.MetricCode='net_profit' THEN o.NumericValue END) NetProfit,
 MAX(CASE WHEN m.MetricCode='net_profit_attributable_to_parent' THEN o.NumericValue END) NetProfitAttributableToParent
FROM DV.IssuerFinancialSeries s
JOIN DV.IssuerFinancialRecord r ON r.FinancialRecordId=s.CurrentRecordId AND r.RecordStatus='published'
JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId
LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId
GROUP BY s.IssuerId,s.SeriesId,r.FinancialRecordId,r.VersionNo,s.PeriodStart,s.PeriodEnd,
 s.FlowBasis,s.PresentationScopeCode,s.AccountingBasisCode,s.CurrencyCode,s.SelectionProfileCode;
