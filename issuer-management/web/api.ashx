<%@ WebHandler Language="C#" Class="IssuerApi" %>
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.Script.Serialization;

public class IssuerApi : IHttpHandler {
 public bool IsReusable { get { return false; } }
 const string Selected = @"WITH ranked AS (
 SELECT r.FinancialRecordId,r.IssuerId,r.RecordStatus,s.PeriodStart,s.PeriodEnd,s.AccountingBasisCode,
 ROW_NUMBER() OVER(PARTITION BY r.IssuerId,s.PeriodEnd ORDER BY r.FinancialRecordId DESC) n
 FROM DV.IssuerFinancialRecord r JOIN DV.IssuerFinancialSeries s ON s.SeriesId=r.SeriesId
 WHERE r.RecordStatus<>'withdrawn') ";
 static List<Dictionary<string,object>> Read(SqlConnection cn,string sql,long id) {
  using(var cmd=new SqlCommand(sql,cn)) {
   cmd.CommandTimeout=30;cmd.Parameters.AddWithValue("@id",id);
   var rows=new List<Dictionary<string,object>>();
   using(var r=cmd.ExecuteReader()) while(r.Read()) {
    var row=new Dictionary<string,object>();
    for(int i=0;i<r.FieldCount;i++) {
     object v=r.IsDBNull(i)?null:r.GetValue(i);
     if(v is decimal || v is long) v=Convert.ToString(v,CultureInfo.InvariantCulture);
     if(v is DateTime) v=((DateTime)v).ToString("yyyy-MM-ddTHH:mm:ss",CultureInfo.InvariantCulture);
     row[r.GetName(i)]=v;
    } rows.Add(row);
   }return rows;
  }
 }
 public void ProcessRequest(HttpContext c) {
  c.Response.ContentType="application/json; charset=utf-8";
  c.Response.Headers["X-Content-Type-Options"]="nosniff";
  c.Response.Cache.SetCacheability(HttpCacheability.NoCache);
  if(c.Request.HttpMethod!="GET" && c.Request.HttpMethod!="HEAD") { c.Response.StatusCode=405;return; }
  string action=c.Request.QueryString["action"]??"issuers";long id;
  if(!long.TryParse(c.Request.QueryString["id"],out id))id=0;
  try {
   var setting=ConfigurationManager.ConnectionStrings["IssuerReadOnly"];
   if(setting==null)throw new InvalidOperationException("missing_config");
   using(var cn=new SqlConnection(setting.ConnectionString)) {
    cn.Open();object data;
    if(action=="issuers") data=Read(cn,@"SELECT i.IssuerId,i.LegalName,i.DisplayName,i.InstitutionType,
      (SELECT COUNT(*) FROM DV.IssuerDocument d WHERE d.IssuerId=i.IssuerId AND d.DocumentStatus='active') DocumentCount,
      (SELECT MAX(d.MainPeriodEnd) FROM DV.IssuerDocument d WHERE d.IssuerId=i.IssuerId) LatestPeriod,
      (SELECT COUNT(DISTINCT s.PeriodEnd) FROM DV.IssuerFinancialSeries s WHERE s.IssuerId=i.IssuerId) PeriodCount,
      (SELECT TOP 1 t.Status FROM DV.IssuerProcessingTask t WHERE t.IssuerId=i.IssuerId AND t.RootTaskId IS NULL ORDER BY t.TaskId DESC) ProcessingStatus
      FROM DV.Issuer i ORDER BY i.IssuerId",0);
    else if(action=="issuer" && id>0) {
     var info=Read(cn,"SELECT IssuerId,LegalName,DisplayName,InstitutionType FROM DV.Issuer WHERE IssuerId=@id",id);
     if(info.Count==0){c.Response.StatusCode=404;c.Response.Write("{\"error\":\"not_found\"}");return;}
     data=new Dictionary<string,object>{
      {"issuer",info[0]},
      {"records",Read(cn,Selected+"SELECT FinancialRecordId,RecordStatus,PeriodStart,PeriodEnd,AccountingBasisCode FROM ranked WHERE n=1 AND IssuerId=@id ORDER BY PeriodEnd DESC",id)},
      {"metrics",Read(cn,Selected+@"SELECT m.FinancialRecordId,m.MetricCode,m.ValueStatus,m.ReviewStatus,m.ReviewNote,o.ObservationId,o.NumericValue,o.UnitCode,o.EntityScopeCode,o.AccountingBasisCode,o.MeasurementBasisCode,o.PeriodStart,o.PeriodEnd FROM ranked r JOIN DV.IssuerFinancialMetric m ON m.FinancialRecordId=r.FinancialRecordId LEFT JOIN DV.IssuerMetricObservation o ON o.ObservationId=m.ObservationId WHERE r.n=1 AND r.IssuerId=@id",id)},
      {"documents",Read(cn,@"SELECT d.DocumentId,d.Title,d.MainPeriodEnd,f.PageCount,f.ByteSize,f.ArchiveStatus,CONVERT(varchar(64),f.Sha256,2) Sha256 FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.IssuerId=@id AND d.DocumentStatus='active' ORDER BY d.MainPeriodEnd DESC,d.DocumentId",id)},
      {"tasks",Read(cn,@"SELECT t.TaskId,t.Stage,t.Status,t.OutcomeCode,t.CreatedAt,t.FinishedAt FROM DV.IssuerProcessingTask t WHERE t.IssuerId=@id AND t.RootTaskId IS NULL ORDER BY t.TaskId DESC",id)}
     };
    } else if(action=="evidence" && id>0) data=Read(cn,@"SELECT e.DocumentId,e.PdfPage,e.EvidenceRole,e.RawValue,e.RawUnit,e.ColumnHeader,e.Method,e.RawText,d.Title FROM DV.IssuerObservationEvidence e JOIN DV.IssuerDocument d ON d.DocumentId=e.DocumentId WHERE e.ObservationId=@id AND d.DocumentStatus='active' ORDER BY e.EvidenceId",id);
    else if(action=="document" && id>0) {
     var docs=Read(cn,@"SELECT d.Title,f.StorageKey,f.ArchiveStatus,f.StorageBackendCode FROM DV.IssuerDocument d JOIN DV.IssuerFile f ON f.FileId=d.FileId WHERE d.DocumentId=@id AND d.DocumentStatus='active'",id);
     if(docs.Count!=1 || (string)docs[0]["ArchiveStatus"]!="archived" || (string)docs[0]["StorageBackendCode"]!="dealviewer_filesystem") {c.Response.StatusCode=404;return;}
     string key=(string)docs[0]["StorageKey"];
     if(!Regex.IsMatch(key,@"\A[a-f0-9]{64}\.pdf\z"))throw new InvalidOperationException("invalid_storage_key");
     string path=Path.Combine(ConfigurationManager.AppSettings["IssuerArchiveRoot"],key);
     if(!File.Exists(path)){c.Response.StatusCode=404;return;}
     long length=new FileInfo(path).Length,start=0,end=length-1;
     c.Response.ContentType="application/pdf";c.Response.Headers["Accept-Ranges"]="bytes";
     c.Response.Headers["Content-Disposition"]="inline; filename=report.pdf; filename*=UTF-8''"+Uri.EscapeDataString((string)docs[0]["Title"]);
     string range=c.Request.Headers["Range"];
     if(!String.IsNullOrEmpty(range)) {
      Match m=Regex.Match(range,@"\Abytes=(\d*)-(\d*)\z");long v;
      if(!m.Success || (m.Groups[1].Value=="" && m.Groups[2].Value=="")){c.Response.StatusCode=416;return;}
      if(m.Groups[1].Value=="") {if(!long.TryParse(m.Groups[2].Value,out v)||v<=0){c.Response.StatusCode=416;return;}start=Math.Max(0,length-v);}
      else {if(!long.TryParse(m.Groups[1].Value,out start)){c.Response.StatusCode=416;return;}if(m.Groups[2].Value!=""){if(!long.TryParse(m.Groups[2].Value,out end)){c.Response.StatusCode=416;return;}end=Math.Min(end,length-1);}}
      if(start<0 || start>=length || end<start){c.Response.StatusCode=416;c.Response.Headers["Content-Range"]="bytes */"+length;return;}
      c.Response.StatusCode=206;c.Response.Headers["Content-Range"]="bytes "+start+"-"+end+"/"+length;
     }
     c.Response.Headers["Content-Length"]=(end-start+1).ToString();
     if(c.Request.HttpMethod!="HEAD")c.Response.TransmitFile(path,start,end-start+1);
     return;
    } else {c.Response.StatusCode=400;data=new {error="invalid_request"};}
    var json=new JavaScriptSerializer();json.MaxJsonLength=16*1024*1024;c.Response.Write(json.Serialize(data));
   }
  } catch(Exception ex) {
   System.Diagnostics.Trace.TraceError("Issuer read API failed: "+ex.GetType().Name);
   c.Response.StatusCode=503;c.Response.Write("{\"error\":\"service_unavailable\"}");
  }
 }
}
