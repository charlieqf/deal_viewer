'use strict';
const $=id=>document.getElementById(id);
const codes=['total_assets','capital_adequacy_ratio','nonperforming_loan_ratio','provision_coverage_ratio','operating_revenue','total_profit','net_profit','net_profit_attributable_to_parent'];
const names=['总资产','资本充足率','不良贷款率','拨备覆盖率','营业收入','利润总额','合并净利润','归母净利润'];
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const day=s=>(s||'').slice(0,10);
const typeName=s=>s==='amc'?'资产管理公司':s==='bank'?'银行':'其他机构';
const statusName=s=>({needs_review:'待复核',succeeded:'处理完成',running:'处理中',queued:'排队中',waiting_quota:'等待额度',failed:'处理异常',cancelled:'已取消'}[s]||'材料已登记');
let overview=null,detail=null,requestGeneration=0,evidenceGeneration=0,documentFilter='all';
let productGeneration=0,productData=null;
function renderProducts(){
  if(!productData)return;
  const status=$('productStatus').value;
  const rows=productData.products.filter(p=>!status||p.StatusTrust===status);
  const active=productData.products.filter(p=>p.StatusTrust==='Duration').length;
  const closed=productData.products.filter(p=>p.StatusTrust==='Liquidated').length;
  $('productSummary').textContent=`累计 ${productData.total} 个项目 · 存续 ${active} 个 · 已清算 ${closed} 个 · 当前显示 ${rows.length} 个`;
  $('productRows').innerHTML=rows.map(p=>`<tr data-trust-id="${esc(p.TrustId)}"><td><strong>${esc(p.TrustNameShort||p.TrustName)}</strong><small>${esc(p.TrustName)}</small></td><td>${esc(p.TrustId)}</td><td>${esc(p.ProductSubject)}</td><td>${esc(day(p.TrustStartDate)||'待补充')}</td><td>${esc(p.TrustStatus||'待确认')}</td></tr>`).join('')||'<tr><td colspan="5" class="empty">当前条件下暂无关联产品</td></tr>';
}
async function openProducts(id){
  const generation=++productGeneration;productData=null;
  $('productsTitle').textContent=(overview?.issuers.find(i=>i.IssuerId===id)?.LegalName||'发行主体')+' · 不良 ABS';
  $('productStatus').value='';$('productStatus').disabled=true;
  $('productSummary').textContent='正在读取关联产品…';$('productRows').innerHTML='';
  $('productError').hidden=true;$('products').showModal();
  try{
    const data=await api('products',id);if(generation!==productGeneration)return;
    productData=data;$('productStatus').disabled=false;renderProducts();
  }catch(e){if(generation===productGeneration){$('productSummary').textContent='产品读取失败';$('productError').textContent=e.message;$('productError').hidden=false;}}
}
function period(s){s=day(s);return s.slice(0,4)+' 年'+({'03-31':'一季度','06-30':'半年度','09-30':'三季度','12-31':'度'}[s.slice(5)]||s.slice(5));}
function decimalAmount(v){
  let s=String(v),neg=s.startsWith('-');if(neg)s=s.slice(1);
  let [whole,frac='']=s.split('.'),micro=BigInt(whole)*1000000n+BigInt(frac.padEnd(6,'0').slice(0,6));
  let cents=(micro+500000000000n)/1000000000000n;
  return (neg&&cents?'-':'')+String(cents/100n).replace(/\B(?=(\d{3})+(?!\d))/g,',')+'.'+String(cents%100n).padStart(2,'0');
}
function missingLabel(m){
  if(!m)return '尚未采集';let n={};try{n=JSON.parse(m.ReviewNote||'{}');}catch{}
  return ({material_label_not_found:'待补充/核查材料',scope_pending:'口径待核对',numeric_evidence_pending:'数字待核对',basis_pending:'准则待核对',evidence_pending:'证据待核对'})[n.missing_evidence?.code]||'待识别';
}
function hasValue(m){return !!m?.ObservationId&&m.ValueStatus==='reported'&&m.NumericValue!=null;}
function display(m){
  if(!hasValue(m))return missingLabel(m);
  return m.UnitCode==='yuan'?decimalAmount(m.NumericValue):String(m.NumericValue).replace(/(\.\d*?)0+$/,'$1').replace(/\.$/,'')+'%';
}
function scopeLabel(m){
  let note={};try{note=JSON.parse(m?.ReviewNote||'{}');}catch{}
  return m?.EntityScopeCode==='unknown'?'口径待确认':m?.EntityScopeCode==='standalone'?'本行 / 母公司口径'+(note.scope_defaulted?'（按规则）':''):'';
}
function missingMarkup(m,issuerId,allowReview=false){
  let note={};try{note=JSON.parse(m?.ReviewNote||'{}');}catch{}
  const code=note.missing_evidence?.code;
  const next=({
    evidence_pending:'需要核对原文的数值、报告期和集团／本行口径，并绑定证据后补录。',
    scope_pending:'未明确集团或合并口径时按本行处理；仍需核对数值、报告期及原文证据后入库。',
    numeric_evidence_pending:'需要对照原图检查小数点、单位和当期列，确认数值后补录。',
    basis_pending:'需要确认这张报表采用的会计准则，再绑定依据。',
    material_label_not_found:'需要检查现有原文是否披露；确实缺少材料时，再补充正式原件。'
  })[code]||'需要核查原始材料，确认数值、报告期及口径。';
  const links=(note.missing_evidence?.searched_documents||[]).slice(0,2).flatMap(d=>{
    if(!/^\d+$/.test(String(d.document_id)))return [];
    return (d.hit_pages||[]).filter(p=>Number.isInteger(p)&&p>0).slice(0,3).map(p=>`<a target="_blank" rel="noopener" href="api.ashx?action=document&amp;id=${encodeURIComponent(d.document_id)}#page=${p}">原文第 ${p} 页 ↗</a>`);
  });
  const issuer=reviewIssuer(issuerId),blocked=reviewRestriction(m,issuer);
  const bankMetric=issuer?.InstitutionType==='amc'&&codes.slice(1,4).includes(m?.MetricCode);
  const shortReason=issuer?.MonitoringStatus!=='active'?'主体暂停采用':bankMetric?'不采集银行监管指标':'暂不开放录入';
  return `<div class="missing-cell"><details class="missing"><summary>${esc(bankMetric?'此处不采集':missingLabel(m))}</summary><small>${esc(blocked||m?.MissingReason||'尚无通过校验的候选。')}</small>${!blocked?`<small class="missing-next">${issuer?.InstitutionType==='amc'?'请核对原文数值、报告期、指标定义及集团／母公司口径；定义不同不录入。':next}</small>`:''}${links.length?`<div class="missing-links" aria-label="待核对的原文线索">${links.join(' ')}</div>`:''}</details>${blocked?`<small class="review-unavailable">${esc(shortReason)}</small>`:allowReview?reviewButton(m,issuerId):''}</div>`;
}
function metricMarkup(m,issuerId,allowReview=false){
  if(!hasValue(m))return missingMarkup(m,issuerId,allowReview);
  return `<button class="metric" data-observation="${esc(m.ObservationId)}"${issuerId?` data-issuer="${esc(issuerId)}"`:''} aria-label="${esc(names[codes.indexOf(m.MetricCode)])} ${esc(display(m))}，查看证据">${display(m)}</button>${scopeLabel(m)?`<small class="scope-pending">${scopeLabel(m)}</small>`:''}${m.ReviewStatus==='verified'?'<small class="value-verified">数值已核对</small>':''}`;
}
function adoptionLabel(issuer){return `<span class="status${issuer.MonitoringStatus==='paused'?' paused':''}">${issuer.MonitoringStatus==='paused'?'暂停采用':'待复核'}</span>`;}
async function api(action,id){
  let r=await fetch('api.ashx?action='+action+(id?'&id='+encodeURIComponent(id):''),{cache:'no-store'});
  if(!r.ok)throw new Error('数据读取失败（'+r.status+'），请刷新重试。');return r.json();
}
function showError(e){$('error').textContent=e.message;$('error').hidden=false;}
function detailMetric(r,code){return detail.metrics.find(m=>m.FinancialRecordId===r?.FinancialRecordId&&m.MetricCode===code);}
function ratingSummary(issuerId,dates=false){
  const rows=(overview.ratings||[]).filter(r=>r.IssuerId===issuerId);
  return rows.map(r=>`<div>${esc(dates?`${day(r.RatingDate)} / ${r.Outlook||'—'}`:`${r.Agency}：${r.RatingAction==='withdrawn'?'已撤销':r.Grade}`)}<small>待复核</small></div>`).join('')||'<span class="unavailable">暂无候选</span>';
}
function renderOverview(){
  const q=$('search').value.trim().toLowerCase(),t=$('type').value,a=$('adoption').value;
  const rows=overview.issuers.filter(x=>(!q||(x.LegalName+x.DisplayName).toLowerCase().includes(q))&&(!t||x.InstitutionType===t)&&(!a||x.MonitoringStatus===a));
  const latest=overview.records.reduce((p,r)=>day(r.PeriodEnd)>p?day(r.PeriodEnd):p,'');
  $('issuerCount').textContent=overview.issuers.length;
  $('issuerComposition').textContent=`银行 ${overview.issuers.filter(x=>x.InstitutionType==='bank').length} 家 · 资产管理公司 ${overview.issuers.filter(x=>x.InstitutionType==='amc').length} 家`;
  $('latestPeriod').textContent=latest?period(latest):'暂无数据';
  $('periodCoverage').textContent=`${overview.records.filter(r=>day(r.PeriodEnd)===latest).length} 家已登记该报告期 · 数值完整性逐项核对`;
  $('summary').textContent=`显示 ${rows.length} / ${overview.issuers.length} 家主体`;
  $('overviewRows').innerHTML=rows.map(x=>{
    const r=overview.records.find(r=>r.IssuerId===x.IssuerId);
    return `<tr data-issuer-row="${esc(x.IssuerId)}"><td class="sticky-name"><a class="issuer-link" href="#issuer=${encodeURIComponent(x.IssuerId)}">${esc(x.LegalName)}</a><small class="issuer-alias">${esc(x.DisplayName)} · ${x.DocumentCount} 份材料</small>${x.MonitoringStatus==='paused'?adoptionLabel(x):''}</td><td><span class="type-tag">${typeName(x.InstitutionType)}</span></td><td><button class="metric product-count" data-products="${esc(x.IssuerId)}" aria-label="${esc(x.LegalName)}不良 ABS 累计 ${x.NplProductCount} 个项目，查看产品列表">${x.NplProductCount}</button><small class="product-count-note">累计项目</small></td><td>${r?day(r.PeriodEnd):'待识别'}<small class="issuer-alias">${r?period(r.PeriodEnd):''}</small></td>${codes.map(code=>`<td>${metricMarkup(overview.metrics.find(m=>m.IssuerId===x.IssuerId&&m.MetricCode===code),x.IssuerId)}</td>`).join('')}<td>${ratingSummary(x.IssuerId)}</td><td>${ratingSummary(x.IssuerId,true)}</td><td>${adoptionLabel(x)}</td></tr>`;
  }).join('')||'<tr><td colspan="15" class="empty">没有找到符合条件的发行主体</td></tr>';
}
function chosenRows(){return detail.records.filter(r=>!$('year').value||day(r.PeriodEnd).startsWith($('year').value));}
function renderMetrics(){
  $('metricRows').innerHTML=chosenRows().map(r=>`<tr><td><strong>${period(r.PeriodEnd)}</strong><small>截至 ${day(r.PeriodEnd)}</small></td>${codes.map(c=>`<td>${metricMarkup(detailMetric(r,c),undefined,true)}</td>`).join('')}<td>${adoptionLabel(detail.issuer)}</td></tr>`).join('')||'<tr><td colspan="10" class="empty">当前年度暂无财务指标</td></tr>';
}
function comparable(a,b){
  if(!hasValue(a)||!hasValue(b))return false;
  for(const k of ['EntityScopeCode','AccountingBasisCode','MeasurementBasisCode','UnitCode']){
    if(!a[k]||a[k]==='unknown'||a[k]!==b[k])return false;
  }
  try{if(JSON.parse(a.ReviewNote||'{}').conflict||JSON.parse(b.ReviewNote||'{}').conflict)return false;}catch{return false;}
  return a.MetricCode===b.MetricCode;
}
function renderKpis(){
  const latest=detail.records[0];
  $('kpiPeriod').textContent=latest?`${period(latest.PeriodEnd)} · 最新报告期 · 金额单位：亿元`:'暂无报告期';
  const prior=latest?detail.records.find(r=>day(r.PeriodEnd)===(Number(day(latest.PeriodEnd).slice(0,4))-1)+'-12-31'):null;
  $('kpis').innerHTML=codes.slice(0,4).map((code,i)=>{
    const m=detailMetric(latest,code),old=detailMetric(prior,code);let change='缺少同口径上年末数据，暂不计算变化';
    if(comparable(m,old)){
      const a=Number(m.NumericValue),b=Number(old.NumericValue);
      if(code==='total_assets'&&b!==0){const d=(a-b)/Math.abs(b)*100;change=`较上年末 ${d>0?'+':''}${d.toFixed(2)}%`;}
      else if(code!=='total_assets'){const d=a-b;change=`较上年末 ${d>0?'+':''}${d.toFixed(2)} 个百分点`;}
    }
    return `<article class="kpi-card"><div class="kpi-label">${names[i]}${i===0?'（亿元）':''}</div><div class="kpi-value">${metricMarkup(m)}</div><div class="kpi-change">${change}</div></article>`;
  }).join('');
}
function renderChart(){
  const code=$('metricSelect').value,index=codes.indexOf(code),flow=index>=4,latest=detail.records[0];
  let records=[...detail.records].reverse();
  if(flow&&latest)records=records.filter(r=>day(r.PeriodEnd).slice(5)===day(latest.PeriodEnd).slice(5));
  const data=records.map(r=>({r,m:detailMetric(r,code)}));
  const values=data.filter(p=>hasValue(p.m));
  $('chartEmpty').hidden=values.length>0;$('trendChart').hidden=!values.length;
  $('chartEmpty').textContent='当前指标尚无可展示的数值，可在历史财务指标中查看缺项原因。';
  $('chartCaption').textContent=(flow?'仅比较最新季度对应的历年累计值。':'按报告期末展示。')+' 实线：同口径、同准则；虚线：口径或准则不同／未确认，仅供观察披露值趋势。缺值断开，数据点可查看证据。';
  if(!values.length){$('trendChart').innerHTML='';return;}
  const unit=index===0||flow?'亿元':'%',scale=unit==='亿元'?1e8:1;
  const nums=values.map(p=>Number(p.m.NumericValue)/scale),lo=Math.min(...nums),hi=Math.max(...nums);
  const gap=Math.max(hi-lo,Math.abs(hi)*.08,.1),min=lo-gap*.2,max=hi+gap*.2;
  const width=Math.min(760,Math.max(360,$('trendChart').parentElement.clientWidth-24)),right=width-30;
  $('trendChart').setAttribute('viewBox',`0 0 ${width} 286`);
  const x=i=>62+(data.length===1?(right-62)/2:i/(data.length-1)*(right-62)),y=v=>230-(v-min)/(max-min)*196;
  let svg=`<title>${esc(names[index])}趋势（${unit}），待业务复核</title><desc>实线表示同口径同准则，虚线仅作披露值趋势参考；空值不按零处理。</desc>`;
  for(let i=0;i<5;i++){const value=min+(max-min)*i/4,py=y(value);svg+=`<line x1="62" x2="${right}" y1="${py}" y2="${py}" stroke="#e8eff8"/><text x="53" y="${py+4}" text-anchor="end" fill="#7d8ba0" font-size="11">${value.toLocaleString('zh-CN',{maximumFractionDigits:1})}</text>`;}
  data.forEach((p,i)=>{
    if(i===0||i===data.length-1||data.length<=4||(width>600&&i%2===0&&i<data.length-2))svg+=`<text x="${x(i)}" y="256" text-anchor="middle" fill="#7d8ba0" font-size="11">${day(p.r.PeriodEnd).slice(0,7).replace('-','.')}</text>`;
    if(!hasValue(p.m))return;
    const px=x(i),py=y(Number(p.m.NumericValue)/scale);
    if(i&&hasValue(data[i-1].m))svg+=`<line data-trend-segment="${comparable(p.m,data[i-1].m)?'comparable':'reference'}" x1="${x(i-1)}" y1="${y(Number(data[i-1].m.NumericValue)/scale)}" x2="${px}" y2="${py}" stroke="#3478f6" stroke-width="2.5" ${comparable(p.m,data[i-1].m)?'':'stroke-dasharray="5 5" opacity=".65"'}><title>${comparable(p.m,data[i-1].m)?'同口径、同准则':'口径或准则不同／未确认，仅供观察披露值趋势'}</title></line>`;
    const label=`${period(p.r.PeriodEnd)} ${display(p.m)}${unit==='亿元'?' 亿元':''} ${scopeLabel(p.m)}，查看证据`;
    svg+=`<a href="#evidence-${esc(p.m.ObservationId)}" data-observation="${esc(p.m.ObservationId)}" aria-label="${esc(label)}"><title>${esc(label)}</title><circle cx="${px}" cy="${py}" r="12" fill="transparent"/><circle cx="${px}" cy="${py}" r="4.5" fill="white" stroke="#165dff" stroke-width="2.5"/></a>`;
    if(i===data.length-1)svg+=`<text x="${px}" y="${py-13}" text-anchor="end" fill="#165dff" font-size="12" font-weight="600">${esc(display(p.m))}${unit==='亿元'?' 亿元':''}</text>`;
  });
  $('trendChart').innerHTML=svg;
}
function reportType(d){return d.DocumentType==='issuer_rating'?'主体评级报告':/资本|第三支柱/.test(d.Title)?'资本披露':/年报|年度报告|半年度|季度/.test(d.Title)?'财务报告':'财报相关材料';}
function renderRatings(){
  const rows=detail.ratings||[];
  const latest=new Map();for(const r of rows)if(!latest.has(r.Agency))latest.set(r.Agency,r);
  document.querySelector('.hero-rating-value').textContent=rows.length?`${latest.size} 家机构`:'暂无评级候选';
  document.querySelector('.hero-rating-date').textContent=rows.length?'按机构分别展示 · 待业务复核':'未取得有效候选不代表未披露';
  $('ratingsPanel').innerHTML='<div class="tab-info">按原文实际日期及机构保留；评级日期来自原文，所有候选待业务复核。</div><div class="table-scroll"><table class="document-table"><thead><tr><th>评级机构</th><th>评级日期</th><th>主体等级</th><th>展望</th><th>事件</th><th>证据 / 状态</th></tr></thead><tbody>'+rows.map(r=>{
    let evidence={};try{evidence=JSON.parse(r.EvidenceJson||'{}');}catch{}
    const pages=[...new Set(Object.values(evidence).map(e=>e.page).filter(p=>Number.isInteger(p)&&p>0))];
    return `<tr><td>${esc(r.Agency)}</td><td>${esc(day(r.RatingDate))}</td><td>${esc(r.Grade||'—')}<small>前次：${esc(r.PreviousGrade||'未给')} · ${esc(r.ChangeDirection||'未知')}</small></td><td>${esc(r.Outlook||'—')}</td><td>${esc({initial:'首次评级',follow_up:'跟踪评级',outlook_change:'展望调整',withdrawn:'撤销 / 终止'}[r.RatingAction]||r.RatingAction)}</td><td>${pages.map(p=>`<a href="api.ashx?action=document&amp;id=${encodeURIComponent(r.DocumentId)}#page=${p}" target="_blank" rel="noopener">原文第 ${p} 页</a>`).join(' · ')}<span class="status">待复核</span><details><summary>查看原文依据</summary>${Object.values(evidence).map(e=>`<p>${esc(e.quote||'')}</p>`).join('')}</details></td></tr>`;
  }).join('')+'</tbody></table>'+(rows.length?'':'<p class="empty">暂无通过证据校验的主体评级候选；未找到与检索失败分别记录。</p>')+'</div>';
}
function renderDocuments(){
  $('allDocCount').textContent=detail.documents.length;$('financeDocCount').textContent=detail.documents.filter(d=>d.DocumentType!=='issuer_rating').length;
  document.querySelectorAll('[data-doc-filter]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.docFilter===documentFilter)));
  $('documents').innerHTML=detail.documents.filter(d=>documentFilter==='all'||(documentFilter==='rating'?d.DocumentType==='issuer_rating':d.DocumentType!=='issuer_rating')).map(d=>`<tr><td><a href="api.ashx?action=document&amp;id=${encodeURIComponent(d.DocumentId)}" download="${esc(d.Title)}" aria-label="下载 ${esc(d.Title)}">${esc(d.Title)}</a></td><td>${reportType(d)}</td><td>${d.DocumentType==='issuer_rating'?'—':day(d.MainPeriodEnd)||'待确认'}</td><td>${day(d.PublishedDate)||'<span class="unavailable">待补充</span>'}</td><td>PDF · ${d.PageCount} 页<small>${(d.ByteSize/1048576).toFixed(1)} MB · ${d.ArchiveStatus==='archived'?'已归档':'归档处理中'}</small></td><td><div class="doc-actions"><a href="api.ashx?action=document&amp;id=${encodeURIComponent(d.DocumentId)}" download="${esc(d.Title)}">下载</a></div></td></tr>`).join('')||'<tr><td colspan="6" class="empty">暂无归档材料</td></tr>';
}
function renderTimeline(){
  const dated=detail.documents.filter(d=>d.PublishedDate).sort((a,b)=>String(b.PublishedDate).localeCompare(String(a.PublishedDate)));
  const items=(dated.length?dated:detail.documents).slice(0,3);
  $('recentTimeline').innerHTML=(!dated.length?'<p class="muted">披露日期待补充，以下按报告期排列。</p>':'')+items.map(d=>`<div class="timeline-item"><a href="api.ashx?action=document&amp;id=${encodeURIComponent(d.DocumentId)}" download="${esc(d.Title)}" aria-label="下载 ${esc(d.Title)}">${esc(d.Title)}</a><p>报告期 ${day(d.MainPeriodEnd)||'待确认'}<br>披露日期 ${day(d.PublishedDate)||'待补充'}</p></div>`).join('');
}
function renderDetail(){
  const x=detail.issuer;let riskLink=document.getElementById('issuerRiskLink');if(!riskLink){riskLink=document.createElement('a');riskLink.id='issuerRiskLink';riskLink.className='secondary-btn';document.querySelector('#detail').prepend(riskLink);}riskLink.href='risk.html?id='+encodeURIComponent(x.IssuerId);riskLink.textContent='风险信息与建议分';$('name').textContent=x.LegalName;$('name').title=x.LegalName;$('name').hidden=false;$('appTitle').hidden=true;$('detailLogo').textContent=x.DisplayName.replace(/^中国/,'').slice(0,2);
  $('institution').textContent=typeName(x.InstitutionType);$('detailStatus').textContent=`${detail.documents.length} 份报告 · ${detail.records.length} 个报告期`;
  $('sourceHold').hidden=x.MonitoringStatus!=='paused';
  document.querySelector('.review-line .status').outerHTML=adoptionLabel(x);
  document.title=x.DisplayName+' · DealViewer';
  const years=[...new Set(detail.records.map(r=>day(r.PeriodEnd).slice(0,4)))],params=new URLSearchParams(location.hash.slice(1));
  $('year').innerHTML='<option value="">全部年度</option>'+years.map(y=>`<option>${y}</option>`).join('');
  if(years.includes(params.get('year')))$('year').value=params.get('year');
  $('metricSelect').innerHTML=codes.map((c,i)=>`<option value="${c}">${names[i]}（${i===0||i>=4?'亿元':'%'}）</option>`).join('');
  if(codes.includes(params.get('metric')))$('metricSelect').value=params.get('metric');
  $('tasks').innerHTML=detail.tasks.map(t=>`<div class="task"><div><strong>${statusName(t.Status)}</strong><p>${t.OutcomeCode==='extraction_incomplete'?'部分识别任务尚未完成':t.Status==='needs_review'?'识别结果已入库，等待业务复核':'处理进度已保存'}</p></div><small>${esc((t.FinishedAt||t.CreatedAt||'').replace('T',' '))} UTC</small></div>`).join('')||'<div class="empty">暂无处理记录</div>';
  renderKpis();renderMetrics();renderChart();renderDocuments();renderTimeline();renderRatings();switchTab(params.get('tab')||'metrics',false);
}
function setHashParam(k,v){const p=new URLSearchParams(location.hash.slice(1));if(v)p.set(k,v);else p.delete(k);history.replaceState(null,'',location.pathname+location.search+'#'+p);}
function switchTab(tab,update=true){
  if(tab==='documents'){tab='metrics';$('documentTitle').scrollIntoView({block:'start'});}
  if(!['metrics','ratings','tasks'].includes(tab))tab='metrics';
  document.querySelectorAll('[role=tab]').forEach(b=>{b.setAttribute('aria-selected',String(b.dataset.tab===tab));b.tabIndex=b.dataset.tab===tab?0:-1;});
  for(const t of ['metrics','ratings','tasks'])$(t+'Panel').hidden=t!==tab;
  if(update)setHashParam('tab',tab);
}
async function route(){
  const generation=++requestGeneration,id=new URLSearchParams(location.hash.slice(1)).get('issuer');
  $('error').hidden=true;$('evidence').close();evidenceGeneration++;$('products').close();productGeneration++;$('loading').hidden=false;
  $('overview').hidden=!!id;$('detail').hidden=!id;$('name').hidden=!id;$('appTitle').hidden=!!id;$('name').textContent=id?'正在读取主体资料…':'';$('name').title='';
  try{
    if(id){
      // Hide the prior issuer's data while loading, including when navigating rapidly.
      $('detail').hidden=true;
      const data=await api('issuer',id);if(generation!==requestGeneration)return;
      detail=data;documentFilter='all';$('detail').hidden=false;renderDetail();
    }else{
      const data=await api('overview');if(generation!==requestGeneration)return;
      overview=data;renderOverview();document.title='发行主体财报与评级监测 · DealViewer';
    }
    $('readAt').textContent='读取于 '+new Date().toLocaleString('zh-CN');
  }catch(e){if(generation===requestGeneration)showError(e);}
  finally{if(generation===requestGeneration)$('loading').hidden=true;}
}
async function evidence(id,issuerId,allowReview=false){
  const generation=++evidenceGeneration;
  $('evidenceTitle').textContent='指标证据';$('evidenceBody').textContent='正在读取原文证据…';$('evidence').showModal();
  try{
    const context=issuerId?await api('issuer',issuerId):detail;
    if(generation!==evidenceGeneration)return;
    const m=context.metrics.find(x=>x.ObservationId===id);if(!m)throw new Error('该指标已更新，请刷新后重试。');
    const r=context.records.find(x=>x.FinancialRecordId===m.FinancialRecordId);
    $('evidenceTitle').textContent=context.issuer.DisplayName+' · '+period(r.PeriodEnd)+' · '+names[codes.indexOf(m.MetricCode)];
    let note={};try{note=JSON.parse(m.ReviewNote||'{}');}catch{}
    const scopes={accounting_consolidated:'会计集团',regulatory_consolidated:'监管并表',standalone:'本行 / 母公司',unknown:'未确认'};
    $('evidenceBody').innerHTML=`<div class="detail-value">${display(m)} ${m.UnitCode==='yuan'?'亿元':''}</div><div class="meta"><span>范围：${esc(scopes[m.EntityScopeCode]||m.EntityScopeCode)}</span><span>准则：${esc(m.AccountingBasisCode==='unknown'?'依据待确认':m.AccountingBasisCode==='not_applicable'?'不适用':m.AccountingBasisCode)}</span><span>${day(m.PeriodStart)} 至 ${day(m.PeriodEnd)}</span></div><p class="warning">${context.issuer.MonitoringStatus==='paused'?'该主体暂停业务采用。 ':''}${m.ReviewStatus==='pending'?'当前数值尚未完成业务复核。':''} ${note.conflict?'同口径候选存在不同数值，需要核对。':''}</p><div id="evidenceItems">正在读取原文证据…</div>`;
    const rows=await api('evidence',id);if(generation!==evidenceGeneration)return;
    $('evidenceItems').innerHTML=(note.reason?`<p class="notice">${esc(note.reason)}</p>`:'')+(allowReview&&context.issuer.MonitoringStatus==='active'?reviewButton(m,context.issuer.IssuerId):'')+rows.map(e=>`<section class="evidence-item"><h3>${e.EvidenceRole==='value'?'数值来源':'单位 / 口径 / 准则依据'} · PDF 第 ${e.PdfPage} 页</h3><a target="_blank" rel="noopener" href="api.ashx?action=document&amp;id=${encodeURIComponent(e.DocumentId)}#page=${e.PdfPage}">${esc(e.Title)} ↗</a><p class="muted">${e.Method==='manual_review'?'人工核对录入':e.Method==='visual_transcription'?'原图核对转录，待人工复核':e.Method==='paddleocr'?'OCR 识别，需核对扫描原图':'PDF 文字提取'} · 原值 ${esc(e.RawValue||'—')} · 原单位 ${esc(e.RawUnit||'—')}</p><pre>${esc(e.RawText)}</pre></section>`).join('')||'暂无证据';
  }catch(e){if(generation===evidenceGeneration)$('evidenceBody').textContent=e.message;}
}
function reviewIssuer(issuerId){
  return issuerId?overview?.issuers.find(i=>i.IssuerId===issuerId)||(detail?.issuer.IssuerId===issuerId?detail.issuer:null):detail?.issuer;
}
function reviewRestriction(m,issuer){
  if(!issuer)return '主体资料尚未读取，请刷新后重试。';
  if(issuer.MonitoringStatus!=='active')return '主体暂停采用，暂不补录。历史资料和证据保留。';
  if(!['bank','amc'].includes(issuer.InstitutionType))return '该机构类型暂不开放核对录入。';
  if(!m?.FinancialRecordId)return '该报告期尚未登记此指标，暂不能录入。';
  if(issuer.InstitutionType==='amc'&&codes.slice(1,4).includes(m.MetricCode))return '资产管理公司不在此处录入银行监管指标；定义不同的指标不混用。';
  return '';
}
function reviewButton(m,issuerId){
  if(reviewRestriction(m,reviewIssuer(issuerId)))return '';
  return `<button type="button" class="review-link secondary-btn" data-review-record="${esc(m.FinancialRecordId)}" data-review-code="${esc(m.MetricCode)}" data-review-issuer="${esc(issuerId||detail?.issuer.IssuerId||'')}">${hasValue(m)?'核对 / 更正':'核对并录入'}</button>`;
}
let reviewContext=null;
async function openReview(button){
  try{
    const context=await api('issuer',button.dataset.reviewIssuer);
    const m=context.metrics.find(m=>m.FinancialRecordId===button.dataset.reviewRecord&&m.MetricCode===button.dataset.reviewCode);
    if(!m)throw new Error('数据已经更新，请刷新后重试。');
    const blocked=reviewRestriction(m,context.issuer);if(blocked)throw new Error(blocked);
    const record=context.records.find(r=>r.FinancialRecordId===m.FinancialRecordId);
    const docs=context.documents.filter(d=>day(d.MainPeriodEnd)===day(record.PeriodEnd));
    if(!docs.length)throw new Error('该报告期尚无已归档原文，需先补齐材料后再录入。');
    const sources=m.ObservationId?await api('evidence',m.ObservationId):[];
    const source=sources.find(e=>e.EvidenceRole==='value');
    let note={};try{note=JSON.parse(m.ReviewNote||'{}');}catch{}
    const hint=note.missing_evidence?.searched_documents?.[0];
    const form=$('reviewForm');form.reset();$('reviewError').hidden=true;$('reviewResult').hidden=true;
    const amc=context.issuer.InstitutionType==='amc';
    $('reviewDefinitionRow').hidden=!amc;$('reviewDefinition').required=amc;
    $('reviewTitle').textContent=context.issuer.DisplayName+' · '+period(record.PeriodEnd)+' · '+names[codes.indexOf(m.MetricCode)];
    $('reviewDocument').innerHTML=docs.map(d=>`<option value="${d.DocumentId}">${esc(d.Title)}</option>`).join('');
    if(source||hint)$('reviewDocument').value=String(source?.DocumentId||hint.document_id);
    if(!$('reviewDocument').value&&docs[0])$('reviewDocument').value=docs[0].DocumentId;
    $('reviewPage').value=source?.PdfPage||hint?.hit_pages?.[0]||1;
    $('reviewValue').value=m.NumericValue||'';
    const ratio=codes.indexOf(m.MetricCode)>=1&&codes.indexOf(m.MetricCode)<=3;
    $('reviewUnit').innerHTML=(ratio?['%']:['元','千元','万元','百万元','亿元']).map(u=>`<option>${u}</option>`).join('');
    const rawUnit=source&&['百万元','亿元','万元','千元','元','%'].find(u=>(source.RawUnit||'').includes(u));
    if(rawUnit&&[...$('reviewUnit').options].some(o=>o.value===rawUnit)&&/^-?[\d,]+(\.\d+)?$/.test(source.RawValue||'')){
      $('reviewValue').value=source.RawValue;$('reviewUnit').value=rawUnit;
    }
    $('reviewScope').innerHTML=(amc?[['','请选择原文披露范围'],['accounting_consolidated','会计集团 / 合并'],['standalone','母公司']]:[['unknown','未明确，按本行处理'],['standalone','本行 / 母公司'],['accounting_consolidated','会计集团 / 合并'],['regulatory_consolidated','监管并表（资本指标）']]).map(([v,l])=>`<option value="${v}">${l}</option>`).join('');
    $('reviewScope').value=amc?(m.EntityScopeCode==='unknown'?'':m.EntityScopeCode||''):(m.EntityScopeCode||'unknown');
    $('reviewBasis').value=ratio?'unknown':m.AccountingBasisCode||'unknown';$('reviewBasis').disabled=ratio;
    $('reviewQuote').value=source?.RawText||'';
    reviewContext={context,m,record,docs,requestId:crypto.randomUUID()};
    $('evidence').close();$('review').showModal();updateReviewDocument();
  }catch(e){showError(e);}
}
function updateReviewDocument(){
  const d=reviewContext?.docs.find(d=>d.DocumentId===$('reviewDocument').value);
  $('reviewPage').max=d?.PageCount||1;
  $('reviewSource').href='api.ashx?action=document&id='+encodeURIComponent(d?.DocumentId||'')+'#page='+encodeURIComponent($('reviewPage').value||1);
}
async function submitReview(event){
  event.preventDefault();if(!reviewContext)return;
  const {context,m,record,docs,requestId}=reviewContext,doc=docs.find(d=>d.DocumentId===$('reviewDocument').value);
  const data={issuer_id:context.issuer.IssuerId,expected_record_id:m.FinancialRecordId,expected_observation_id:m.ObservationId,
    metric_code:m.MetricCode,period_end:day(record.PeriodEnd),document_id:doc.DocumentId,document_sha256:doc.Sha256,
    page:$('reviewPage').value,raw_value:$('reviewValue').value,raw_unit:$('reviewUnit').value,
    scope:$('reviewScope').value,basis:$('reviewBasis').value,reviewer:$('reviewerName').value,
    reason:$('reviewReason').value,quote:$('reviewQuote').value,confirmed:$('reviewConfirmed').checked,
    definition_confirmed:$('reviewDefinition').checked,request_id:requestId};
  $('reviewSave').disabled=true;$('reviewError').hidden=true;
  try{
    const r=await fetch('api.ashx?action=review',{method:'POST',headers:{'Content-Type':'application/json','X-Review-Key':$('reviewKey').value},body:JSON.stringify(data)});
    const result=await r.json();if(!r.ok)throw new Error(result.error||'保存失败，请重试。');
    $('reviewResult').textContent='已确认入库。原值与修改记录已保留，关闭后可查看更新的数值。';$('reviewResult').hidden=false;
    $('reviewKey').value='';reviewContext=null;await route();
  }catch(e){$('reviewError').textContent=e.message;$('reviewError').hidden=false;$('reviewSave').disabled=false;}
}
function initReview(){
  $('reviewForm').addEventListener('submit',submitReview);
  $('reviewClose').onclick=()=>{$('review').close();$('reviewKey').value='';};
  $('review').addEventListener('cancel',()=>{$('reviewKey').value='';});
  $('reviewForm').addEventListener('input',()=>{if(reviewContext)reviewContext.requestId=crypto.randomUUID();});
  $('reviewDocument').onchange=updateReviewDocument;$('reviewPage').oninput=updateReviewDocument;
  document.addEventListener('click',e=>{const b=e.target.closest('[data-review-record]');if(b){$('reviewSave').disabled=false;openReview(b);}});
}
// Keep the window's vertical scrolling and the original table's horizontal
// scrolling. A noninteractive, aria-hidden copy follows the measured columns.
function initPinnedHeaders(){
  const tables=[...document.querySelectorAll('.table-scroll > table')].map(table=>{
    const overlay=document.createElement('div');
    overlay.className='pinned-table-head';overlay.hidden=true;
    overlay.setAttribute('aria-hidden','true');overlay.inert=true;document.body.append(overlay);
    return {table,wrap:table.parentElement,overlay,signature:''};
  });
  let frame=0;
  function update(){
    frame=0;const top=document.querySelector('.topbar').getBoundingClientRect().bottom;
    for(const state of tables){
      const {table,wrap,overlay}=state,rect=table.getBoundingClientRect(),wr=wrap.getBoundingClientRect();
      const head=table.tHead,hr=head.getBoundingClientRect();
      const visible=table.getClientRects().length&&hr.top<top&&rect.bottom>top+hr.height;
      overlay.hidden=!visible;if(!visible)continue;
      const row=[...table.tBodies[0].rows].find(r=>[...r.cells].every(c=>c.colSpan===1));
      if(!row){overlay.hidden=true;continue;}
      const widths=[...row.cells].map(c=>c.getBoundingClientRect().width);
      const signature=[rect.width,hr.height,...widths].join('|');
      if(signature!==state.signature){
        const clone=document.createElement('table');clone.className=table.className;
        clone.style.width=rect.width+'px';clone.style.minWidth=rect.width+'px';
        const cols=document.createElement('colgroup');
        widths.forEach(w=>{const col=document.createElement('col');col.style.width=w+'px';cols.append(col);});
        clone.append(cols,head.cloneNode(true));clone.querySelectorAll('[id]').forEach(n=>n.removeAttribute('id'));
        overlay.replaceChildren(clone);state.signature=signature;
      }
      Object.assign(overlay.style,{top:top+'px',left:wr.left+'px',width:wrap.clientWidth+'px',height:hr.height+'px'});
      overlay.scrollLeft=wrap.scrollLeft;
    }
  }
  const schedule=()=>{if(!frame)frame=requestAnimationFrame(update);};
  document.addEventListener('scroll',schedule,true);window.addEventListener('resize',schedule);
  document.addEventListener('toggle',schedule,true);
  const observer=new ResizeObserver(schedule);tables.forEach(s=>observer.observe(s.table));
  new MutationObserver(schedule).observe($('overview'),{attributes:true,attributeFilter:['hidden']});
  new MutationObserver(schedule).observe($('detail'),{attributes:true,attributeFilter:['hidden'],subtree:true});
  schedule();
}
function csvSafe(v){const s=String(v);return /^[=+@\r\n]/.test(s)||/^-(?!\d)/.test(s)?'\t'+s:s;}
function exportCsv(){
  const rows=[['主体','报告期','指标','显示值','原始入库值（文本，避免科学计数）','单位','口径','准则','复核状态','缺项原因','主体采用状态']];
  for(const r of chosenRows())for(const code of codes){
    const m=detailMetric(r,code)||{};
    rows.push([detail.issuer.LegalName,day(r.PeriodEnd),names[codes.indexOf(code)],display(m),m.NumericValue==null?'':'\t'+String(m.NumericValue),({yuan:'元',percent:'%'})[m.UnitCode]||'',({accounting_consolidated:'会计集团',regulatory_consolidated:'监管并表',standalone:'本行 / 母公司',unknown:'未确认'})[m.EntityScopeCode]||'',m.AccountingBasisCode||'',({pending:'待复核',verified:'已核对',corrected:'已更正'})[m.ReviewStatus]||'待复核',m.MissingReason||'',detail.issuer.MonitoringStatus==='paused'?'暂停采用':'待业务核准']);
  }
  const csv='\ufeff'+rows.map(r=>r.map(v=>'"'+csvSafe(v).replace(/"/g,'""')+'"').join(',')).join('\r\n');
  const url=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'})),a=document.createElement('a');
  a.href=url;a.download=detail.issuer.DisplayName+'-财务指标.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
async function boot(){
  // A tab kept open across a deployment may have the previous HTML. Restore the
  // current shell once before attaching listeners, without a reload loop.
  if(!$('appTitle')||!$('reviewDefinition')||!$('products')){
    try{
      const response=await fetch('index.html?v=20261027-20',{cache:'reload'});
      if(!response.ok)throw new Error('页面更新失败，请刷新。');
      const doc=new DOMParser().parseFromString(await response.text(),'text/html');
      if(!doc.getElementById('products'))throw new Error('页面版本更新中，请刷新。');
      document.body.replaceChildren(...doc.body.childNodes);
      const sheet=document.querySelector('link[rel=stylesheet]');if(sheet)sheet.href='style.css?v=20261027-20';
    }catch(e){document.body.textContent=e.message;return;}
  }
  $('search').addEventListener('input',()=>overview&&renderOverview());$('type').onchange=()=>overview&&renderOverview();$('adoption').onchange=()=>overview&&renderOverview();$('refresh').onclick=route;
  $('year').onchange=()=>{renderMetrics();setHashParam('year',$('year').value);};
  $('metricSelect').onchange=()=>{renderChart();setHashParam('metric',$('metricSelect').value);};
  document.querySelectorAll('[role=tab]').forEach(b=>{b.onclick=()=>switchTab(b.dataset.tab);b.onkeydown=e=>{
    const tabs=[...document.querySelectorAll('[role=tab]')],i=tabs.indexOf(b);let next;
    if(e.key==='ArrowRight')next=tabs[(i+1)%tabs.length];if(e.key==='ArrowLeft')next=tabs[(i+tabs.length-1)%tabs.length];
    if(e.key==='Home')next=tabs[0];if(e.key==='End')next=tabs[tabs.length-1];
    if(next){e.preventDefault();switchTab(next.dataset.tab);next.focus();}
  };});
  document.querySelectorAll('[data-doc-filter]').forEach(b=>b.onclick=()=>{documentFilter=b.dataset.docFilter;renderDocuments();});
  document.addEventListener('click',e=>{const b=e.target.closest('[data-observation]');if(b){e.preventDefault();evidence(b.dataset.observation,b.dataset.issuer,!!b.closest('#metricRows'));}const link=e.target.closest('.issuer-link,.breadcrumb');if(link)window.scrollTo(0,0);const jump=e.target.closest('[data-jump]');if(jump){if(jump.dataset.jump==='metricsTab')switchTab('metrics');$(jump.dataset.jump).scrollIntoView({block:'start'});}});
  let resizeFrame;window.addEventListener('resize',()=>{cancelAnimationFrame(resizeFrame);resizeFrame=requestAnimationFrame(()=>{if(detail&&!$('detail').hidden)renderChart();});});
  $('close').onclick=()=>{$('evidence').close();evidenceGeneration++;};$('evidence').addEventListener('cancel',()=>evidenceGeneration++);
  $('productsClose').onclick=()=>{$('products').close();productGeneration++;};
  $('products').addEventListener('cancel',()=>productGeneration++);$('productStatus').onchange=renderProducts;
  document.addEventListener('click',e=>{const b=e.target.closest('[data-products]');if(b)openProducts(b.dataset.products);});
  initReview();initPinnedHeaders();$('csv').onclick=exportCsv;window.addEventListener('hashchange',route);route();
}
boot();
