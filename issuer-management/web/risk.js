'use strict';
const $=id=>document.getElementById(id), esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels={penalty:'监管处罚',rating:'主体评级下调',default:'公开市场违约',pending:'待确认',confirmed:'已确认适用',excluded:'已排除',below_threshold:'未达阈值',other_debtor:'非本法人（代发）'};
const stamp=s=>s?new Date(s.replace(' ','T').replace(/\.\d+/,m=>m.slice(0,4))+'Z').toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'—';
let issuers=[],risk=null,mappings=[],current=null,issuerDetails=null,activeIndicator=null;
const params=new URLSearchParams(location.search),mapping=params.has('mapping');
async function api(action,id){const r=await fetch(`api.ashx?action=${action}${id?'&id='+encodeURIComponent(id):''}`);const d=await r.json();if(!r.ok)throw Error(d.error||'读取失败');return d;}
function safeUrl(url){try{const u=new URL(url);return ['https:','http:'].includes(u.protocol)?u.href:'';}catch{return '';}}
function eventCard(e){const f=JSON.parse(e.FieldsJson),evidence=JSON.parse(e.EvidenceJson);return `<article class="event"><p class="event-indicator">所属指标：${esc(indicatorName(e.Category))}</p><h3>${esc(labels[e.Category])} <span class="tag">${esc(labels[e.Status])}</span></h3><p>事件日：${esc(e.EventDate||'待确认')} · 披露日：${esc(e.PublishedDate||'未提供')} · 发现：${esc(stamp(e.DiscoveredAt))}</p><p>${esc(e.Category==='penalty'?`当事人：${f.party||'待核清'}；决定书：${f.decision_number||'待核清'}；罚款合计：${f.fine_total??'待核清'} ${f.unit||'元'}`:preKinds.includes(e.Category)?'核对实际发生事实、产品关系及适用条件':e.Category==='rating'?`${f.agency||''}：${f.previous_grade||'前次未给'} → ${f.grade||'未提供'}；${f.change_direction||'未知'}`:`实际债务人：${f.debtor||'待核清'}；本主体角色：${({lead_underwriter:'主承销商代发',disclosure_agent:'代发机构',debtor:'债务人',guarantor:'担保人',keepwell:'维好义务人',spv:'SPV相关义务'})[f.issuer_role]||'待核清'}`)}</p>${preEvent(e,f)}<p>${esc(e.Notes)}</p>${f.evidence?.review?`<blockquote>更正引文（第 ${esc(f.evidence.review.page)} 页）：${esc(f.evidence.review.quote)}</blockquote>`:''}${evidence.map(s=>`<div>${s.document_id?`<a href="api.ashx?action=document&id=${encodeURIComponent(s.document_id)}" target="_blank" rel="noopener">归档原文</a>`:`<a href="api.ashx?action=risk-original&id=${e.RiskEventId}&hash=${encodeURIComponent(s.sha256)}" target="_blank" rel="noopener">归档网页原件</a>`}${safeUrl(s.source_url)?`<a href="${esc(safeUrl(s.source_url))}" target="_blank" rel="noopener">来源公告</a>`:''}<details><summary>引文及原件哈希</summary><small>${esc(s.sha256)}</small>${Object.values(s.evidence||{}).filter(x=>x&&typeof x==='object').map(q=>`<blockquote>第 ${esc(q.page)} 页：${esc(q.quote)}</blockquote>`).join('')}</details></div>`).join('')}<button data-event="${e.RiskEventId}">核对 / 更正</button></article>`;}
function indicatorName(category){const item=risk?.indicators?.find(x=>x.category===category);return item?`${item.stage_label} ${item.letter}：${item.label}`:labels[category]||category;}
function statusNotes(item){
 const notes=[];
 if(item.status==='not_detected')notes.push('已查渠道未检出，无需人工操作');
 if(item.confirmed_events)notes.push(`近24个月已确认 ${item.confirmed_events} 项`);
 if(item.pending_events)notes.push(`待核对 ${item.pending_events} 项，可点击查看明细核对`);
 if(item.historical_events)notes.push(`历史已确认 ${item.historical_events} 项`);
 if(item.excluded_events)notes.push(`已排除等记录 ${item.excluded_events} 项`);
 if(item.pending_documents)notes.push(`本类别待解析 ${item.pending_documents} 份`);
 if(item.query_end)notes.push(`查询截至 ${item.query_end}`);
 if(item.sources.some(x=>x.status!=='success'||x.stale))notes.push('部分来源未完成查询或检查已过期，详见明细');
 return notes.map(note=>`<small>${esc(note)}</small>`).join('');
}
function renderIndicators(r){
 const items=r.indicators;
 if(!Array.isArray(items)||items.length!==6)throw Error('指标数据暂不可用，请刷新重试。');
 const first=items[0];$('checkWindow').textContent=`业务日期 ${first.business_date}；事件整理区间 ${first.window_start} 至 ${first.business_date}（含首尾）`;
 for(const stage of ['pre','post']){
  $(stage+'Items').innerHTML='<table class="indicator-table"><thead><tr><th>条目</th><th>评价指标</th><th>当前整理结果</th><th>查阅</th></tr></thead><tbody>'+items.filter(x=>x.stage===stage).map(item=>
   `<tr data-category="${esc(item.category)}" data-status="${esc(item.status)}"><td class="indicator-letter">${esc(item.letter)}.</td><td class="indicator-label">${esc(item.label)}</td><td><span class="tag status-${esc(item.status)}">${esc(item.status_label)}</span>${statusNotes(item)}</td><td><button data-indicator="${esc(item.code)}" aria-label="查看${esc(item.stage_label)}${esc(item.letter)}明细">查看明细${item.event_count?'（'+item.event_count+'）':''}</button></td></tr>`
  ).join('')+'</tbody></table>';
 }
 const select=$('category'),previous=select.value;
 select.innerHTML=['pre','post'].map(stage=>`<optgroup label="${stage==='pre'?'投前':'投后'}评价指标">${items.filter(x=>x.stage===stage).map(item=>`<option value="${esc(item.category)}">${esc(item.stage_label)} ${esc(item.letter)} · ${esc(labels[item.category])}</option>`).join('')}</optgroup>`).join('');
 select.value=previous||'abs_suspension';
}
function openIndicator(code){
 const item=risk?.indicators.find(x=>x.code===code);if(!item)return;
 activeIndicator=code;
 $('indicatorTitle').textContent=`${item.stage_label}评价指标 · ${item.letter}`;
 $('indicatorDescription').textContent=item.label;
 $('indicatorStatus').innerHTML=`<p><strong>${esc(item.status_label)}</strong>${statusNotes(item)}</p>`;
 const events=risk.events.filter(e=>e.IndicatorCode===code);
 $('events').innerHTML=events.filter(e=>['pending','confirmed'].includes(e.Status)).map(eventCard).join('')||'<p>暂无已确认或待核对事件，请结合本指标来源与查询状态查阅。</p>';
 $('collapsed').innerHTML=events.filter(e=>!['pending','confirmed'].includes(e.Status)).map(eventCard).join('')||'<p>暂无事项</p>';
 $('excludedDetails').open=false;
 const sourceLabels={success:'查询成功',not_connected:'未接入',not_queried:'尚未查询',failed:'查询失败'};
 $('indicatorSources').innerHTML='<table><thead><tr><th>来源</th><th>查询状态</th><th>检查区间</th></tr></thead><tbody>'+item.sources.map(x=>`<tr><td>${esc(x.label)}</td><td>${esc(sourceLabels[x.status]||'查询未完成')}${x.stale?'<small class="red">尚无近期成功检查</small>':''}<small>待解析 ${esc(x.pending_documents||0)} 份</small><small>${esc(x.failure_reason||'')}</small></td><td>${esc(x.query_start||'未确定')} 至 ${esc(x.query_end||'未确定')}<small>检查 ${esc(stamp(x.checked_at))}</small></td></tr>`).join('')+'</tbody></table>';
 if(item.category==='rating')$('indicatorSources').insertAdjacentHTML('beforeend','<p class="intro">国际评级目前由人工补录，未开展定期国际检索。</p>');
 if(!$('indicatorDialog').open)$('indicatorDialog').showModal();
}
async function load(){
 const id=$('issuer').value;if(!id)return;
 try{
  $('error').hidden=true;
  $('preItems').textContent=$('postItems').textContent='正在读取指标…';
  const [r,d]=await Promise.all([api('risk',id),api('issuer',id)]);if(id!==$('issuer').value)return;
  risk=r;issuerDetails=d;renderIndicators(r);$('title').textContent=d.issuer.LegalName+' · 风险信息';
  $('sources').innerHTML='<table><thead><tr><th>来源</th><th>最近成功检查 / 区间</th><th>本次结果</th><th>采集与导入时间（北京）</th></tr></thead><tbody>'+r.sources.map(x=>`<tr><td>${esc(x.label)}</td><td class="${x.red?'red':''}">${x.success?`${esc(stamp(x.success.CheckedAt))}<small>${esc(x.success.QueryStart||'未确定')} 至 ${esc(x.success.QueryEnd||'未确定')}</small>`:'尚无成功检查'}</td><td>${!x.latest?'尚未查询':x.latest.Status==='not_connected'?'未接入':x.latest.Status==='success'?'查询成功':'查询失败'}<small>${esc(x.latest?.FailureReason||'')}</small><small>待解析 ${x.latest?.PendingDocuments||0} 份</small></td><td>${esc(stamp(x.latest?.CheckedAt))}<small>导入 ${esc(stamp(x.latest?.ImportedAt))}</small></td></tr>`).join('')+'</tbody></table>';
  if($('indicatorDialog').open&&activeIndicator)openIndicator(activeIndicator);
 }catch(e){if(id!==$('issuer').value)return;risk=null;$('preItems').textContent=$('postItems').textContent='指标读取失败，请刷新重试。';$('indicatorDialog').close();$('error').textContent=e.message;$('error').hidden=false;}
}
function renderMappings(){const q=$('mappingSearch').value.trim();const list=mappings.filter(m=>[m.ProductId,m.OriginalProductSubject,m.LegalName].some(v=>String(v||'').includes(q)));$('mappings').innerHTML='<table><thead><tr><th>产品ID</th><th>原始主体</th><th>映射发起人</th><th>来源及依据</th><th>操作</th></tr></thead><tbody>'+list.map(m=>`<tr><td>${esc(m.ProductId)}</td><td>${esc(m.OriginalProductSubject)}</td><td>${esc(m.LegalName||'未解决')}</td><td>${m.MappingSource==='manual'?'手工':'自动'}<small>${esc(m.Basis)}</small></td><td><button data-mapping="${m.ProductId}">修正</button></td></tr>`).join('')+'</tbody></table>';}
function categoryFields(){$('editIndicator').hidden=mapping;$('editIndicator').textContent=mapping?'':'所属指标：'+indicatorName($('category').value);preCategoryFields();for(const [id,c] of [['penaltyFields','penalty'],['ratingFields','rating'],['defaultFields','default']])$(id).hidden=mapping||$('category').value!==c;}
function edit(item,adding=false){current={item,adding};$('form').reset();$('formError').textContent='';$('editTitle').textContent=mapping?'修正产品发起人':adding?'补录风险材料':'核对 / 更正';for(const id of ['categoryRow','statusRow','dateRow','quoteRow','pageRow'])$(id).hidden=mapping;$('statusRow').hidden=mapping||adding;$('mappingIssuerRow').hidden=!mapping;$('documentRow').hidden=!adding;$('category').disabled=!adding;
if(mapping){$('mappingIssuer').value=item.IssuerId||'';$('reason').value=item.Basis||'';}else{const f=adding?{}:JSON.parse(item.FieldsJson);$('category').value=item?.Category||'penalty';$('status').value=item?.Status||'pending';$('eventDate').value=item?.EventDate||'';$('decision').value=f.decision_number||'';$('party').value=f.party||issuerDetails.issuer.LegalName;$('partyType').value=f.party_type||'unknown';$('fine').value=f.fine_total??'';$('complete').checked=!!(f.amounts_complete&&f.amounts_reliable);$('agency').value=f.agency||'';$('grade').value=f.grade||'';$('previous').value=f.previous_grade||'';$('direction').value=f.change_direction||'未知';$('debtor').value=f.debtor||'';$('role').value=f.issuer_role||'unknown';$('actualDefault').checked=f.has_default===true;$('ownAbnormal').checked=f.own_obligation_abnormal===true;$('document').innerHTML=issuerDetails.documents.map(d=>`<option value="${d.DocumentId}">${esc(d.Title)}</option>`).join('');$('reason').value=item?.Notes||'';}
preEdit(mapping||adding?{}:JSON.parse(item.FieldsJson));categoryFields();$('editor').showModal();}
$('form').onsubmit=async e=>{e.preventDefault();$('save').disabled=true;try{const p={operation:mapping?'product_mapping':current.adding?'risk_add':'risk_event',id:mapping?current.item.ProductId:current.adding?$('issuer').value:current.item.RiskEventId,version:current.item?.RowVersion,reviewer:$('reviewer').value,reason:$('reason').value,request_id:crypto.randomUUID()};if(mapping)p.issuer_id=$('mappingIssuer').value||null;else{const f=current.adding?{}:JSON.parse(current.item.FieldsJson);Object.assign(f,{event_date:$('eventDate').value||null,decision_number:$('decision').value,party:$('party').value,party_type:$('partyType').value,fine_total:$('fine').value||null,unit:'元',amounts_complete:$('complete').checked,amounts_reliable:$('complete').checked,agency:$('agency').value,grade:$('grade').value,previous_grade:$('previous').value||null,change_direction:$('direction').value,debtor:$('debtor').value,issuer_role:$('role').value,has_default:$('actualDefault').checked,own_obligation_abnormal:$('ownAbnormal').checked});Object.assign(f,prePayload());if($('quote').value)f.evidence={...(f.evidence||{}),review:{page:Number($('page').value),quote:$('quote').value}};Object.assign(p,{status:$('status').value,event_date:$('eventDate').value||null,fields:f,category:$('category').value,document_id:$('document').value});}const r=await fetch('api.ashx?action=review',{method:'POST',headers:{'Content-Type':'application/json','X-Review-Key':$('key').value},body:JSON.stringify(p)});const d=await r.json();if(!r.ok)throw Error(d.error);$('key').value='';$('editor').close();if(mapping){mappings=await api('mappings');renderMappings();}else await load();}catch(e){$('formError').textContent=e.message;}finally{$('save').disabled=false;}};
$('cancel').onclick=()=>$('editor').close();$('category').onchange=categoryFields;$('add').onclick=()=>{if(issuerDetails&&risk)edit(null,true);};$('mappingSearch').oninput=renderMappings;$('issuer').onchange=()=>{$('indicatorDialog').close();activeIndicator=null;risk=null;issuerDetails=null;$('checkWindow').textContent='';$('sources').innerHTML='';history.replaceState({},'',`risk.html?id=${$('issuer').value}`);load();};
$('closeIndicator').onclick=()=>$('indicatorDialog').close();
document.addEventListener('click',e=>{const trigger=e.target.closest('[data-indicator]');if(trigger)openIndicator(trigger.dataset.indicator);const a=e.target.closest('[data-event]'),b=e.target.closest('[data-mapping]');if(a)edit(risk.events.find(x=>String(x.RiskEventId)===a.dataset.event));if(b)edit(mappings.find(x=>String(x.ProductId)===b.dataset.mapping));});
document.addEventListener('DOMContentLoaded',async()=>{try{issuers=await api('issuers');const options=issuers.map(i=>`<option value="${i.IssuerId}">${esc(i.LegalName)}</option>`).join('');$('issuer').innerHTML=options;$('mappingIssuer').innerHTML='<option value="">未解决</option>'+options;if(params.has('id'))$('issuer').value=params.get('id');$('riskView').hidden=mapping;$('mappingView').hidden=!mapping;$('issuerLabel').hidden=mapping;if(mapping){$('title').textContent='产品发起人映射';mappings=await api('mappings');renderMappings();}else await load();}catch(e){$('error').textContent=e.message;$('error').hidden=false;}});
