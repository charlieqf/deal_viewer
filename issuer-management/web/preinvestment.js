'use strict';
const preKinds=['abs_suspension','servicer_failure','maturity_unpaid'];
const preLabels={abs_suspension:'暂停受理新ABS备案',servicer_failure:'存续期管理失职并启动后备机构',maturity_unpaid:'法定到期日未全部清偿'};
Object.assign(labels,preLabels);
const preFlags={has_event:'已核实实际发生，非预设合同条款',issuer_applicable:'已核实本发起人与事件／产品的适用关系',filing_suspended:'已被公开暂停受理新ABS备案',management_failure:'存续产品已发生管理失职',backup_activated:'已因此启动后备催收／后备服务机构介入',legal_maturity_reached:'已达到合同法定到期日',covered_payment_unpaid:'优先／次级本金或应付优先利息／固定资金成本仍未全部清偿'};
const preRequired={abs_suspension:['filing_suspended'],servicer_failure:['management_failure','backup_activated'],maturity_unpaid:['legal_maturity_reached','covered_payment_unpaid']};
for(const kind of preKinds){const option=document.createElement('option');option.value=kind;option.textContent=preLabels[kind];$('category').append(option);}
const preFields=document.createElement('div');preFields.id='preFields';preFields.hidden=true;
preFields.innerHTML='<label>涉事产品名称<input id="preProduct"></label>'+Object.entries(preFlags).map(([k,v])=>`<label data-pre-flag="${k}"><input type="checkbox" id="pre_${k}"> ${v}</label>`).join('')+'<label>当前有效状态<select id="preEffect"><option value="unknown">待核实</option><option value="active">仍有效</option><option value="resolved">已解除／已纠正</option></select></label><label>状态依据截至日期<input id="preAsOf" type="date"></label><p>状态以原文或人工核实日期为准，不凭暂停期限届满推定恢复。恢复／清偿证据写入下方原文引文。</p>';
$('defaultFields').after(preFields);
function preCategoryFields(){const kind=$('category').value;preFields.hidden=mapping||!preKinds.includes(kind);for(const node of preFields.querySelectorAll('[data-pre-flag]'))node.hidden=!['has_event','issuer_applicable',...(preRequired[kind]||[])].includes(node.dataset.preFlag);}
function preEdit(f){$('preProduct').value=f.product_name||'';for(const key of Object.keys(preFlags))$('pre_'+key).checked=f[key]===true;$('preEffect').value=f.effectiveness||'unknown';$('preAsOf').value=f.effectiveness_as_of||'';preCategoryFields();}
function prePayload(){if(!preKinds.includes($('category').value))return {};const value={product_name:$('preProduct').value,effectiveness:$('preEffect').value,effectiveness_as_of:$('preAsOf').value||null};for(const key of Object.keys(preFlags))value[key]=$('pre_'+key).checked;return value;}
function preEvent(e,f){if(!preKinds.includes(e.Category))return '';const s=e.CurrentState||{label:'当前状态待核实'};return `<p>涉事产品：${esc(f.product_name||'待核清')}；${esc(s.label)}${s.as_of?'（证据截至 '+esc(s.as_of)+'）':''}</p>`;}
function renderPreinvestment(r){
 const s=r.preinvestment_summary;if(!s)return;
 $('preScore').innerHTML=`<h2>投前负面舆情：新增三项</h2><p>建议扣分 <strong>${s.SuggestedDeduction} / 5</strong>；近24个月已确认适用事件计分，三项合计最多扣5分。</p><p>本区不含不良率、拨备覆盖率两项；评价人汇总时沿用负面舆情总扣分上限5分。当前有效状态单独展示，已解除的历史事件保留。</p>`;
 $('preItems').innerHTML='<table><thead><tr><th>核查事项</th><th>当前整理结果</th><th>建议扣分</th></tr></thead><tbody>'+preKinds.map((kind,index)=>{
 const events=r.events.filter(e=>e.Category===kind),pending=events.filter(e=>e.Status==='pending').length;
 const confirmed=events.filter(e=>e.Status==='confirmed'&&e.EventDate>=s.WindowStart&&e.EventDate<=s.BusinessDate).length;
 const sources=r.sources.filter(x=>x.code==='chinamoney_'+kind||(kind==='abs_suspension'&&x.code==='amac_abs_suspension'));
 const unready=sources.some(x=>!x.latest||x.latest.Status!=='success');
 const backlog=sources.reduce((n,x)=>Math.max(n,x.latest?.PendingDocuments||0),0);
 const text=confirmed?`已确认 ${confirmed} 项`:pending?`待确认 ${pending} 项`:unready?'来源尚未完成查询':backlog?'材料待解析':'本次已查渠道未检出相关事件';
 const deduction=[s.SuspensionDeduction,s.ServicerDeduction,s.MaturityDeduction][index];
 return `<tr><td>${esc(preLabels[kind])}</td><td>${esc(text)}${pending?`<small>待确认 ${pending} 项</small>`:''}${backlog?`<small>本类别待解析 ${backlog} 份</small>`:''}${unready?'<small>存在未接入或查询失败来源，请查看来源表</small>':''}</td><td>${deduction}</td></tr>`;
 }).join('')+'</tbody></table>';
}
