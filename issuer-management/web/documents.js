'use strict';
const $=id=>document.getElementById(id);
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let page=1,controller=null;
function params(){const q=new URLSearchParams(new FormData($('filters')));q.set('page',page);return q;}
function options(el,items,value){el.replaceChildren(new Option(el.options[0].text,''),...items.map(([text,id])=>new Option(text,id)));el.value=value;}
async function load(){
  if(controller)controller.abort();const current=controller=new AbortController();
  $('error').hidden=true;$('summary').textContent='正在读取文档…';$('prev').disabled=$('next').disabled=true;$('rows').replaceChildren();
  try{
    const q=params(),response=await fetch('api.ashx?action=library&'+q,{signal:current.signal});
    if(!response.ok)throw Error('文档读取失败，请稍后重试。');const data=await response.json();
    if(controller!==current)return;
    options($('issuer'),data.issuers.map(i=>[i.LegalName+'（'+i.DocumentCount+'）',i.IssuerId]),q.get('issuer'));
    options($('year'),data.years.map(y=>[y+' 年',String(y)]),q.get('year'));
    page=data.page;const pages=Math.max(1,Math.ceil(data.total/data.page_size));
    $('summary').textContent='找到 '+data.total+' 份文档';$('pageInfo').textContent=page+' / '+pages+' 页';
    $('prev').disabled=page<=1;$('next').disabled=page>=pages;
    $('rows').innerHTML=data.documents.map(d=>{
      const rating=d.DocumentType==='issuer_rating',url='api.ashx?action=document&amp;download=1&amp;id='+encodeURIComponent(d.DocumentId);
      return `<tr data-document-id="${esc(d.DocumentId)}"><td><a class="doc-title" href="${url}" download="${esc(d.Title)}">${esc(d.Title)}</a><a class="issuer-name" href="./#issuer=${encodeURIComponent(d.IssuerId)}">${esc(d.LegalName)}</a>${d.MonitoringStatus==='paused'?'<span class="paused">暂停采用 · 保留历史</span>':''}</td><td><span class="tag ${rating?'rating':''}">${rating?'主体评级':'财报及相关披露'}</span></td><td class="date">${rating?'—':esc(d.MainPeriodEnd?.slice(0,10)||'待确认')}</td><td class="date">${esc(d.PublishedDate?.slice(0,10)||'待补充')}</td><td class="file-info">PDF · ${esc(d.PageCount??'—')} 页<small>${(d.ByteSize/1048576).toFixed(2)} MB</small></td><td><a class="download" href="${url}" download="${esc(d.Title)}" aria-label="下载 ${esc(d.Title)}">下载 ↓</a></td></tr>`;
    }).join('')||'<tr><td colspan="6" class="empty">没有符合条件的文档，请调整筛选条件。</td></tr>';
    const state=params();for(const [k,v] of [...state])if(!v||(k==='page'&&v==='1'))state.delete(k);
    history.replaceState(null,'',location.pathname+(state.size?'?'+state:''));
  }catch(e){if(e.name!=='AbortError'){$('error').textContent=e.message;$('error').hidden=false;$('summary').textContent='未能读取文档';}}
}
const initial=new URLSearchParams(location.search);
for(const name of ['q','type','issuer','year']){const el=document.querySelector(`[name="${name}"]`),v=initial.get(name)||'';if(el.tagName==='SELECT'&&v&&![...el.options].some(o=>o.value===v))el.add(new Option(v,v));el.value=v;}
page=/^[1-9]\d{0,5}$/.test(initial.get('page')||'')?Number(initial.get('page')):1;
$('filters').addEventListener('submit',e=>{e.preventDefault();page=1;load();});
for(const id of ['issuer','type','year'])$(id).addEventListener('change',()=>{page=1;load();});
$('reset').addEventListener('click',()=>{for(const name of ['q','type','issuer','year'])document.querySelector(`[name="${name}"]`).value='';page=1;load();});
$('prev').addEventListener('click',()=>{page--;load();});
$('next').addEventListener('click',()=>{page++;load();});
load();
