"""Use the existing dedicated browser fixture, never production writes."""
import argparse,json
from pathlib import Path
from playwright.sync_api import sync_playwright,expect

p=argparse.ArgumentParser();p.add_argument('--base',required=True);p.add_argument('--config',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
a=p.parse_args();cfg=json.loads(a.config.read_text());base=a.base.rstrip('/')+'/'
errors=[]
with sync_playwright() as pw:
    browser=pw.chromium.launch(headless=True);page=browser.new_page(viewport={'width':1440,'height':1100})
    page.on('pageerror',lambda e:errors.append(str(e)))
    page.goto(base+'risk.html?id='+str(cfg['issuer_id']))
    expect(page.locator('#preItems')).to_contain_text('尚未完成查询',timeout=45000)
    assert page.locator('#preItems').inner_text().count('尚未完成查询')==3
    page.locator('#add').click();page.locator('#category').select_option('servicer_failure')
    expect(page.locator('#preFields')).to_be_visible()
    page.locator('#preProduct').fill('浏览器隔离验收ABS')
    page.locator('#eventDate').fill('2026-11-06');page.locator('#quote').fill('Synthetic isolated browser acceptance evidence.')
    page.locator('#reason').fill('测试材料补录，尚未确认');page.locator('#reviewer').fill('隔离库浏览器验收');page.locator('#key').fill(cfg['review_key']);page.locator('#save').click()
    expect(page.locator('#editor')).not_to_be_visible(timeout=45000)
    expect(page.locator('tr[data-category="servicer_failure"]')).to_have_attribute('data-status','pending_review')
    page.locator('[data-indicator="pre_b"]').click()
    card=page.locator('#events .event').filter(has_text='存续期管理失职并启动后备机构').last
    card.locator('button').click();page.locator('#status').select_option('confirmed')
    page.locator('#pre_has_event').check();page.locator('#pre_issuer_applicable').check();page.locator('#pre_management_failure').check()
    page.locator('#reason').fill('只确认失职，缺少后备启动条件');page.locator('#reviewer').fill('隔离库浏览器验收');page.locator('#key').fill(cfg['review_key']);page.locator('#save').click()
    expect(page.locator('#formError')).to_contain_text('全部触发条件',timeout=45000)
    page.locator('#pre_backup_activated').check();page.locator('#reason').fill('两条件均确认，保留原文')
    page.locator('#save').click();expect(page.locator('#editor')).not_to_be_visible(timeout=45000)
    expect(page.locator('tr[data-category="servicer_failure"]')).to_have_attribute('data-status','confirmed',timeout=45000)
    expect(page.locator('#score, #preScore')).to_have_count(0)
    card=page.locator('#events .event').filter(has_text='存续期管理失职并启动后备机构').last
    card.locator('button').click();page.locator('#preEffect').select_option('resolved');page.locator('#preAsOf').fill('2026-11-06')
    page.locator('#quote').fill('Synthetic evidence: servicing remedied on 2026-11-06.')
    page.locator('#reason').fill('更新已纠正状态，历史事件仍在24个月窗口');page.locator('#reviewer').fill('隔离库浏览器验收');page.locator('#key').fill(cfg['review_key']);page.locator('#save').click()
    expect(page.locator('#editor')).not_to_be_visible(timeout=45000)
    expect(page.locator('#events')).to_contain_text('已解除／已纠正')
    a.out.mkdir(parents=True,exist_ok=True);page.screenshot(path=str(a.out/'preinvestment-desktop.png'),full_page=True)
    page.set_viewport_size({'width':390,'height':844});page.screenshot(path=str(a.out/'preinvestment-mobile.png'),full_page=True)
    assert not errors,errors
    browser.close()
result={'passed':True,'checks':['three_unknown_not_no','candidate_pending','joint_condition_required','confirmed_status','no_score_display','dated_resolution_display'],'page_errors':errors}
(a.out/'preinvestment-browser-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(result))
