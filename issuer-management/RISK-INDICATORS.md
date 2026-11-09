# 风险页投前、投后指标（2026-11-09）

风险页取消建议分、建议扣分及总分说明，按投前 a/b/c、投后 a/b/c 顺序展示用户确认的六条完整指标。每条“查看明细”打开对应事件和来源状态弹窗，事件卡片及核对表单明确标注所属条目；原文、历史事件和已排除记录继续可查。

## 结构化接入

主体 `api.ashx?action=risk&id=<IssuerId>` 和产品 `api.ashx?action=product-risk&id=<ProductId>` 共用同一风险结果。后者从响应的 `risk` 字段读取；未解决发起人映射时仍为 `risk=null`。

新增 `indicators` 数组，顺序固定如下，调用方直接读取，无需抓取页面或交给 AI 重新识别条目和计算分数。

| code | stage | category | 指标 |
| --- | --- | --- | --- |
| pre_a | pre | abs_suspension | 公开信息披露暂停该主体备案新ABS项目的 |
| pre_b | pre | servicer_failure | 已发行未结清产品存在存续期管理失职，启动后备催收机构介入情形的 |
| pre_c | pre | maturity_unpaid | 存续产品出现到达合同约定的法定到期日仍未全部清偿优先及次级档本金或优先级发行利率、固定资金成本的 |
| post_a | post | penalty | 近两年发起人被监管机构处罚，且单次罚款金额超过1亿元 |
| post_b | post | rating | 发起人评级下调 |
| post_c | post | default | 发起人在债券等公开市场发生违约情形 |

每项含 `stage_label`、`letter`、`order`、完整 `label`、`status/status_label`、`requires_review`、`confirmed_events`、`pending_events`、`historical_events`、`excluded_events`、`pending_documents`、`event_ids/event_count`、`window_start/business_date`、`query_end` 和本类别 `sources`。`event_ids` 包含历史及排除记录，对应响应的 `events[].RiskEventId`。事件同时附带 `IndicatorCode`、`IndicatorStage`、`IndicatorLabel`；证据继续来自原有 `EvidenceJson`。

未查询、未接入、查询失败、材料待解析不会变成“未检出”；只有具体待核对事件产生 `requires_review=true`。状态沿用既有近24个自然月范围，历史事件另行计数、保留查阅。国际评级为人工补录，不冒充定期来源。

新指标对象不返回分数。旧 `summary`、`preinvestment_summary`、`preinvestment_items` 保留兼容，未修改已保存评分、既有判定规则、采集流程及数据库结构。

## 验证及发布

`python -m unittest test_risk_indicators test_preinvestment_status test_preinvestment test_risk` 验证状态隔离、日期边界、六类映射和旧接口兼容。

`test_risk_indicators_browser.py --base <候选或线上地址> --out <验收目录>` 先核验真实接口与六条明细，再用浏览器拦截响应覆盖事件分类、原文链接、核对刷新、排除与历史事件、失败及积压。所有核对写请求均被拦截，不写真实数据库。`--local` 仅供发布前本地页面预览，不可用作上线凭据。

仅发布 Web：从 main 精确提交导出源码，运行 `deployment/release-risk-indicators.sh <commit> prepare`；候选 18883 浏览器验收通过后，将 `acceptance.json` 放到对应服务器备份目录的 `candidate-acceptance.json`，运行 `activate`。保留原容器，健康失败自动回退。采集镜像及宿主采集源码不切换，无迁移和定时任务变动；网页版本见 `/health` 和宿主 `web-release.json`。
