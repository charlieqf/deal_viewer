# 发行主体资料管理

## 风险信息 V2.3（2026-11-02）

风险页入口 `risk.html?id=<IssuerId>`，产品映射入口 `risk.html?mapping=1`。只提供证据、来源状态和建议分，投后实际分数仍由原系统保存。三张新增表为 `ProductIssuerMapping`、`RiskEvent`、`RiskCheckLog`；建议分读取 `DV.vw_IssuerRiskSummary` 当前已确认事件，使用北京时间近24个自然月，不保存评分快照。

周二北京时间04:00运行风险采集，独立 `risk/acquisition.sqlite` 与每批JSON参数；月任务显式只采财报。每日处理先续跑财报，再处理风险。共用站点文件锁、暂停标记、PDF下载10秒及翻页1.5秒节流；原八列searches保持不变。来源列表按批归档，新增主体本地回放；日志导入与查询成功分开记录，解析积压单列。

`production_migration_008_risk.sql` 放宽评级固定年份限制，增加前次等级及调整方向；`009` 将原产品关联视图改为按已保存的ID映射。自动映射使用NFKC与trim，保留手工结果。已知丰耀32641角色错误依据原文修正，其他未解决项目保持空映射。实际评价项目名单尚未取得，未自动把全库候选主体当成客户评价名单。

同决定书同法人可靠罚款合计≤1亿元，或可靠罚没上界≤1亿元，自动折叠为未达阈值。代发须有实际债务人、本主体角色及引文证据；本主体义务异常线索仍待确认。下载前执行五词标题/附件名筛选；普通兑付公告不下载。下载后核实无事件只归档，不新增事件状态。人工更正使用已有复核口令并写入 `IssuerChangeLog`，保留原始抽取和原件。

其他来源/国际评级可通过 `python -m acquisition.manual_risk --issuer-id … --file … --source-url … --title …` 先归档PDF，再在风险页选择该原件补录、核对；国际等级按原文文本保存。该操作不冒充定期来源检查。企查查尚未开通，未做付费调用或宣称试用通过；缺少应用凭据、接口权益以及历史、分页、许可和费用样本验证。

部署使用 `deployment/release-risk.sh <完整main提交>`；两镜像与宿主源码都记录同一提交。隔离验收使用授权实例上的 `DealViewerRiskTest_20261102`，专用账号无生产库访问权。测试命令及边界见 [V2.3验收说明](deployment/RISK-V23.md)。

## 不良 ABS 产品关联（2026-10-27，页面 v20）

按业务确认的条件，读取 `PortfolioManagement.DV.tbl_view_Products` 中“银行间债券市场／信贷资产证券化／不良资产重组”产品，通过 `ProductSubject=DV.Issuer.LegalName` 精确关联。新增只读视图 `DV.vw_IssuerNplProduct`；仅向网页读账号授予该视图 SELECT 权限，仍不允许直接读取或修改原产品表。不复制产品记录，不新增映射维护流程。

概览显示累计项目数，包含已清算产品，按 TrustId 去重；点击数字打开产品名称、ID、发行主体、发行日期及状态明细，可筛选存续／已清算。缺失发行日期显示待补充，不推断日期。暂停采用主体的产品历史仍可查询，与财务数据采用状态分开。范围不沿用财报的 2024 年起限制。

迁移：`production_migration_006_products.sql`；发布：`deployment/release-products-v20.sh`。v19 容器保留为回退，采集与财报复核流程保持原有配置。首次核验 26 家主体、863 个不同产品全部匹配，包括本次补齐的 35325、35326、35327。新 API `action=products&id=<IssuerId>` 返回只读产品明细；overview/issuers 增加 NplProductCount。

浏览器验收脚本：`test_product_links_browser.py`，通过实际 Chrome 验证全部主体数量与明细、三项修复产品、产品状态筛选、表头固定、移动端横向滚动、暂停主体历史、财务证据入口及页面脚本错误。截图及验收记录保存在工作区 `tmp/issuer-ratings/ui-v20/`。候选环境另验证旧 HTML 自动恢复及请求失败后重试。

## 网站自动采集与评级（2026-10-26）

2026-10-27 批量运行修正：首轮检索触发六小时上限，不能将单份材料验收视为全量完成。后台现按本月检查点跳过已完成查询和已下载文件，优先未检索主体；同一轮失败附件不随关键词重复下载。超时运行明确标记 interrupted，采集服务失败后间隔六小时断点续跑。评级队列优先处理；募集资金使用、存续期认证材料不作为主体财报处理，无法确定报告期的材料进入元数据核对。无有效财务候选时不新增财务展示版本。评级允许从单一 JSON 代码块后的普通说明中读取原始 JSON，但拒绝第二个 JSON 对象；仍逐项验证证据。新增回归后共 76 项测试通过。

后续恢复版本 `20261027-v2` 已在服务器通过全部 78 项测试。附件 HTTP 403 立即归类，不再等待下载超时；附件失败不重复整段查询。仅在确认按日期倒序且整页日期早于 2024 年时停止翻页。采集和处理使用独立锁，可并行运行。`python3 -m acquisition.report --root <队列目录> --output <输出目录>` 只读导出进度及稳定来源失败清单。`issuers_search_started` 仅表示已开始搜索；文件 `pending_review` 数量不等于有效评级条数，评级条数须查询正式库候选表。

`20261027-v3` 下载修正：403 不能直接归因为服务器 IP 封禁。对同一失败附件，复用旧 ABN 采集的公开 `fileDownLoad.do?mode=open&contentId=…&priority=…` 接口，以独立 HTTP 会话下载成功；另抽查五份失败附件也返回 PDF。旧代理测试连接超时，未启用为生产路径。下载现在优先使用公开接口，网络或 HTTP 失败才回退浏览器；保留附件 priority，禁止重定向，验证 PDF 文件头并限制流式读取体积。新增测试后本地共 80 项通过。

`20261027-v4`：v3 批量运行后仍发生持续 403，故不能以少量直连成功判定恢复。同一时刻三份主体/ABN 附件在 R760 直连为 403、操作员电脑为 200；旧 ProxyJet 连普通网页也超时。已有 Webshare 列表的十个节点均通过 ABN PDF 文件头探针，三份完整 PDF 通过解析及 SHA-256 校验，代理浏览器检索请求正常。购买前仅做小流量验证；用户完成付款后才启用正式补采。

v4 已部署并通过服务器 82 项测试。代理使用独立私有文件 `/data/dealviewer-issuer-production/private/acquisition-network.json`，示例见 `deployment/acquisition-network.example.json`；文件仅挂载到采集容器，HTTP 与 Chromium 使用同一配置，不读取系统代理，也不修改其他项目代理绑定。实际账号须从私有渠道配置，不写入源码。启用前先验证套餐额度、实际目标附件和浏览器检索。请求至少间隔十秒；连续五次 403/407/429 后保留检查点并结束该批，等待已有六小时重试机制，避免持续无效请求。未经启用的配置不影响默认直连。

Webshare 扩大验证：同一节点快速请求 20 份附件仅 10 份成功；固定另一节点、间隔 10 秒后，同一组 20 份全部返回 PDF，包含快速测试失败的文件。另有三份完整 PDF 解析通过。此结果支持低频代理下载方案，不代表首次全量采集完成或长期可用性保证。

付费启用：用户已购买 100 节点、250 GB 套餐，正式补采第 6 轮于 2026-10-27 00:19 UTC 启动。主体采集与旧 ABN 使用不同固定节点及私有配置；旧 ABN 已发布 `dealviewer-crawler:r760-20261027-abn-private-proxy`，13 项预检和实际 PDF 下载通过，详见 `../deployment/r760-crawler/RUN-2026-10-27.md`。套餐升级使旧免费节点返回 407，用户授权同步修复；MedEvidence R760、CN 和 StressTest 的代理配置已迁移，R760 与 CN 的实际非缓存问答成功。历史登录失效状态保持原状。

`20261027-v5` 允许读取单一完整 JSON 对象后附带的普通说明，与已有代码块响应处理保持一致；拒绝第二个对象、数组或代码块，不修改提取值，后续证据校验不变。已保存响应可恢复，避免重复模型调用。共 83 项回归测试通过。首轮历史补采和人工业务复核仍独立进行，不能把代理恢复等同于全部材料已入库。

单份来源异常：浦发银行 2024 年半年度财务报告（中国货币网 contentId 2951537）在付费代理和工作站直连取得相同 SHA-256、22,386,684 字节的文件，但内部压缩流错误、缺少结束标记；仍拒绝作为有效 PDF 归档，不放宽校验，不使用其他网站文件替代。该异常与网络 403 分开记录。

新增 `acquisition/` 后台模块，使用 Python + Playwright 操作中国货币网正常检索和下载入口。Crawl4AI、Firecrawl 未引入。此前的财报初始化仍属于人工文件导入；本模块补充真正的网站发现、下载和周期处理。

- 从正式库刷新主体与已确认别名；暂停采用主体跳过。财报按年度、半年度、季度、财务和信息披露公告检索；评级按主体评级及评级检索。处理范围从 2024 年起，不自动改用其他网站。
- 检索聚合响应和结果分页分别识别；分页截断、网络失败和无结果分开记录。详情页按网站自己的下载流程取得 PDF，不保存临时签名或浏览器会话。单个损坏附件不阻断后续报告。
- `acquisition.sqlite` 保存检索、文件版本、失败和处理队列；原件按 SHA-256 去重。相同网址内容改变保留新版本。已有正式文件只关联中国货币网来源，不重复模型提取。
- 浏览器容器不挂载数据库或模型密钥。宿主机核验并归档文件，处理容器只读正式原件。财报复用已有 OCR、Gateway、校验及草稿流程；增量选值保留已有指标和人工核对结果。
- 评级提示词在 `acquisition/rating.py`。只接受目标法人主体评级，排除债项、ABS 分层、担保人、子公司、短期、个体实力和政府支持评级。评级日期与上传日期分开，支持中文日期，逐项校验连续原文及物理页。机构、评级日期、等级、展望、评级事件及证据进入新增 `DV.IssuerRatingCandidate`，全部为 `pending`，不自动业务核准。英文主体别名未确认或证据不足的材料不会强制入库。
- 页面 v19 增加按机构展示的评级候选、历史记录、证据和评级文档筛选。初期仍须人工核对；当前评级页面提供证据查看，不包含业务发布或评级审批功能。

部署目录：`/data/dealviewer-issuer-production/acquisition-src`（源码）、`acquisition`（队列、运行产物）、`archive`（正式原件）。月度检索任务为 `dealviewer-issuer-acquisition.timer`，北京时间每月 1 日 02:00，加最多 15 分钟随机延迟；`dealviewer-issuer-processing.timer` 每日北京时间 10:00 续跑已下载任务，不重新搜索网站。每批最多处理 10 份新材料，模型调用沿用现有 SQL 配额和响应缓存。

运维入口：

```bash
# 手动触发完整周期；与定时任务使用同一锁及队列
systemctl start dealviewer-issuer-acquisition.service
# 只处理已有队列
cd /data/dealviewer-issuer-production/acquisition-src
python3 -m acquisition.scheduled --skip-collect --limit-jobs 10
# 查看任务和下次运行时间
systemctl status dealviewer-issuer-acquisition.service dealviewer-issuer-processing.service
systemctl list-timers 'dealviewer-issuer-*' --all
```

验证包含本机和服务器 Python 3.10 容器的 74 项测试、真实网页分页与附件下载、现有财报文件复用，以及浦发银行 2026 年大公主体评级的 OCR → 模型 → 原文校验 → SQL → 页面闭环。完整重跑的 SQL 表计数相同、额外模型调用为零、原财务指标和记录保持一致。服务器验收产物位于 `acquisition/verification/`，浏览器证据位于工作区 `tmp/issuer-ratings/ui-v19/`。这些结果验证管线，不代表所有主体已完成首次全量采集。

原财务管线和旧任务不通过本模块重新初始化；新增评级迁移为 `production_migration_005_rating.sql`。发布脚本为 `deployment/release-rating-v19.sh`，定时任务安装脚本为 `deployment/enable-acquisition.sh`。v18 容器作为回退保留；回退页面不删除新增评级或原件。

## 正式接入（2026-10-20）

用户授权网页先部署在 R760，数据库接入正式 `PortfolioManagement.DV`。入口：https://r760.instmarket.com.au:1443/dealviewer/issuers/ 。部署状态和逐主体结果见工作区 `年报/设计文档/正式接入20261020`，部署细节见 `deployment/README.md`。下方 V1/V2 章节是历史试点说明，不代表当前正式库状态。

- 原件按 SHA-256 独立归档，298 个来源对应 296 个唯一 PDF；来源记录与文件内容分别保留。
- `production_run_issuer.sh` 要求上一主体已有真实浏览器验收记录，才启动下一主体。原件上传和 OCR 准备可提前进行，不写正式 SQL。
- `production_ocr_quality.py` 对低质量疑似横置扫描分别尝试方向校正，保留原 OCR、旋转方向、页面和框坐标。定向高分辨率补识别使用独立 `ocr-refined` 目录。
- 模型只接收有用的证据页；正式账户最多四路并发，请求起始间隔至少 4.2 秒。失败响应原样保留；明确输出上限错误不重复同一输入。已保存的 JSON 仅允许去除完整对象后冗余的闭括号，不修补数值、不接受第二个对象。
- 已通过的候选和历史草稿追加保留；不充分的原始数值、单位、口径或准则依据不会强制填补。所有当前入库选择均待业务复核。
- 浏览查询使用独立只读 SQL 账户。原件通过 DocumentId 下载；证据链接直接打开浏览器原生 PDF 阅读器并定位页码，列表不再提供逐页图片预览。
- v16 增加轻量人工核对：共享复核口令、核对人、数值/单位、文件页码和核对说明。`issuer_review.py` 追加观察值、证据、展示版本和审计记录；保留原件与旧版本，校验主体/报告期/页码、乐观并发和请求幂等性。独立写入账户只允许所需表追加及序列版本号更新，不允许发布、覆盖原始观察值或访问其他业务表。
- 用户于 2026-10-20 确认：不能明确集团/合并范围时按本行处理，并保留规则判定标签；集团候选仍优先。后续导入将未知范围规范为本行，重新选值保留当前人工核对结果。暂停采用主体禁止通过核对入口补录。

v16 已在正式地址验证桌面/窄屏固定名称与表头、水平列对齐、原文下载、趋势实线/参考虚线、CSV 原始金额文本格式及人工核对闭环。渤海银行三项净利润通过浏览器表单实际入库；另 103 项按已确认的口径规则处理。核对功能沿用现有数据表，无业务表结构迁移。部署脚本：`deployment/release-feedback-v16.sh`；证据：工作区 `tmp/issuer-production/ui-v16/`。共享口令仅保存在服务器私有配置和操作员私有交接文件，禁止放入源码、截图、审计说明或日志。

v17 修正核对入口：待核对项直接显示“核对并录入”，被限制项展示原因。AMC 的财务金额允许人工核对，必须明确集团/母公司口径并确认指标定义一致；银行监管指标不通过该入口录入，暂停采用主体仍禁止补录。表单提供三步提示，修复母公司选项及窄屏下拉框高度。原有银行默认本行规则不变。验证采用真实浏览器和数据库事务回滚，不保存测试数值。部署脚本：`deployment/release-review-v17.sh`；证据：`tmp/issuer-production/ui-v17/`。

v18 按用户要求将录入集中到历史财务指标明细。主体概览、顶部指标卡片和趋势图只提供证据查看；仅从明细数值打开的证据弹窗保留“核对 / 更正”。明细缺项仍提供“核对并录入”。本次仅更新前端，不修改后台或财务数据。部署脚本：`deployment/release-review-location-v18.sh`；浏览器证据：`tmp/issuer-production/ui-v18/`。

回归命令：`python -m unittest discover -s deal_viewer/issuer-management -p "test_*.py"`。

## V2：浦发重跑与中信验证

V2 沿用既有 15 张表，不修改已执行的初始 SQL。`run-config.json` 明确主体、简称、运行标识和处理版本；两家主体分别使用独立产物目录，仍写入同一隔离试点库。新的候选和草稿追加保存，旧版结果保留。

流程入口为 `pilot_v2_batch.py`：等待页面覆盖完成 → 准备证据请求 → 登记原件和请求 → 调用 Gateway → 校验并导入候选 → 选择本次运行的草稿 → 重放并核对全部表计数。完成后仍需业务复核，不自动发布。

- `prepare_pilot.py` 检查每页文字、图像面积及乱码，生成 OCR 页清单；扫描表格不能因 PDF 其他页有文字而跳过。
- `prepare_v2_ocr.py` 生成带原始页映射的 PDF 切片；`pilot_ocr_v2.py` 使用 200 DPI 灰度渲染，逐页缓存包含文件哈希、页号、模型及预处理版本。可用最多四个独立进程分片，单进程仍限制为两核。
- `pilot_v2_prepare.py` 检索财务报表、指标和准则声明；每份最多 48 页送给 LLM，并记录被截断的候选页。覆盖不足会中止当前批次，不会把未处理页当成未披露。
- `pilot_v2_validation.py` 核对原始数字、指标行、单位、口径、准则引用；明确的四列表检查集团/本行当期列。其他布局仍须复核，不能声称已完成通用表格结构识别。
- `pilot_v2_context_resolve.py` 用 OCR 框高度重建轻微倾斜表格的行，并在相邻同表续页中寻找明确单位声明。规则处理单独登记 `context_resolver` Attempt，保留原模型 Attempt 和数字，不冒充新的模型调用。
- `pilot_v2_repair.py` 对校验失败项做一次有上限的补充提取；`pilot_v2_focus.py` 对仍缺失的展示指标扩展全文证据检索。已通过的响应、旧校验包、旧候选不会因规则调整而被覆盖。
- `pilot_v2_drafts.py` 仅从当前主体、当前根任务选值。资本充足率使用监管并表，金额使用会计合并范围；不同口径不互相填补。按序列锁分配版本，选择结果相同则复用草稿。
- `pilot_transfer.py` 使用有界批量传输、断点续传和最终 SHA-256 校验；不再以文件存在或长度相同作为传输成功依据。

调用前的 SQL 预算登记采用事务级应用锁，每个根任务最多 36 次尝试；试点每日最多 400 次，正式管线按《实施方案与里程碑》第 4 节最多 450 次，重试同样计入。应用按 UTC 日统计，Gateway 自身仍是账户总配额的权威；此处未调整 Gateway 的 500 次账户日限额或 token 限额。达到应用上限须等待下一配额日，未知管线仍采用 400 次较低上限。

无法解析的 PDF 不得用虚构页数通过数据库约束。`input/source-quarantine.json` 必须记录同名同哈希文件的实际字节数、PDF 声明长度和至少两个独立解析器的失败证据，确认文件被截断且原件仍保留，才允许跳过该文件并处理其他有效材料。完整来源清单及批次幂等键保持不变；隔离项不生成模型请求和文档记录。

恒丰银行 2025 年年度报告原件仅有 18,898,648 字节，PDF 声明 30,602,600 字节，已保留归档并记录隔离。其余 13 份有效材料沿用原登记根任务恢复处理。恢复前的运行源码及清单保留在 `runtime-src-before-quarantine` 和 `runtime-source-manifest.before-quarantine.json`；`production_verify_quarantine.py` 可复核原件哈希、有效文件集合与根任务唯一性。以后取得完整材料时，应按新文件哈希正常登记，保留本次异常历史。

本机回归：`python -m unittest discover -s deal_viewer/issuer-management -p "test_pilot*.py"`。
R760 批处理：设置 `ISSUER_PILOT_ROOT` 为对应运行目录后执行 `python pilot_v2_batch.py`。密钥只通过私有挂载读取。

同一运行目录用于重放相同输入；改变提示词、选页或校验策略时应使用新的 `run_id` 和运行目录。历史 V1 的固定主体脚本仅用于追溯，新增主体走 V2。`pilot_v2_final_check.py` 会对两家主体重放入库，核对 15 张表的计数和旧版浦发选择。

以下保留 V1 试点说明，最新结果以 V2 试运行报告为准。

本目录是 2026-10-20 的真实材料试点代码。使用已下载的浦发 12 份 PDF、R760 PaddleOCR 和专用 Gateway `goldencode` 账户，在 **DealViewerIssuerPilot_20261020** 保存原件登记、候选、证据及待复核草稿。完整设计与实际结果在工作区 `年报/设计文档`。

## 边界

- `guarded_connection()` 核对 SQL Server 实例、地址、版本以及试点数据库所有权标记；不对现有 DealViewer 业务表写入。
- 15 张表是设计验证版本。正式发布权限、逐指标策略、并发预算、撤回与恢复流程仍需补齐，不能直接作为生产迁移包。
- 原 PDF 当前位于操作员本机，数据库明确记录 `operator_local / registered_local`，归档时间为空。数据登记与服务器归档是不同状态。
- AI 结果必须经过候选校验和业务复核。试点的 10 期展示均为草稿，程序不自动发布。
- SQL 初始迁移执行后保持哈希不变，后续字段通过 002、003 增量迁移加入。

## 使用到的处理链

| 阶段 | 文件 |
| --- | --- |
| PDF 哈希、逐文件隔离解析、原生文字 | `prepare_pilot.py` |
| 建库保护、DDL、原件与来源登记 | `pilot_runtime.py`、`pilot_schema.sql`、`pilot_migration_*.sql` |
| 提示词、选页、Decimal 与证据校验 | `pilot_extract.py` |
| 本机准备真实请求及输入页清单 | `pilot_local.py prepare` |
| 调用前登记任务、尝试和预留 | `pilot_register_plan.py` |
| R760 通过现有 Gateway 回环地址调用 | `pilot_retry_remote.py`；新请求使用 `--fresh` |
| 使用已有 PaddleOCR 镜像处理指定图片 | `pilot_ocr_images.py` |
| 原始响应验证及候选包 | `pilot_build_bundle.py` |
| 校验租约、幂等写入候选和证据 | `pilot_import_bundle.py` |
| 自动初选草稿及 SQL 精确读回 | `pilot_create_drafts.py` |
| 真实 SQL 事务回滚检查 | `pilot_sql_checks.py` |
| HTML、CSV 及本机原文访问 | `pilot_preview.py`、`serve_pilot_preview.py` |

`ISSUER_PILOT_ROOT` 指向阶段产物目录，R760 默认为 `/pilot`。模型和数据库凭证均从环境外的私有文件挂载读取，不能放进源码或产物。模型调用前必须登记 Attempt，不能直接重放失败 HTTP 请求而不登记新的尝试。

`pilot_local.py invoke` 是已验证但受本机网络影响的备用通路；本次转为 R760 调用。`upload_pilot.py` 的大文件传输和 `pilot_ocr.py` 的全文件 OCR 路径没有作为本次成功路径验收。小产物使用 `upload_small_parts.py` / `download_small_artifacts.py`，参数传入运维入口，不在仓库固定公网运维地址。

## 本机验证与展示

```powershell
python -m unittest discover -s deal_viewer\issuer-management -p test_pilot_validation.py -v
python deal_viewer\issuer-management\pilot_preview.py --input tmp\issuer-pilot-spdb\artifacts\sql-readback.json --output 年报\设计文档\浦发试点
python deal_viewer\issuer-management\serve_pilot_preview.py --directory 年报\设计文档\浦发试点
```

最后一条命令仅监听 `127.0.0.1:18749`，只提供试点页面和数据库已登记的 PDF。独立 HTML 可直接打开；HTTP 预览把原件链接映射为本机 PDF 路由。它是试点核对入口，不包含用户登录、在线复核写回或正式 DealViewer 菜单。

## 数据质量与重跑

模型原始输出、输入哈希、每次请求状态和 token 用量分别保存。未知用量保持 unknown，不以 0 替代。金额使用 Decimal 后写入 `decimal(28,6)`，API/导出中的 bigint 标识与金额保留字符串；百分数 172.84 表示 172.84%，不会再次乘以 100。

同一 Attempt 与候选序号只写入一次；快照初选也可重放。自动草稿策略以原生文字和主报告优先，保留其他来源、单体与 OCR 候选；它不自动解决同口径冲突。未知会计准则、比例字段的会计基础异常和 OCR 来源会进入 ReviewNote。对暂缺值使用 unresolved，绝不由“选定页未找到”推断整份报告未披露。

真实 SQL 检查对临时身份、草稿修改、发布、重放与冻结使用回滚事务；结果保存在 `SQL检查结果.json`。这些子项不替代设计文档 D01–D16 的全部验收要求。
