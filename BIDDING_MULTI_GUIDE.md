# 多站点招投标爬虫 (bidding_multi_spider.py)

在 `ccgp_keyword_spider.py` 抽取能力的基础上，把抓取流程抽象为 **"引擎 + 站点插件"** 两层。想接入一个新网站，只需要实现一个继承 `BaseSite` 的类即可，不用重写限流 / 去重 / 抽取 / LLM 兜底。

## 已接入（阶段 1，纯 HTML，无需登录）

| 代号 | 站点 | 入口 |
|---|---|---|
| `ccgp` | 中国政府采购网 | https://search.ccgp.gov.cn/bxsearch |
| `ustc_zhc` | 中科大资产与后勤处 | https://zhc.ustc.edu.cn/10843/list.htm |
| `ipp` | 等离子体物理研究所采购平台 | http://www.ipp.ac.cn/ztbxx/zbxx/ |
| `ihep` | 中科院高能所通知公告 | http://www.ihep.ac.cn/xwdt2022/tzgg_1/ |
| `caep` | 绵阳 CAEP 工物院招投标信息网 | https://ztbxx.caep.ac.cn/ |

## 尚未接入（阶段 2，需要 cookie / 登录 / JS 渲染）

| 站点 | 阻碍 |
|---|---|
| 青岛大学采购中心 (`zhaobiao.qdu.edu.cn`) | 域名已重定向到后勤处主页，招标子系统疑似下线 |
| 上海科大 (`old.shanghaitech.edu.cn`) | 域名不解析，已迁移到新入口，需要提供新 URL |
| 华东师大采购信息网 (`zcb.ecnu.edu.cn`) | 全站跳转到 SSO 登录，需要统一身份认证 cookie |
| 广东教育部门零散采购 (`gdedulscg.cn`) | ASP.NET + ViewState，详情需账号 |
| 机电产品招标 (`chinabidding.com`) | 详情页需注册登录 |
| 比联网 (`ebnew.com`) | 详情页需注册登录 |
| 深圳公共资源交易网 (`szggzy.com`) | SPA，需逆向 ajax 接口 |
| 南方科大采招网 (`sustech.edu.cn`) | SPA / JS 渲染 |
| 高校快速采购 (`wisdombidding.com`) | 首页访问超时，域名不稳定 |

## 用法

```bash
# 5 个站点 + 5 个关键词 + 前 3 页（每站每关键词）
python pyspider/examples/bidding_multi_spider.py \
    --sites ccgp,ustc_zhc,ipp,ihep,caep \
    --kw 质谱仪,X射线,光源,光学元件,光谱仪 \
    --start 2025-01-01 --end 2025-12-31 \
    --pages 3 --out output/bidding --use-llm
```

- `--sites`：逗号分隔的站点代号，默认 `ccgp,ustc_zhc,ipp,ihep,caep`
- `--kw`：逗号分隔的关键词。**只有 ccgp 用作搜索词**（调用站内搜索接口）；其它站点"全量抓 → 本地匹配 (title + content)"
- `--start / --end`：**只对 ccgp 生效**（其它站点没有时间范围过滤接口）
- `--pages`：每站每关键词抓取前 N 页（hard upper bound）
- `--out`：输出目录，产出 `bidding.db` + `bidding.jsonl`（跨站共用、URL-MD5 去重）
- `--use-llm`：对还缺字段的公告调用 LLM 兜底（需要 `OPENAI_API_KEY`）

## 输出字段

和 ccgp 单站爬虫相同的 13 个字段，外加一个 `site` 字段标识来源：

```json
{
  "url": "https://ztbxx.caep.ac.cn/jyxx/002001/002001001/20260413/xxx.html",
  "site": "caep",
  "title": "电磁驱动聚变大科学装置国家重大科技基础设施项目第一批招标公告",
  "category": "招标公告",
  "publish_date": "2026-04-13",
  "project_no": "",
  "buyer": "中国工程物理研究院流体物理研究所",
  "agent": "四川蜀工国际招标有限公司",
  "region": "",
  "budget": "",
  "winner": "",
  "amount": "",
  "content": "...",
  "crawled_at": "2026-04-20T10:00:00"
}
```

## 架构

```
bidding_multi_spider.py
├── Notice / Store                # 数据模型 + SQLite + JSONL
├── FIELD_PATTERNS                # 正则
├── TABLE_LABELS                  # 表格字段同义词
├── MULTILINE_LABELS              # 多行 key\nvalue 同义词
├── extract_pipeline()            # 三级流水线: table > multiline > regex
├── BaseSite                      # 站点插件基类 (3 个钩子)
│   ├── CCGPSite
│   ├── USTCZhcSite
│   ├── IPPSite
│   ├── IHEPSite
│   └── CAEPSite
└── Engine                        # HTTP / 限流 / 去重 / LLM 兜底 / 关键词过滤
```

## 加新站点

1. 继承 `BaseSite`，实现：

```python
class XxxSite(BaseSite):
    name = "xxx"
    home = "https://..."

    def list_urls(self, page):
        return [f"https://.../list_{page}.html"]

    def parse_list(self, html, list_url):
        soup = BeautifulSoup(html, "html.parser")
        return [ListItem(url=..., title=..., publish_date=...) for ...]

    def parse_detail_hints(self, soup, url):
        return title, body_el, category, publish_date
```

2. 在 `SITE_REGISTRY` 字典里注册：
```python
SITE_REGISTRY["xxx"] = XxxSite
```

3. `python bidding_multi_spider.py --sites xxx --kw ...` 即可。

## 本地冒烟测试结果（阶段 1）

广关键词 (`服务器,仪器,采购,...`)、`--pages 1`：

| 站 | 条数 | project_no | buyer | agent | budget | winner | amount |
|---|---|---|---|---|---|---|---|
| ustc_zhc | 15 | 0% | 47% | 0% | 0% | 0% | 47% |
| ipp | 20 | 100% | 100% | 20% | 75% | 0% | 0% |
| ihep | 3 | 0% | 0% | 0% | 0% | 0% | 0% |
| caep | 2 | 0% | 100% | 100% | 0% | 0% | 0% |

> **为什么 ustc_zhc 正文短 / 字段少**：USTC 采购处详情页只放一个外部链接（跳 `ahtba.org.cn` 安徽招投标平台），本站不存正文。想抓全文得再跟进外链（阶段 2 考虑）。
>
> **为什么 ihep 命中少**：`xwdt_1` 是"通知公告"合集，招标只是其中一类。本地关键词匹配把不相关的通知过滤掉了，符合预期。
>
> **字段命中率**：各站版式差异大，加 `--use-llm` 可以把 winner/amount 这种缺字段从 0% 补到 60%+。

## 限流 & 反爬

- UA 池轮换 + `Sec-Fetch-*` 完整 header
- 每请求 1.2-2.8s 随机 sleep（`default_delay`，可在子类里覆盖）
- 列表页先做 `warm_up`（访问站点首页拿 cookie）
- 失败指数退避 (2^i + random 秒)
- MD5(URL) 去重，重跑不会重复

## 已知限制

1. **关键词匹配是子串匹配**（大小写不敏感），没做分词 / 同义词扩展。如果想 "光源" 也匹配 "激光器"，需要在脚本里扩展 `keywords` 或加 LLM 重排序
2. **`--start / --end` 只影响 ccgp**。其它站点抓全量后再本地过滤日期需要自己后处理
3. **JSONL 是 append 模式**，多次运行会追加；想清空就删掉 `output/bidding/bidding.jsonl`
4. **阶段 2 的 9 个站点没接入**，需要先解决 cookie / SSO / SPA 逆向问题（见上表）
