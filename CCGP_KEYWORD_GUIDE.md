# 中国政府采购网 (ccgp.gov.cn) 关键词爬虫

基于 `search.ccgp.gov.cn/bxsearch` 搜索接口的采购公告定向抓取脚本，位于
<ref_file file="pyspider/examples/ccgp_keyword_spider.py" />。

适合场景：按行业 / 产品关键词（例如 "服务器"、"CT 机"、"云服务"）
监控政府采购项目，产出结构化 `JSONL + SQLite` 便于进一步分析。

---

## 1. 快速上手

```bash
cd superspider
pip install requests beautifulsoup4

# 关键词 "服务器"，2025 全年，前 3 页
python pyspider/examples/ccgp_keyword_spider.py \
    --kw 服务器 \
    --start 2025-01-01 \
    --end   2025-12-31 \
    --pages 3 \
    --out   output/ccgp
```

产物：
- `output/ccgp/ccgp.jsonl`  每行一条公告（便于 `jq` / pandas 消费）
- `output/ccgp/ccgp.db`     SQLite，`notice` 表带 URL 去重

---

## 2. CLI 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--kw` | — (必填) | 搜索关键词 |
| `--start` | `2025-01-01` | 起始日期 `YYYY-MM-DD` |
| `--end` | 今天 | 结束日期 |
| `--pages` | `3` | 最多抓取前 N 页（每页 20 条）|
| `--out` | `output/ccgp` | 输出目录 |
| `--use-llm` | 关 | 启用 LLM 兜底（需 `OPENAI_API_KEY`）|

---

## 3. 字段抽取策略

ccgp 详情页大多是 **混合布局**：一部分用表格（`<table>` 里 header + value 两行），
一部分是"键一行、值下一行"的平铺文本，还有少量 `key: value` 行。所以脚本用
**三级流水线**，先命中的优先：

1. **`<table>` 解析** (`_extract_from_tables`) —— 识别两种表格：
   - 水平布局：第 0 行是字段名 (`供应商名称 | 中标金额 | ...`)，第 1 行是值
   - 垂直布局：两列 (`项目编号 | 2025-...`)
2. **多行键值** (`_extract_multiline`) —— 处理 "中标（成交）金额\n79.80" 这种
   label 独占一行、值在下一行的情况
3. **正则** (`_regex_extract`) —— 兜底单行 `key: value` 文本

可选第四级：`--use-llm` 时调用
<ref_file file="pyspider/ai_extractor/llm_extractor.py" />
的 `LLMExtractor` 只对**仍缺失**的字段做结构化抽取（省 token）。

### 抽取字段

| 字段 | 说明 | 来源 |
|---|---|---|
| `title` | 公告标题 | `<h2>` / `<title>` |
| `category` | `{中央/地方}/{子类}` | URL 自推 |
| `publish_date` | 发布日期 | URL 自推 |
| `project_no` | 项目/招标编号 | 正则 + 表格 |
| `buyer` | 采购人 | 正则 + 表格 |
| `agent` | 代理机构 | 正则 + 表格 |
| `region` | 所属地区 | 正则（ccgp 大部分公告未提供）|
| `budget` | 预算金额（含单位）| 表格 > 多行 > 正则 |
| `winner` | 中标 / 成交供应商 | 表格 > 正则 |
| `amount` | 中标 / 成交金额 | 表格 > 多行 > 正则 |
| `content` | 纯文本正文（截断到 20 K） | `body` innerText |

### 实测命中率（关键词 "服务器"，2025 年，40 条样本）

| 字段 | 命中率 | 备注 |
|---|---|---|
| title / category / publish_date | 100 % | 结构化推导 |
| buyer | 100 % | |
| project_no | 87 % | |
| agent | 60 % | |
| winner | 30 %（在中标/成交公告中 66 %）| 仅在 `zbgg/cjgg/gzgg` 类别中有意义 |
| amount | 35 % | 同上 |
| budget | 40 % | 主要出现在 `xjgg/gkzb` |
| region | 0 % | 详情页极少含 |

> winner/amount 整体命中率看起来低，是因为 40 条样本里只有 18 条属于"中标/成交"
> 类（`zbgg/cjgg/gzgg`），其余如 `xjgg` (询价) / `gkzb` (开标) 本来就没中标信息。
> 按类别计算实际命中率在 **60 %+**。

### LLM 兜底 (`--use-llm`)

启用后只对**没被前三级抓到**的字段发 prompt，典型能把 `budget/amount/winner`
提到 85 %+：

```bash
export OPENAI_API_KEY=sk-xxx
python pyspider/examples/ccgp_keyword_spider.py \
    --kw 医疗设备 --pages 5 --use-llm
```

---

## 4. 反爬要点

ccgp 对 HTTP 头敏感，脚本已处理：

- `User-Agent` 池 + 每次请求随机轮换
- `Sec-Fetch-*` / `Upgrade-Insecure-Requests` 等浏览器特征头
- 会话复用 `requests.Session()`，进入前先 warm-up 访问 home page
- 请求间 1.2 ~ 2.8 秒随机 sleep
- 检测"频繁访问"告警自动退避重试

如果长时间跑仍被拦，可以：

- 切换本地网络 / 代理 IP
- 再拉大 `DELAY_RANGE`
- 接入仓库 <ref_file file="pyspider/antibot/__init__.py" /> 的 `AntiBotManager`
  做 TLS 指纹轮换

---

## 5. 数据消费示例

```bash
# 统计每个类别的预算总和
jq -r 'select(.budget != "") | [.category, .budget] | @tsv' \
    output/ccgp/ccgp.jsonl | column -t

# SQLite 查询
sqlite3 output/ccgp/ccgp.db \
  "SELECT category, count(*), sum(cast(replace(budget,'元','') as real)) \
   FROM notice WHERE budget != '' GROUP BY category;"
```

---

## 6. 进阶

- **增量**：脚本内置 URL 级 MD5 去重（`notice.id`），重复跑只会补新数据
- **分布式**：`(keyword, page)` 元组扔 Redis 队列即可多 worker 并发，参考
  <ref_file file="pyspider/examples/distributed_demo.py" />
- **扩展字段**：在 `FIELD_PATTERNS` / `TABLE_LABELS` / `MULTILINE_LABELS`
  各加一行即可，三套抽取器会自动 pick up

---
