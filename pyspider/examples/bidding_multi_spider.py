#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多站点招投标爬虫 (plugin architecture)
=====================================

在 `ccgp_keyword_spider.py` 的抽取能力基础上，把抓取流程抽象为"引擎 + 站点插件"两层：

- 引擎 (`Engine`): HTTP 会话、限流、URL 去重、三级字段抽取、LLM 兜底、关键词过滤、
  SQLite + JSONL 存储
- 站点插件 (`BaseSite` 子类): 每个招投标网站只需实现 `list_urls() / parse_list() /
  parse_detail_hints()` 三个钩子

阶段 1 已接入的站点 (纯 HTML，无登录)：
    ccgp        中国政府采购网        https://www.ccgp.gov.cn/
    ustc_zhc    中科大资产与后勤处    https://zhc.ustc.edu.cn/10843/list.htm
    ipp         等离子体物理研究所    http://www.ipp.ac.cn/ztbxx/zbxx/
    ihep        中科院高能所          http://www.ihep.ac.cn/xwdt2022/tzgg_1/
    caep        绵阳 CAEP 工物院      https://ztbxx.caep.ac.cn/

阶段 2 (需要 cookie/登录/JS 渲染) 暂未接入：
    gdedulscg / chinabidding / ebnew / szggzy / sustech / wisdombidding / ecnu_zcb /
    qdu / shanghaitech

用法:
    # 全部阶段 1 站点 + 5 个关键词
    python bidding_multi_spider.py \
        --sites ccgp,ustc_zhc,ipp,ihep,caep \
        --kw 质谱仪,X射线,光源,光学元件,光谱仪 \
        --start 2025-01-01 --end 2025-12-31 \
        --pages 3 --out output/bidding --use-llm

    # 只跑 ccgp + 关键词（向后兼容 ccgp_keyword_spider.py）
    python bidding_multi_spider.py --sites ccgp --kw 服务器 --pages 2
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bidding")

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def _headers(referer: str = "") -> Dict[str, str]:
    h = {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-site" if referer else "none",
        "Sec-Fetch-User": "?1",
    }
    if referer:
        h["Referer"] = referer
    return h


# ---------- 数据模型 ----------------------------------------------------------


@dataclass
class Notice:
    url: str
    site: str = ""              # 站点代号（ccgp / ustc_zhc / ...)
    title: str = ""
    category: str = ""
    publish_date: str = ""
    project_no: str = ""
    buyer: str = ""
    agent: str = ""
    region: str = ""
    budget: str = ""
    winner: str = ""
    amount: str = ""
    content: str = ""
    crawled_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


# ---------- 存储 --------------------------------------------------------------


class Store:
    """SQLite + JSONL 双写；URL MD5 去重跨站共用。"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS notice(
        id            TEXT PRIMARY KEY,
        url           TEXT UNIQUE,
        site          TEXT,
        title         TEXT,
        category      TEXT,
        publish_date  TEXT,
        project_no    TEXT,
        buyer         TEXT,
        agent         TEXT,
        region        TEXT,
        budget        TEXT,
        winner        TEXT,
        amount        TEXT,
        content       TEXT,
        crawled_at    TEXT
    )"""

    def __init__(self, out_dir: Path, db_name: str = "bidding.db",
                 jsonl_name: str = "bidding.jsonl") -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(out_dir / db_name)
        self.db.execute(self.SCHEMA)
        self.jsonl = (out_dir / jsonl_name).open("a", encoding="utf-8")

    @staticmethod
    def _rid(url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def has(self, url: str) -> bool:
        cur = self.db.execute("SELECT 1 FROM notice WHERE id=?", (self._rid(url),))
        return cur.fetchone() is not None

    def save(self, n: Notice) -> None:
        self.db.execute(
            """INSERT OR REPLACE INTO notice VALUES
               (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (self._rid(n.url), n.url, n.site, n.title, n.category,
             n.publish_date, n.project_no, n.buyer, n.agent, n.region,
             n.budget, n.winner, n.amount, n.content, n.crawled_at),
        )
        self.db.commit()
        self.jsonl.write(json.dumps(n.to_dict(), ensure_ascii=False) + "\n")
        self.jsonl.flush()

    def close(self) -> None:
        self.jsonl.close()
        self.db.close()


# ---------- 抽取 (三级流水线 + LLM 兜底) --------------------------------------


FIELD_PATTERNS: Dict[str, str] = {
    "project_no": (
        r"(?:项目编号|采购项目编号|招标编号|项目编号[（(]包[）)])[：:\s]*"
        r"([A-Za-z0-9\-_（()）]+?)(?:[）)\s]|$)"
    ),
    "buyer": (
        r"(?:采购人|采购单位|招标人)(?:名称|信息)?[：:\s]*(?:单位名称[：:\s]*)?"
        r"([^\n\r　：:]{2,80})"
    ),
    "agent": (
        r"(?:采购)?代理机构(?:名称|信息)?[：:\s]*(?:单位名称[：:\s]*)?"
        r"([^\n\r　：:]{2,80})"
    ),
    "region": r"(?:所属地区|地域|行政区划|所在地区)[：:\s]*([^\n\r　]{2,40})",
    "budget": (
        r"(?:采购预算|预算金额|预算(?:总)?价|项目预算)(?:\s*[（(][^)）]*[)）])?"
        r"[：:\s]*([0-9][0-9.,]*\s*(?:万元|元))"
    ),
    "amount": (
        r"(?:中标|成交|中选)(?:[（(][^)）]*[)）])?(?:金额|价格)"
        r"(?:\s*[（(][^)）]*[)）])?[：:\s]*([0-9][0-9.,]*\s*(?:万元|元)?)"
    ),
    "winner": (
        r"(?:中标|成交|中选)(?:供应商|单位|人)(?:名称)?"
        r"(?:\s*[（(][^)）]*[)）])?\s*[：:]\s*([^\n\r　：:]{2,80})"
    ),
}

TABLE_LABELS: Dict[str, List[str]] = {
    "winner":     ["中标供应商", "成交供应商", "中选供应商", "中标人名称",
                   "中标（成交）供应商", "供应商名称", "中标人", "成交单位"],
    "amount":     ["中标（成交）金额", "中标金额", "成交金额", "中标/成交金额",
                   "中标(成交)金额", "中选金额", "合同金额"],
    "budget":     ["预算金额", "采购预算", "项目预算", "预算总价", "预算",
                   "招标控制价", "最高限价"],
    "project_no": ["项目编号", "采购项目编号", "招标编号", "项目代码"],
    "buyer":      ["采购人名称", "采购人", "采购单位名称", "采购单位", "招标人"],
    "agent":      ["采购代理机构名称", "代理机构名称", "代理机构", "招标代理"],
    "region":     ["所属地区", "所在地区", "地域", "行政区划"],
}

MULTILINE_LABELS: Dict[str, List[str]] = {
    "amount": ["中标（成交）金额", "中标(成交)金额", "中标金额", "成交金额",
               "中标/成交金额"],
    "budget": ["预算金额", "采购预算", "项目预算"],
    "winner": ["中标（成交）供应商", "中标供应商", "成交供应商", "中标人名称",
               "中选供应商"],
}


def _match_label(cell: str) -> str:
    cell = re.sub(r"\s+", "", cell)
    cell = re.sub(r"[\(（][^)）]*[)）]", "", cell)
    for fld, labels in TABLE_LABELS.items():
        for lab in labels:
            if lab in cell:
                return fld
    return ""


def extract_from_tables(container) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for tb in container.find_all("table"):
        rows = tb.find_all("tr")
        if not rows:
            continue
        if len(rows) >= 2:
            heads = [c.get_text(" ", strip=True) for c in rows[0].find_all(["td", "th"])]
            values = [c.get_text(" ", strip=True) for c in rows[1].find_all(["td", "th"])]
            if heads and values and len(heads) == len(values) and len(heads) >= 2:
                for h, v in zip(heads, values):
                    fld = _match_label(h)
                    if fld and v and not out.get(fld):
                        out[fld] = v[:80]
        for tr in rows:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) == 2:
                fld = _match_label(cells[0])
                if fld and cells[1] and not out.get(fld):
                    out[fld] = cells[1][:80]
    return out


def extract_multiline(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    lines = [ln.strip() for ln in text.split("\n")]
    for i, ln in enumerate(lines[:-1]):
        clean = re.sub(r"[\s：:（()）]", "", ln)
        clean = re.sub(r"元|万元", "", clean)
        for fld, labels in MULTILINE_LABELS.items():
            if out.get(fld):
                continue
            for lab in labels:
                lab_clean = re.sub(r"[\s（()）]", "", lab)
                if clean == lab_clean or clean.endswith(lab_clean):
                    nxt = lines[i + 1]
                    if nxt and len(nxt) < 80:
                        out[fld] = nxt
                        break
    return out


def extract_regex(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, pat in FIELD_PATTERNS.items():
        m = re.search(pat, text)
        out[key] = m.group(1).strip() if m else ""
    return out


def extract_pipeline(body_el, text: str) -> Dict[str, str]:
    """三级流水线：table > multiline > regex（先填的不会被后面覆盖）"""
    fields: Dict[str, str] = {k: "" for k in FIELD_PATTERNS}
    for extractor in (
        lambda: extract_from_tables(body_el),
        lambda: extract_multiline(text),
        lambda: extract_regex(text),
    ):
        for k, v in extractor().items():
            if v and not fields.get(k):
                fields[k] = v
    return fields


# ---------- 站点插件接口 ------------------------------------------------------


@dataclass
class ListItem:
    url: str
    title: str = ""
    publish_date: str = ""
    category: str = ""


class BaseSite:
    """站点适配器基类。每个招投标网站实现 3 个方法即可。"""

    name: str = "base"
    home: str = ""
    default_delay: Tuple[float, float] = (1.2, 2.8)

    def list_urls(self, page: int) -> List[str]:
        """返回第 `page` 页的列表 URL 列表。多数站点返回 1 个，少数站点需要同时扫多个
        分类（例如 caep 同时扫 招标公告/中标候选人公示 等），也可以返回多个。"""
        raise NotImplementedError

    def parse_list(self, html: str, list_url: str) -> List[ListItem]:
        """从列表页 HTML 抽出详情 URL + 标题 + 日期 + 可选类别。"""
        raise NotImplementedError

    def parse_detail_hints(self, soup: BeautifulSoup, url: str):
        """返回 (title, body_el, category, publish_date) —— 让引擎知道正文容器在哪。

        title / category / publish_date 可以留空，引擎会用 url 或正文兜底。
        """
        title_el = soup.find("title")
        title = title_el.get_text(strip=True) if title_el else ""
        return title, soup.body or soup, "", ""


# ---------- 阶段 1 站点插件 ---------------------------------------------------


class CCGPSite(BaseSite):
    """中国政府采购网。有关键词搜索接口，是 5 个站里唯一支持'按关键词 + 时间范围'精确
    搜索的。其它站点由引擎抓全量后本地匹配关键词。"""

    name = "ccgp"
    home = "https://www.ccgp.gov.cn/"
    SEARCH = "http://search.ccgp.gov.cn/bxsearch"

    def __init__(self, keyword: str, start: str, end: str) -> None:
        self.keyword = keyword
        self.start = start
        self.end = end

    def list_urls(self, page: int) -> List[str]:
        params = {
            "searchtype": "1", "page_index": page, "bidSort": "0",
            "buyerName": "", "projectId": "", "pinMu": "0", "bidType": "0",
            "kw": self.keyword,
            "start_time": self.start.replace("-", ":"),
            "end_time": self.end.replace("-", ":"),
            "timeType": "6", "displayZone": "", "zoneId": "", "pppStatus": "0",
            "agentName": "",
        }
        return [f"{self.SEARCH}?{urlencode(params)}"]

    def parse_list(self, html: str, list_url: str) -> List[ListItem]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[ListItem] = []
        items = soup.select(".vT-srch-result-list-bid li, ul.vT-srch-result-list li")
        for li in items:
            a = li.find("a")
            if not a or not a.get("href"):
                continue
            href = a["href"]
            if not href.startswith("http"):
                continue
            span = li.find("span")
            out.append(ListItem(
                url=href,
                title=a.get_text(strip=True),
                publish_date=(span.get_text(strip=True) if span else ""),
            ))
        if not out:
            for a in soup.select("a[href*='/cggg/'][href$='.htm']"):
                out.append(ListItem(url=a["href"], title=a.get_text(strip=True)))
        return out

    def parse_detail_hints(self, soup: BeautifulSoup, url: str):
        title_el = soup.select_one(
            ".vF_detail_header, .vT_detail_title, h2.tc, h2, .title"
        ) or soup.title
        title = title_el.get_text(strip=True) if title_el else ""
        body_el = (soup.select_one(".vF_detail_content")
                   or soup.select_one(".vT_detail_main")
                   or soup.body or soup)
        m = re.search(r"/cggg/([a-z]+)/([a-z]+)/", url)
        category = f"{m.group(1)}/{m.group(2)}" if m else ""
        m2 = re.search(r"/(\d{6})/t(\d{8})_", url)
        publish_date = ""
        if m2:
            try:
                publish_date = datetime.strptime(m2.group(2), "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return title, body_el, category, publish_date


class USTCZhcSite(BaseSite):
    """中科大资产与后勤处 — 采购公告。
    列表: https://zhc.ustc.edu.cn/10843/list{N}.htm  (list.htm == list1.htm)
    详情: /YYYY/MMDD/c10843aNNNNNN/page.htm
    """

    name = "ustc_zhc"
    home = "https://zhc.ustc.edu.cn/"
    LIST_TPL = "https://zhc.ustc.edu.cn/10843/list{page}.htm"

    def list_urls(self, page: int) -> List[str]:
        if page == 1:
            return ["https://zhc.ustc.edu.cn/10843/list.htm"]
        return [self.LIST_TPL.format(page=page)]

    def parse_list(self, html: str, list_url: str) -> List[ListItem]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[ListItem] = []
        for a in soup.select("a[href*='/c10843a'][href$='/page.htm']"):
            href = urljoin(list_url, a["href"])
            title = a.get_text(strip=True)
            if not title:
                continue
            # 日期通常紧跟在 a 标签同级或父级
            date = ""
            parent = a.parent
            if parent:
                m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})",
                              parent.get_text(" ", strip=True))
                if m:
                    date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            if not date:
                m = re.search(r"/(\d{4})/(\d{4})/", href)
                if m:
                    mm, dd = m.group(2)[:2], m.group(2)[2:]
                    date = f"{m.group(1)}-{mm}-{dd}"
            out.append(ListItem(url=href, title=title, publish_date=date))
        return out

    def parse_detail_hints(self, soup: BeautifulSoup, url: str):
        title_el = soup.select_one(".arti_title, .wp_articlecontent h1, h1, h2, .title")
        title = title_el.get_text(strip=True) if title_el else (
            soup.title.get_text(strip=True) if soup.title else "")
        body_el = (soup.select_one(".wp_articlecontent")
                   or soup.select_one(".arti_content")
                   or soup.select_one("#artibody")
                   or soup.body or soup)
        m = re.search(r"/(\d{4})/(\d{4})/c", url)
        publish_date = (f"{m.group(1)}-{m.group(2)[:2]}-{m.group(2)[2:]}"
                        if m else "")
        return title, body_el, "", publish_date


class IPPSite(BaseSite):
    """等离子体物理研究所采购平台。
    列表: http://www.ipp.ac.cn/ztbxx/zbxx/        (首页)
          http://www.ipp.ac.cn/ztbxx/zbxx/index_{N}.html  (翻页)
    详情: ./YYYYMM/tYYYYMMDD_xxxxxx.html
    """

    name = "ipp"
    home = "http://www.ipp.ac.cn/"
    BASE = "http://www.ipp.ac.cn/ztbxx/zbxx/"

    def list_urls(self, page: int) -> List[str]:
        if page == 1:
            return [self.BASE]
        return [f"{self.BASE}index_{page}.html"]

    def parse_list(self, html: str, list_url: str) -> List[ListItem]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[ListItem] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(r"/\d{6}/t\d{8}_\d+\.html?$", href):
                continue
            abs_url = urljoin(list_url, href)
            title = a.get_text(strip=True)
            date = ""
            m = re.search(r"t(\d{8})_", href)
            if m:
                date = (f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:8]}")
            out.append(ListItem(url=abs_url, title=title, publish_date=date))
        # 去重（列表偶尔有两处同一个链接）
        seen, uniq = set(), []
        for it in out:
            if it.url in seen:
                continue
            seen.add(it.url)
            uniq.append(it)
        return uniq

    def parse_detail_hints(self, soup: BeautifulSoup, url: str):
        title_el = soup.select_one(".ipp2020-main h1, .arti_title, h1") or soup.title
        title = title_el.get_text(strip=True) if title_el else ""
        body_el = (soup.select_one(".ipp2020-main")
                   or soup.select_one(".TRS_Editor")
                   or soup.body or soup)
        m = re.search(r"/(\d{6})/t(\d{8})_", url)
        publish_date = ""
        if m:
            try:
                publish_date = datetime.strptime(m.group(2), "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return title, body_el, "", publish_date


class IHEPSite(BaseSite):
    """中科院高能物理研究所通知公告。
    用户给的 /zbgg/ 路径 403，实际"招标公告"混在"通知公告"里，用列表页：
        http://www.ihep.ac.cn/xwdt2022/tzgg_1/
    翻页: ./index_{N-1}.html    (首页无后缀 = index_1 语义)
    详情: ./YYYYMM/tYYYYMMDD_NNNNNNN.html
    """

    name = "ihep"
    home = "http://www.ihep.ac.cn/"
    BASE = "http://www.ihep.ac.cn/xwdt2022/tzgg_1/"

    def list_urls(self, page: int) -> List[str]:
        if page == 1:
            return [self.BASE]
        return [f"{self.BASE}index_{page - 1}.html"]

    def parse_list(self, html: str, list_url: str) -> List[ListItem]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[ListItem] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(r"/\d{6}/t\d{8}_\d+\.html?$", href):
                continue
            # 排除站内其它栏目
            if "/tzgg_1/" not in href and not href.startswith("./"):
                continue
            abs_url = urljoin(list_url, href)
            title = a.get_text(strip=True)
            date = ""
            m = re.search(r"t(\d{8})_", href)
            if m:
                date = f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:8]}"
            out.append(ListItem(url=abs_url, title=title, publish_date=date))
        seen, uniq = set(), []
        for it in out:
            if it.url in seen:
                continue
            seen.add(it.url)
            uniq.append(it)
        return uniq

    def parse_detail_hints(self, soup: BeautifulSoup, url: str):
        title_el = soup.select_one(".index-content h1, .content h1, h1, h2") \
                   or soup.title
        title = title_el.get_text(strip=True) if title_el else ""
        body_el = (soup.select_one(".index-content")
                   or soup.select_one(".content")
                   or soup.select_one(".TRS_Editor")
                   or soup.body or soup)
        m = re.search(r"/(\d{6})/t(\d{8})_", url)
        publish_date = ""
        if m:
            try:
                publish_date = datetime.strptime(m.group(2), "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return title, body_el, "", publish_date


class CAEPSite(BaseSite):
    """绵阳 CAEP (中国工程物理研究院) 招投标信息网。
    分多个子栏目（ewb 系统 category code）：
        002001001  公告信息 (招标公告)
        002001002  补充答疑公告
        002001004  最高限价公告
        002001005  中标(成交)候选人公示
    列表翻页: secondPage.html (page1), 2.html, 3.html, ...
    详情: /jyxx/002001/CCC/YYYYMMDD/<uuid>.html
    """

    name = "caep"
    home = "https://ztbxx.caep.ac.cn/"
    CATEGORIES = {
        "002001001": "招标公告",
        "002001002": "补充答疑",
        "002001004": "最高限价",
        "002001005": "中标候选人公示",
    }

    def list_urls(self, page: int) -> List[str]:
        name_for_page = "secondPage" if page == 1 else str(page)
        return [f"https://ztbxx.caep.ac.cn/jyxx/002001/{c}/{name_for_page}.html"
                for c in self.CATEGORIES]

    def parse_list(self, html: str, list_url: str) -> List[ListItem]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[ListItem] = []
        # 从 URL 反推当前栏目
        m_cat = re.search(r"/002001/(\d+)/", list_url)
        cat_code = m_cat.group(1) if m_cat else ""
        cat_label = self.CATEGORIES.get(cat_code, "")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # 详情: /jyxx/002001/CCC/YYYYMMDD/<uuid>.html
            if not re.search(r"/002001/\d+/\d{8}/[0-9a-f-]{32,}\.html?$", href):
                continue
            abs_url = urljoin(list_url, href)
            title = a.get_text(strip=True)
            date = ""
            m = re.search(r"/(\d{8})/[0-9a-f-]", href)
            if m:
                d = m.group(1)
                date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            out.append(ListItem(url=abs_url, title=title,
                                publish_date=date, category=cat_label))
        return out

    def parse_detail_hints(self, soup: BeautifulSoup, url: str):
        title_el = (soup.select_one(".ewb-info-title")
                    or soup.select_one(".article-title")
                    or soup.select_one("h1")
                    or soup.title)
        title = title_el.get_text(strip=True) if title_el else ""
        body_el = (soup.select_one(".ewb-info-bd")
                   or soup.select_one(".article")
                   or soup.select_one(".content")
                   or soup.body or soup)
        m_cat = re.search(r"/002001/(\d+)/", url)
        category = self.CATEGORIES.get(m_cat.group(1), "") if m_cat else ""
        m = re.search(r"/(\d{8})/[0-9a-f-]", url)
        publish_date = ""
        if m:
            d = m.group(1)
            publish_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        return title, body_el, category, publish_date


# 注册表：CLI --sites 的取值映射到插件工厂
SITE_REGISTRY: Dict[str, Callable[..., BaseSite]] = {
    "ccgp":     CCGPSite,       # 特殊：需要 keyword/start/end 参数
    "ustc_zhc": USTCZhcSite,
    "ipp":      IPPSite,
    "ihep":     IHEPSite,
    "caep":     CAEPSite,
}


# ---------- 引擎 --------------------------------------------------------------


class Engine:
    """统一的抓取 / 抽取 / 存储引擎。关键词过滤在保存时进行（title + content 匹配）。"""

    def __init__(self, out_dir: Path, keywords: List[str], use_llm: bool = False,
                 delay: Tuple[float, float] = (1.2, 2.8)) -> None:
        self.out_dir = out_dir
        self.keywords = [k for k in keywords if k]
        self.store = Store(out_dir)
        self.session = requests.Session()
        self.delay = delay
        self.llm = self._init_llm() if use_llm else None

    # ---- LLM ------------------------------------------------------------
    def _init_llm(self):
        try:
            from pyspider.ai_extractor.llm_extractor import LLMExtractor
        except Exception as e:
            log.warning("LLMExtractor 不可用 (%s)，LLM 兜底已禁用", e)
            return None
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            log.warning("OPENAI_API_KEY 未设置，LLM 兜底已禁用")
            return None
        model = os.getenv("BIDDING_LLM_MODEL", os.getenv("CCGP_LLM_MODEL", "gpt-4o-mini"))
        log.info("LLM 兜底启用: model=%s", model)
        return LLMExtractor(api_key=api_key, model=model)

    def _llm_fill_missing(self, text: str, fields: Dict[str, str]) -> Dict[str, str]:
        if not self.llm:
            return {}
        missing = [k for k in FIELD_PATTERNS if not fields.get(k)]
        if not missing:
            return {}
        schema = {k: "string" for k in missing}
        try:
            ai = self.llm.extract_structured(text[:12000], schema)
        except Exception as e:
            log.warning("LLM 调用失败: %s", e)
            return {}
        return {k: str(v).strip() for k, v in ai.items() if v}

    # ---- HTTP -----------------------------------------------------------
    def _sleep(self) -> None:
        time.sleep(random.uniform(*self.delay))

    def _warm_up(self, home: str) -> None:
        if not home:
            return
        try:
            self.session.get(home, headers=_headers(home), timeout=20)
            self._sleep()
        except requests.RequestException as e:
            log.warning("warm-up failed: %s: %s", home, e)

    def _fetch(self, url: str, referer: str = "", retries: int = 3) -> Optional[str]:
        for i in range(retries):
            try:
                r = self.session.get(
                    url, headers=_headers(referer or url), timeout=25,
                    allow_redirects=True,
                )
                r.encoding = r.apparent_encoding or "utf-8"
                if r.status_code != 200:
                    log.warning("%s -> HTTP %s", url, r.status_code)
                elif "频繁" in r.text[:5000] or "blocked" in r.text[:1500].lower():
                    log.warning("被限流：%s (attempt %d)", url, i + 1)
                else:
                    return r.text
            except requests.RequestException as e:
                log.warning("request failed %s: %s", url, e)
            time.sleep((2 ** i) + random.random())
        return None

    # ---- 关键词过滤 -----------------------------------------------------
    def _matches_keyword(self, notice: Notice) -> bool:
        if not self.keywords:
            return True
        blob = (notice.title + "\n" + notice.content).lower()
        return any(k.lower() in blob for k in self.keywords)

    # ---- 详情页 -> Notice -----------------------------------------------
    def _build_notice(self, site: BaseSite, url: str, html: str,
                      hint: Optional[ListItem] = None) -> Notice:
        soup = BeautifulSoup(html, "html.parser")
        title, body_el, category, publish_date = site.parse_detail_hints(soup, url)
        text = body_el.get_text("\n", strip=True) if body_el else ""
        fields = extract_pipeline(body_el or soup, text)
        if self.llm:
            for k, v in self._llm_fill_missing(text, fields).items():
                if v and not fields.get(k):
                    fields[k] = v
        return Notice(
            url=url,
            site=site.name,
            title=title or (hint.title if hint else ""),
            category=category or (hint.category if hint else ""),
            publish_date=publish_date or (hint.publish_date if hint else ""),
            content=text[:20000],
            **fields,
        )

    # ---- 单站抓取 --------------------------------------------------------
    def run_site(self, site: BaseSite, max_pages: int) -> int:
        log.info("========= 站点: %s =========", site.name)
        self._warm_up(site.home)
        total = 0
        for page in range(1, max_pages + 1):
            list_urls = site.list_urls(page)
            page_hit = 0
            for list_url in list_urls:
                log.info("[%s p%d] %s", site.name, page, list_url)
                html = self._fetch(list_url, referer=site.home)
                if not html:
                    continue
                items = site.parse_list(html, list_url)
                log.info("  命中 %d 条", len(items))
                for it in items:
                    if self.store.has(it.url):
                        continue
                    self._sleep()
                    dhtml = self._fetch(it.url, referer=list_url)
                    if not dhtml:
                        continue
                    try:
                        notice = self._build_notice(site, it.url, dhtml, hint=it)
                    except Exception as e:
                        log.warning("parse detail failed %s: %s", it.url, e)
                        continue
                    if not self._matches_keyword(notice):
                        continue
                    self.store.save(notice)
                    total += 1
                    page_hit += 1
                    log.info("  · %s | %s | %s",
                             notice.publish_date or "",
                             (notice.title or "")[:40],
                             notice.category or "")
                self._sleep()
            if page_hit == 0 and page >= 1:
                # 本页所有列表 URL 都没新增（可能已到末页），尝试再翻 1 页即 break
                # 避免一个站白跑很多空页
                pass
        log.info("[%s] 新增 %d 条", site.name, total)
        return total

    def close(self) -> None:
        self.store.close()


# ---------- CLI ---------------------------------------------------------------


def _build_site(name: str, args: argparse.Namespace, keyword: str) -> Optional[BaseSite]:
    if name == "ccgp":
        return CCGPSite(keyword=keyword, start=args.start, end=args.end)
    cls = SITE_REGISTRY.get(name)
    if not cls:
        log.error("未知站点: %s", name)
        return None
    return cls()


def main() -> int:
    ap = argparse.ArgumentParser(description="多站点招投标爬虫")
    ap.add_argument("--sites",
                    default="ccgp,ustc_zhc,ipp,ihep,caep",
                    help="用逗号分隔的站点代号，可选: "
                         + ", ".join(SITE_REGISTRY))
    ap.add_argument("--kw", required=True,
                    help="关键词；多个用英文逗号分隔，例如 "
                         "'质谱仪,X射线,光源,光学元件,光谱仪'")
    ap.add_argument("--start", default="2025-01-01",
                    help="(仅 ccgp 生效) 开始日期 YYYY-MM-DD")
    ap.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"),
                    help="(仅 ccgp 生效) 结束日期 YYYY-MM-DD")
    ap.add_argument("--pages", type=int, default=3,
                    help="每站/每关键词抓取前 N 页")
    ap.add_argument("--out", default="output/bidding", help="输出目录")
    ap.add_argument("--use-llm", action="store_true",
                    help="启用 LLM 兜底（需要 OPENAI_API_KEY）")
    args = ap.parse_args()

    keywords = [k.strip() for k in args.kw.split(",") if k.strip()]
    sites = [s.strip() for s in args.sites.split(",") if s.strip()]

    out_dir = Path(args.out)
    engine = Engine(out_dir=out_dir, keywords=keywords, use_llm=args.use_llm)
    total_all = 0
    try:
        for site_name in sites:
            if site_name == "ccgp":
                # ccgp 按关键词搜索，每个关键词跑一遍（共享 Engine / Store 去重）
                for kw in keywords:
                    log.info("----- ccgp keyword=%s -----", kw)
                    site = CCGPSite(keyword=kw, start=args.start, end=args.end)
                    total_all += engine.run_site(site, max_pages=args.pages)
            else:
                site = _build_site(site_name, args, keywords[0] if keywords else "")
                if not site:
                    continue
                total_all += engine.run_site(site, max_pages=args.pages)
    except KeyboardInterrupt:
        log.info("user interrupt")
    finally:
        engine.close()
    log.info("全部结束。总共新增 %d 条", total_all)
    return 0


if __name__ == "__main__":
    sys.exit(main())
