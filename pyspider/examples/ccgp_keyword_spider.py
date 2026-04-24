#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中国政府采购网 (ccgp.gov.cn) 按关键词搜索爬虫

- 基于 search.ccgp.gov.cn/bxsearch 搜索接口
- 支持按关键词 / 时间范围 / 公告类型 搜索
- 详情页抽取：先正则兜底，缺失字段再交给 LLM
- 输出：SQLite + JSONL

用法:
    python ccgp_keyword_spider.py --kw 服务器 --start 2025-01-01 --end 2025-12-31 --pages 3
    python ccgp_keyword_spider.py --kw 服务器 --pages 3 --use-llm     # 启用 LLM 兜底
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
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ccgp")

HOME = "https://www.ccgp.gov.cn/"
SEARCH = "http://search.ccgp.gov.cn/bxsearch"

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


def _headers(referer: str = HOME) -> Dict[str, str]:
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Referer": referer,
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-User": "?1",
    }


@dataclass
class Notice:
    url: str
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

    def to_dict(self) -> Dict:
        return asdict(self)


class Store:
    """SQLite + JSONL 双写，URL 去重"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS notice(
        id            TEXT PRIMARY KEY,
        url           TEXT UNIQUE,
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

    def __init__(self, out_dir: Path):
        out_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(out_dir / "ccgp.db")
        self.db.execute(self.SCHEMA)
        self.jsonl = (out_dir / "ccgp.jsonl").open("a", encoding="utf-8")

    def has(self, url: str) -> bool:
        rid = hashlib.md5(url.encode()).hexdigest()
        cur = self.db.execute("SELECT 1 FROM notice WHERE id=?", (rid,))
        return cur.fetchone() is not None

    def save(self, n: Notice) -> None:
        rid = hashlib.md5(n.url.encode()).hexdigest()
        self.db.execute(
            """INSERT OR REPLACE INTO notice VALUES
               (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (rid, n.url, n.title, n.category, n.publish_date, n.project_no,
             n.buyer, n.agent, n.region, n.budget, n.winner, n.amount,
             n.content, n.crawled_at),
        )
        self.db.commit()
        self.jsonl.write(json.dumps(n.to_dict(), ensure_ascii=False) + "\n")
        self.jsonl.flush()

    def close(self) -> None:
        self.jsonl.close()
        self.db.close()


class CCGPSpider:
    """CCGP 按关键词搜索爬虫"""

    # 详情页正则（大多数公告都遵循这一套格式）
    # 注：ccgp 很多字段是表格布局（key 和 value 在相邻两行），所以同时支持两种格式
    FIELD_PATTERNS = {
        "project_no": (
            r"(?:项目编号|采购项目编号|招标编号|项目编号[（(]包[）)])[：:\s]*"
            r"([A-Za-z0-9\-_（()）]+?)(?:[）)\s]|$)"
        ),
        "buyer": (
            r"采购人(?:名称|信息)?[：:\s]*(?:单位名称[：:\s]*)?"
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
            r"(?:中标|成交)(?:[（(][^)）]*[)）])?(?:金额|价格)"
            r"(?:\s*[（(][^)）]*[)）])?[：:\s]*([0-9][0-9.,]*\s*(?:万元|元)?)"
        ),
        # winner 必须有显式冒号分隔 label 和 value，否则容易吃到
        # "成交人自成交通知书出具之日起..." 这类散文，把条款当成供应商名
        "winner": (
            r"(?:中标|成交)(?:供应商|单位|人)(?:名称)?"
            r"(?:\s*[（(][^)）]*[)）])?\s*[：:]\s*([^\n\r　：:]{2,80})"
        ),
    }

    def __init__(
        self,
        keyword: str,
        start: str,
        end: str,
        out_dir: Path,
        max_pages: int = 3,
        use_llm: bool = False,
        delay: Tuple[float, float] = (1.2, 2.8),
    ) -> None:
        self.keyword = keyword
        self.start = start
        self.end = end
        self.max_pages = max_pages
        self.delay = delay
        self.session = requests.Session()
        self.store = Store(out_dir)
        self.llm = self._init_llm() if use_llm else None

    # ---- LLM fallback ----------------------------------------------------
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
        model = os.getenv("CCGP_LLM_MODEL", "gpt-4o-mini")
        log.info("LLM 兜底启用：model=%s", model)
        return LLMExtractor(api_key=api_key, model=model)

    # ---- HTTP ------------------------------------------------------------
    def _sleep(self) -> None:
        time.sleep(random.uniform(*self.delay))

    def _warm_up(self) -> None:
        """先访问首页拿 cookie，后续请求才不会被判为爬虫"""
        try:
            self.session.get(HOME, headers=_headers(HOME), timeout=20)
            self._sleep()
        except requests.RequestException as e:
            log.warning("warm-up failed: %s", e)

    def _fetch(self, url: str, referer: str = HOME, retries: int = 3) -> Optional[str]:
        for i in range(retries):
            try:
                r = self.session.get(
                    url, headers=_headers(referer), timeout=25, allow_redirects=True
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

    # ---- 搜索 -----------------------------------------------------------
    def _search_url(self, page: int) -> str:
        params = {
            "searchtype": "1",
            "page_index": page,
            "bidSort": "0",
            "buyerName": "",
            "projectId": "",
            "pinMu": "0",
            "bidType": "0",
            "kw": self.keyword,
            "start_time": self.start.replace("-", ":"),
            "end_time": self.end.replace("-", ":"),
            "timeType": "6",        # 6 = 自定义时间范围
            "displayZone": "",
            "zoneId": "",
            "pppStatus": "0",
            "agentName": "",
        }
        return f"{SEARCH}?{urlencode(params)}"

    def _parse_search(self, html: str) -> List[Tuple[str, str, str]]:
        """返回 [(detail_url, title, date)]"""
        soup = BeautifulSoup(html, "html.parser")
        results: List[Tuple[str, str, str]] = []
        items = soup.select(".vT-srch-result-list-bid li, ul.vT-srch-result-list li")
        for li in items:
            a = li.find("a")
            if not a or not a.get("href"):
                continue
            url = a["href"]
            if not url.startswith("http"):
                continue
            title = a.get_text(strip=True)
            span = li.find("span")
            date = span.get_text(strip=True) if span else ""
            results.append((url, title, date))
        # 兜底：搜索结果布局偶尔变动
        if not results:
            for a in soup.select("a[href*='/cggg/'][href$='.htm']"):
                results.append((a["href"], a.get_text(strip=True), ""))
        return results

    # ---- 详情页 ---------------------------------------------------------
    @staticmethod
    def _infer_category(url: str) -> str:
        m = re.search(r"/cggg/([a-z]+)/([a-z]+)/", url)
        return f"{m.group(1)}/{m.group(2)}" if m else ""

    @staticmethod
    def _infer_date(url: str) -> str:
        m = re.search(r"/(\d{6})/t(\d{8})_", url)
        if not m:
            return ""
        try:
            return datetime.strptime(m.group(2), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return ""

    def _regex_extract(self, text: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, pat in self.FIELD_PATTERNS.items():
            m = re.search(pat, text)
            out[key] = m.group(1).strip() if m else ""
        return out

    # 表格字段 <-> Notice 字段 映射（按优先级匹配第一条命中）
    TABLE_LABELS: Dict[str, List[str]] = {
        "winner":     ["中标供应商", "成交供应商", "中标人名称", "中标（成交）供应商",
                       "供应商名称"],
        "amount":     ["中标（成交）金额", "中标金额", "成交金额", "中标/成交金额",
                       "中标(成交)金额"],
        "budget":     ["预算金额", "采购预算", "项目预算", "预算总价", "预算"],
        "project_no": ["项目编号", "采购项目编号", "招标编号"],
        "buyer":      ["采购人名称", "采购人", "采购单位名称", "采购单位"],
        "agent":      ["采购代理机构名称", "代理机构名称", "代理机构"],
        "region":     ["所属地区", "所在地区", "地域", "行政区划"],
    }

    def _match_label(self, cell: str) -> str:
        """把一个单元格文本归一化到 Notice 字段名，匹配不到返回空串"""
        cell = re.sub(r"\s+", "", cell)
        cell = re.sub(r"[\(（][^)）]*[)）]", "", cell)  # 去括号注释
        for field, labels in self.TABLE_LABELS.items():
            for lab in labels:
                if lab in cell:
                    return field
        return ""

    def _extract_from_tables(self, container) -> Dict[str, str]:
        """从 <table> 中抽字段，支持两种布局：
        1. 水平布局：第 0 行是表头（字段名），第 1 行是值
        2. 垂直布局：两列（key | value）
        """
        out: Dict[str, str] = {}
        for tb in container.find_all("table"):
            rows = tb.find_all("tr")
            if not rows:
                continue
            # 布局 1：header + value
            if len(rows) >= 2:
                heads = [c.get_text(" ", strip=True) for c in rows[0].find_all(["td", "th"])]
                values = [c.get_text(" ", strip=True) for c in rows[1].find_all(["td", "th"])]
                if heads and values and len(heads) == len(values) and len(heads) >= 2:
                    for h, v in zip(heads, values):
                        field = self._match_label(h)
                        if field and v and not out.get(field):
                            out[field] = v[:80]
            # 布局 2：label / value 两列
            for tr in rows:
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(cells) == 2:
                    field = self._match_label(cells[0])
                    if field and cells[1] and not out.get(field):
                        out[field] = cells[1][:80]
        return out

    # "label\nvalue" 形式（去掉 html 后的纯文本，多行表格被拍平）
    MULTILINE_LABELS: Dict[str, List[str]] = {
        "amount": ["中标（成交）金额", "中标(成交)金额", "中标金额", "成交金额",
                   "中标/成交金额"],
        "budget": ["预算金额", "采购预算", "项目预算"],
        "winner": ["中标（成交）供应商", "中标供应商", "成交供应商", "中标人名称"],
    }

    def _extract_multiline(self, text: str) -> Dict[str, str]:
        """key 在一行、value 在下一行的情形"""
        out: Dict[str, str] = {}
        lines = [ln.strip() for ln in text.split("\n")]
        for i, ln in enumerate(lines[:-1]):
            clean = re.sub(r"[\s：:（()）]", "", ln)
            clean = re.sub(r"元|万元", "", clean)
            for field, labels in self.MULTILINE_LABELS.items():
                if out.get(field):
                    continue
                for lab in labels:
                    lab_clean = re.sub(r"[\s（()）]", "", lab)
                    if clean == lab_clean or clean.endswith(lab_clean):
                        nxt = lines[i + 1]
                        if nxt and len(nxt) < 80:
                            out[field] = nxt
                            break
        return out

    def _llm_extract(self, html: str, row: Dict[str, str]) -> Dict[str, str]:
        """只对还缺的字段调用 LLM，节省 token"""
        if not self.llm:
            return {}
        missing = [k for k in self.FIELD_PATTERNS if not row.get(k)]
        if not missing:
            return {}
        schema = {k: "string" for k in missing}
        try:
            ai = self.llm.extract_structured(html[:12000], schema)
        except Exception as e:
            log.warning("LLM 调用失败: %s", e)
            return {}
        return {k: str(v).strip() for k, v in ai.items() if v}

    def _parse_detail(self, html: str, url: str) -> Notice:
        soup = BeautifulSoup(html, "html.parser")
        title_el = soup.select_one(
            ".vF_detail_header, .vT_detail_title, h2.tc, h2, .title"
        ) or soup.title
        title = title_el.get_text(strip=True) if title_el else ""
        body_el = (
            soup.select_one(".vF_detail_content")
            or soup.select_one(".vT_detail_main")
            or soup.body
            or soup
        )
        text = body_el.get_text("\n", strip=True)

        # 抽取优先级: 表格 > 多行 label/value > 单行正则 (先填的不会被后面的覆盖)
        fields: Dict[str, str] = {k: "" for k in self.FIELD_PATTERNS}
        for extractor in (
            lambda: self._extract_from_tables(body_el),
            lambda: self._extract_multiline(text),
            lambda: self._regex_extract(text),
        ):
            for k, v in extractor().items():
                if v and not fields.get(k):
                    fields[k] = v

        if self.llm:
            for k, v in self._llm_extract(text, fields).items():
                if v and not fields.get(k):
                    fields[k] = v

        return Notice(
            url=url,
            title=title,
            category=self._infer_category(url),
            publish_date=self._infer_date(url),
            content=text[:20000],
            **fields,
        )

    # ---- 主循环 ---------------------------------------------------------
    def run(self) -> int:
        log.info("keyword=%r | date=[%s ~ %s] | pages=%d | llm=%s",
                 self.keyword, self.start, self.end, self.max_pages, bool(self.llm))
        self._warm_up()
        total = 0
        for page in range(1, self.max_pages + 1):
            url = self._search_url(page)
            log.info("[page %d] %s", page, url)
            html = self._fetch(url, referer=HOME)
            if not html:
                log.error("  页面获取失败，跳过")
                continue
            items = self._parse_search(html)
            log.info("  命中 %d 条", len(items))
            if not items:
                break
            for detail_url, title, date in items:
                if self.store.has(detail_url):
                    continue
                self._sleep()
                dhtml = self._fetch(detail_url, referer=url)
                if not dhtml:
                    continue
                notice = self._parse_detail(dhtml, detail_url)
                self.store.save(notice)
                total += 1
                log.info("  · %s | %s | %s",
                         notice.publish_date or date,
                         (notice.title or title)[:40],
                         notice.category)
            self._sleep()
        log.info("done. 新增 %d 条", total)
        self.store.close()
        return total


def main() -> int:
    ap = argparse.ArgumentParser(description="ccgp.gov.cn 关键词爬虫")
    ap.add_argument("--kw", required=True,
                    help="搜索关键词；多个用英文逗号分隔，例如 "
                         "'质谱仪,X射线,光源,光学元件,光谱仪'")
    ap.add_argument("--start", default="2025-01-01", help="开始日期 YYYY-MM-DD")
    ap.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"),
                    help="结束日期 YYYY-MM-DD")
    ap.add_argument("--pages", type=int, default=3, help="每个关键词抓取前 N 页")
    ap.add_argument("--out", default="output/ccgp", help="输出目录")
    ap.add_argument("--use-llm", action="store_true",
                    help="启用 LLM 兜底（需要 OPENAI_API_KEY）")
    args = ap.parse_args()

    keywords = [k.strip() for k in args.kw.split(",") if k.strip()]
    total_all = 0
    for kw in keywords:
        log.info("========= 关键词: %s =========", kw)
        spider = CCGPSpider(
            keyword=kw,
            start=args.start,
            end=args.end,
            out_dir=Path(args.out),
            max_pages=args.pages,
            use_llm=args.use_llm,
        )
        try:
            total_all += spider.run()
        except KeyboardInterrupt:
            log.info("user interrupt")
            break
    log.info("全部结束。总共新增 %d 条", total_all)
    return 0


if __name__ == "__main__":
    sys.exit(main())
