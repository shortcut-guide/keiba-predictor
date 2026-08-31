#!/usr/bin/env python3
"""Fetch and export August 2026 JRA/Nankan race results from keiba-intelligence.jp."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://data.keiba-intelligence.jp"
SITEMAP_URL = f"{BASE_URL}/sitemap-0.xml"
RACE_URL_RE = re.compile(
    r"^https://data\.keiba-intelligence\.jp/"
    r"(?P<source>jra|nankan)/results/2026/08/(?P<day>\d{2})/"
    r"(?P<venue>[a-z]+)/(?P<race>\d+)/$"
)
EXPECTED_SOURCE_COUNTS = {"jra": 360, "nankan": 288}
USER_AGENT = "keiba-monthly-backfill/1.0 (+https://github.com/shortcut-guide/keiba)"

VENUES = {
    "sapporo": "札幌",
    "hakodate": "函館",
    "fukushima": "福島",
    "niigata": "新潟",
    "tokyo": "東京",
    "nakayama": "中山",
    "chukyo": "中京",
    "kyoto": "京都",
    "hanshin": "阪神",
    "kokura": "小倉",
    "urawa": "浦和",
    "funabashi": "船橋",
    "ooi": "大井",
    "kawasaki": "川崎",
}

_thread_local = threading.local()


@dataclass(frozen=True)
class ParsedPage:
    race: dict[str, Any]
    horses: list[dict[str, Any]]
    payouts: list[dict[str, Any]]


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=0.8,
        status_forcelist=(408, 425, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=12, pool_maxsize=12)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})
    return session


def session_for_thread() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = build_session()
        _thread_local.session = session
    return session


def get_text(url: str, *, timeout: int = 45) -> str:
    response = session_for_thread().get(url, timeout=timeout)
    response.raise_for_status()
    if not response.encoding or response.encoding.lower() == "iso-8859-1":
        response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def clean_text(value: str | None) -> str:
    return " ".join((value or "").replace("\u3000", " ").split())


def nullable(value: str | None) -> str | None:
    text = clean_text(value)
    return None if not text or text in {"-", "―", "－"} else text


def first_named(text: str, names: tuple[str, ...]) -> str | None:
    compact = clean_text(text)
    for name in names:
        if name in compact:
            return name
    return None


def parse_int(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"\d+", text.replace(",", ""))
    return int(match.group()) if match else None


def overview_map(soup: BeautifulSoup) -> dict[str, str]:
    for heading in soup.find_all(["h2", "h3"]):
        if "レース概要" not in heading.get_text(" ", strip=True):
            continue
        dl = heading.find_next("dl")
        if dl is None:
            break
        result: dict[str, str] = {}
        for dt in dl.find_all("dt", recursive=False):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                result[clean_text(dt.get_text(" ", strip=True))] = clean_text(
                    dd.get_text(" ", strip=True)
                )
        return result
    return {}


def find_table(soup: BeautifulSoup, required_headers: set[str]) -> tuple[Tag, list[str]]:
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if first_row is None:
            continue
        headers = [clean_text(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]
        if required_headers.issubset(set(headers)):
            return table, headers
    raise ValueError(f"required table not found: {sorted(required_headers)}")


def parse_url_parts(url: str) -> re.Match[str]:
    match = RACE_URL_RE.match(url)
    if not match:
        raise ValueError(f"unexpected race URL: {url}")
    return match


def make_race_id(source: str, date: str, venue_slug: str, race_number: int) -> str:
    return f"ki_{source}_{date.replace('-', '')}_{venue_slug}_{race_number:02d}"


def parse_race(url: str, html: str) -> ParsedPage:
    match = parse_url_parts(url)
    source = match.group("source")
    venue_slug = match.group("venue")
    race_number = int(match.group("race"))
    soup = BeautifulSoup(html, "html.parser")

    time_tag = soup.find("time", attrs={"datetime": True})
    if time_tag is None:
        raise ValueError("race date is missing")
    date = clean_text(str(time_tag.get("datetime")))
    if not re.fullmatch(r"2026-08-\d{2}", date):
        raise ValueError(f"unexpected race date: {date}")

    title = soup.find("h1")
    race_name = clean_text(title.get_text(" ", strip=True)) if title else ""
    if not race_name:
        raise ValueError("race name is missing")

    overview = overview_map(soup)
    course = overview.get("距離・コース", "")
    distance = parse_int(course)
    surface = first_named(course, ("ダート", "芝", "障害"))
    weather = first_named(overview.get("天候", ""), ("小雨", "小雪", "晴", "曇", "雨", "雪"))
    condition = first_named(overview.get("馬場状態", ""), ("不良", "稍重", "重", "良"))
    grade_match = re.search(r"(?:J[・-]?)?G[ⅠⅡⅢI]{1,3}", race_name, flags=re.IGNORECASE)
    grade = grade_match.group(0) if grade_match else None

    venue = VENUES.get(venue_slug)
    if venue is None:
        raise ValueError(f"unknown venue slug: {venue_slug}")
    race_id = make_race_id(source, date, venue_slug, race_number)
    race = {
        "id": race_id,
        "date": date,
        "venue": venue,
        "raceNumber": race_number,
        "raceName": race_name,
        "distance": distance,
        "surface": surface,
        "grade": grade,
        "weather": weather,
        "trackCondition": condition,
        "url": url,
        "source": source,
    }

    result_table, headers = find_table(soup, {"着順", "馬名", "騎手", "調教師", "タイム", "人気"})
    horses: list[dict[str, Any]] = []
    for row_index, row in enumerate(result_table.find_all("tr")[1:], start=1):
        cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
        if not cells:
            continue
        if len(cells) != len(headers):
            raise ValueError(f"result row column mismatch: headers={len(headers)} cells={len(cells)}")
        item = dict(zip(headers, cells, strict=True))
        horse_name = clean_text(item.get("馬名"))
        if not horse_name:
            continue
        rank_text = clean_text(item.get("着順"))
        rank_match = re.match(r"^(\d+)", rank_text)
        ranking = int(rank_match.group(1)) if rank_match else 0
        horses.append(
            {
                "id": f"{race_id}_horse_{row_index:02d}",
                "raceId": race_id,
                "horseName": horse_name,
                "ranking": ranking,
                "finishTime": nullable(item.get("タイム")),
                "jockey": nullable(item.get("騎手")),
                "trainer": nullable(item.get("調教師")),
                "weight": None,
                "weightChange": None,
                "odds": None,
                "popularity": parse_int(item.get("人気")),
                "horseId": None,
            }
        )
    if not horses:
        raise ValueError("no horse result rows")

    payout_values: list[tuple[str, str | None, int]] = []
    for heading in soup.find_all(["h2", "h3"]):
        if "払戻金" not in heading.get_text(" ", strip=True):
            continue
        dl = heading.find_next("dl")
        if dl is not None:
            for dt in dl.find_all("dt", recursive=False):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                bet_type = clean_text(dt.get_text(" ", strip=True))
                value = clean_text(dd.get_text(" ", strip=True))
                amount_match = re.search(r"([\d,]+)円", value)
                if not amount_match:
                    continue
                amount = int(amount_match.group(1).replace(",", ""))
                numbers = re.sub(r"番", "", value[: amount_match.start()])
                numbers = clean_text(numbers) or None
                payout_values.append((bet_type, numbers, amount))
        break

    try:
        payout_table, payout_headers = find_table(soup, {"券種", "組番", "払戻金"})
    except ValueError:
        payout_table = None
        payout_headers = []
    if payout_table is not None:
        for row in payout_table.find_all("tr")[1:]:
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
            if not cells or len(cells) != len(payout_headers):
                continue
            item = dict(zip(payout_headers, cells, strict=True))
            amount_match = re.search(r"([\d,]+)円", item.get("払戻金", ""))
            if not amount_match:
                continue
            amount = int(amount_match.group(1).replace(",", ""))
            numbers = re.sub(r"番", "", clean_text(item.get("組番"))) or None
            payout_values.append((clean_text(item.get("券種")), numbers, amount))

    seen: set[tuple[str, str | None, int]] = set()
    payouts: list[dict[str, Any]] = []
    for bet_type, numbers, amount in payout_values:
        key = (bet_type, numbers, amount)
        if not bet_type or key in seen:
            continue
        seen.add(key)
        payouts.append(
            {
                "id": f"{race_id}_payout_{len(payouts) + 1:02d}",
                "raceId": race_id,
                "betType": bet_type,
                "horseNumbers": numbers,
                "odds": amount / 100.0,
                "payoutAmount": amount,
            }
        )
    if not payouts:
        raise ValueError("no payout rows")

    return ParsedPage(race=race, horses=horses, payouts=payouts)


def fetch_and_parse(url: str) -> ParsedPage:
    return parse_race(url, get_text(url))


def discover_urls() -> list[str]:
    xml = get_text(SITEMAP_URL)
    root = ElementTree.fromstring(xml)
    urls = sorted(
        element.text.strip()
        for element in root.iter()
        if element.tag.endswith("loc") and element.text and RACE_URL_RE.match(element.text.strip())
    )
    source_counts = Counter(parse_url_parts(url).group("source") for url in urls)
    if dict(source_counts) != EXPECTED_SOURCE_COUNTS:
        raise RuntimeError(
            f"unexpected sitemap counts: actual={dict(source_counts)} expected={EXPECTED_SOURCE_COUNTS}"
        )
    return urls


def write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_database(path: Path, races: list[dict[str, Any]], horses: list[dict[str, Any]], payouts: list[dict[str, Any]]) -> dict[str, Any]:
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE races (
                id TEXT PRIMARY KEY NOT NULL,
                date TEXT NOT NULL,
                venue TEXT NOT NULL,
                raceNumber INTEGER NOT NULL,
                raceName TEXT,
                distance INTEGER,
                surface TEXT,
                grade TEXT,
                weather TEXT,
                trackCondition TEXT,
                url TEXT
            );
            CREATE TABLE horses (
                id TEXT PRIMARY KEY NOT NULL,
                raceId TEXT NOT NULL,
                horseName TEXT NOT NULL,
                ranking INTEGER NOT NULL,
                finishTime TEXT,
                jockey TEXT,
                trainer TEXT,
                weight INTEGER,
                weightChange INTEGER,
                odds REAL,
                popularity INTEGER,
                horseId TEXT,
                FOREIGN KEY (raceId) REFERENCES races(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
            CREATE INDEX horses_horseId_idx ON horses(horseId);
            CREATE TABLE payouts (
                id TEXT PRIMARY KEY NOT NULL,
                raceId TEXT NOT NULL,
                betType TEXT NOT NULL,
                horseNumbers TEXT,
                odds REAL NOT NULL,
                payoutAmount INTEGER NOT NULL,
                FOREIGN KEY (raceId) REFERENCES races(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
            """
        )
        race_columns = ["id", "date", "venue", "raceNumber", "raceName", "distance", "surface", "grade", "weather", "trackCondition", "url"]
        horse_columns = ["id", "raceId", "horseName", "ranking", "finishTime", "jockey", "trainer", "weight", "weightChange", "odds", "popularity", "horseId"]
        payout_columns = ["id", "raceId", "betType", "horseNumbers", "odds", "payoutAmount"]
        connection.executemany(
            f"INSERT INTO races ({','.join(race_columns)}) VALUES ({','.join('?' for _ in race_columns)})",
            [[row.get(column) for column in race_columns] for row in races],
        )
        connection.executemany(
            f"INSERT INTO horses ({','.join(horse_columns)}) VALUES ({','.join('?' for _ in horse_columns)})",
            [[row.get(column) for column in horse_columns] for row in horses],
        )
        connection.executemany(
            f"INSERT INTO payouts ({','.join(payout_columns)}) VALUES ({','.join('?' for _ in payout_columns)})",
            [[row.get(column) for column in payout_columns] for row in payouts],
        )
        connection.commit()
        return {
            "integrity_check": connection.execute("PRAGMA integrity_check").fetchone()[0],
            "foreign_key_violations": len(connection.execute("PRAGMA foreign_key_check").fetchall()),
            "counts": {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("races", "horses", "payouts")
            },
        }
    finally:
        connection.close()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/keiba-races-2026-08"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--local-sample", type=Path)
    args = parser.parse_args()

    if args.local_sample:
        parsed = parse_race(
            "https://data.keiba-intelligence.jp/jra/results/2026/08/08/sapporo/8/",
            args.local_sample.read_text(encoding="utf-8"),
        )
        print(json.dumps({"race": parsed.race, "horses": len(parsed.horses), "payouts": len(parsed.payouts)}, ensure_ascii=False, indent=2))
        return 0

    started = time.monotonic()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    urls = discover_urls()
    print(f"discovered race URLs: {len(urls)}", flush=True)

    parsed_pages: list[ParsedPage] = []
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_to_url = {executor.submit(fetch_and_parse, url): url for url in urls}
        for completed, future in enumerate(as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                parsed_pages.append(future.result())
            except Exception as exc:
                failures.append({"url": url, "error": f"{type(exc).__name__}: {exc}"})
                print(f"ERROR {url}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            if completed % 25 == 0 or completed == len(urls):
                print(f"processed: {completed}/{len(urls)} failures={len(failures)}", flush=True)

    parsed_pages.sort(key=lambda item: (item.race["date"], item.race["venue"], item.race["raceNumber"]))
    races = [page.race for page in parsed_pages]
    horses = [horse for page in parsed_pages for horse in page.horses]
    payouts = [payout for page in parsed_pages for payout in page.payouts]
    failure_path = output_dir / "failures.json"
    failure_path.write_text(json.dumps(failures, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if failures:
        raise RuntimeError(f"failed to fetch or parse {len(failures)} race pages; see {failure_path}")
    if len(races) != len(urls):
        raise RuntimeError(f"race count mismatch: parsed={len(races)} urls={len(urls)}")

    race_columns = ["id", "date", "venue", "raceNumber", "raceName", "distance", "surface", "grade", "weather", "trackCondition", "url"]
    horse_columns = ["id", "raceId", "horseName", "ranking", "finishTime", "jockey", "trainer", "weight", "weightChange", "odds", "popularity", "horseId"]
    payout_columns = ["id", "raceId", "betType", "horseNumbers", "odds", "payoutAmount"]
    write_csv(output_dir / "races.csv", race_columns, races)
    write_csv(output_dir / "horses.csv", horse_columns, horses)
    write_csv(output_dir / "payouts.csv", payout_columns, payouts)

    db_path = output_dir / "keiba-races-2026-08.db"
    validation = write_database(db_path, races, horses, payouts)
    if validation["integrity_check"] != "ok" or validation["foreign_key_violations"] != 0:
        raise RuntimeError(f"database validation failed: {validation}")

    source_counts = Counter(race["source"] for race in races)
    venue_counts = Counter(race["venue"] for race in races)
    date_counts = Counter(race["date"] for race in races)
    null_surface = sum(race["surface"] is None for race in races)
    metadata = {
        "period": "2026-08",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": BASE_URL,
        "sitemap": SITEMAP_URL,
        "race_url_count": len(urls),
        "source_counts": dict(sorted(source_counts.items())),
        "venue_counts": dict(sorted(venue_counts.items())),
        "date_counts": dict(sorted(date_counts.items())),
        "counts": {"races": len(races), "horses": len(horses), "payouts": len(payouts)},
        "source_limitations": {
            "surface_missing_races": null_surface,
            "horse_weight_odds_and_ids_unavailable": True,
        },
        "validation": validation,
        "files": {},
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    for path in (output_dir / "races.csv", output_dir / "horses.csv", output_dir / "payouts.csv", db_path, failure_path):
        metadata["files"][path.name] = {"bytes": path.stat().st_size, "sha256": sha256(path)}
    (output_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = "\n".join(
        [
            "# 2026年8月 過去レース取得結果",
            "",
            f"- JRA: {source_counts['jra']:,}レース",
            f"- 南関: {source_counts['nankan']:,}レース",
            f"- 合計: {len(races):,}レース / {len(horses):,}頭 / {len(payouts):,}払戻レコード",
            f"- DB整合性: {validation['integrity_check']}",
            f"- 外部キー違反: {validation['foreign_key_violations']}件",
            f"- DB SHA-256: `{sha256(db_path)}`",
            "",
            "## 対象",
            "",
            "data.keiba-intelligence.jp の正規サイトマップに掲載された2026年8月のJRA・南関競馬結果ページ。",
            "",
            "## 注記",
            "",
            f"- 元ページにコース種別がないレースはsurfaceをNULLで保存: {null_surface:,}件",
            "- 元ページに馬体重・単勝オッズ・馬IDがないため該当列はNULL。",
            "- 払戻は組番ごとに1レコードで保存。oddsは100円当たり払戻額から算出。",
            "",
        ]
    )
    (output_dir / "report.md").write_text(report, encoding="utf-8")
    print(json.dumps(metadata, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
