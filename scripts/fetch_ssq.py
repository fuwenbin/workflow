#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# Optional deps
try:
	import requests  # type: ignore
except Exception:
	requests = None  # type: ignore

try:
	from bs4 import BeautifulSoup  # type: ignore
except Exception:
	BeautifulSoup = None  # type: ignore

# Stdlib networking fallback
import urllib.request
import urllib.parse
import gzip
import zlib
import argparse


DEFAULT_OUTPUT = "/workspace/data/ssq_history.csv"
USER_AGENT = (
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
	"(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class DrawRecord:
	issue: str
	red1: str
	red2: str
	red3: str
	red4: str
	red5: str
	red6: str
	blue: str
	date: str

	def to_row(self) -> List[str]:
		return [
			self.issue,
			self.red1,
			self.red2,
			self.red3,
			self.red4,
			self.red5,
			self.red6,
			self.blue,
			self.date,
		]


class HttpClient:
	def __init__(self, timeout_seconds: float = 15.0, max_retries: int = 3, backoff_seconds: float = 1.5):
		if not requests:
			raise RuntimeError("requests is required for HttpClient; use urllib fallback instead")
		self.session = requests.Session()  # type: ignore
		self.session.headers.update({
			"User-Agent": USER_AGENT,
			"Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
			"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
		})
		self.timeout_seconds = timeout_seconds
		self.max_retries = max_retries
		self.backoff_seconds = backoff_seconds

	def get_text(self, url: str, params: Optional[Dict[str, str]] = None, force_encoding: Optional[str] = None) -> str:
		last_exc: Optional[Exception] = None
		for attempt in range(1, self.max_retries + 1):
			try:
				resp = self.session.get(url, params=params, timeout=self.timeout_seconds)  # type: ignore
				resp.raise_for_status()
				if force_encoding:
					resp.encoding = force_encoding
				return resp.text
			except Exception as exc:
				last_exc = exc
				if attempt < self.max_retries:
					time.sleep(self.backoff_seconds * (2 ** (attempt - 1)))
				else:
					raise last_exc

	def get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> dict:
		text = self.get_text(url, params=params)
		try:
			import json
			return json.loads(text)
		except Exception as exc:
			raise RuntimeError(f"Invalid JSON from {url}") from exc


# -------- Utilities --------

def _strip_tags(html_snippet: str) -> str:
	text = re.sub(r"<[^>]+>", "", html_snippet)
	text = text.replace("&nbsp;", " ")
	return text.strip()


def _detect_encoding_from_meta(html_bytes: bytes, default: str = "utf-8") -> str:
	try:
		head = html_bytes[:4096].decode("ascii", errors="ignore")
		m = re.search(r"charset=([\w-]+)", head, flags=re.IGNORECASE)
		if m:
			return m.group(1).lower()
	except Exception:
		pass
	return default


def _decompress_body(data: bytes, content_encoding: str) -> bytes:
	enc = (content_encoding or "").lower().strip()
	if "gzip" in enc:
		try:
			return gzip.decompress(data)
		except Exception:
			pass
	if "deflate" in enc:
		try:
			return zlib.decompress(data, -zlib.MAX_WBITS)
		except Exception:
			try:
				return zlib.decompress(data)
			except Exception:
				pass
	return data


def _urllib_get(url: str, params: Optional[Dict[str, str]] = None, timeout: float = 20.0, force_encoding: Optional[str] = None, extra_headers: Optional[Dict[str, str]] = None) -> str:
	if params:
		qs = urllib.parse.urlencode(params)
		sep = '&' if ('?' in url) else '?'
		url = f"{url}{sep}{qs}"
	headers = {
		"User-Agent": USER_AGENT,
		"Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
		"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
		"Accept-Encoding": "gzip, deflate",
	}
	if extra_headers:
		headers.update(extra_headers)
	req = urllib.request.Request(url, headers=headers)
	with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec - external URL by user request
		data = resp.read()
		content_encoding = resp.info().get("Content-Encoding", "")
		if content_encoding:
			data = _decompress_body(data, content_encoding)
		if force_encoding:
			enc = force_encoding
		else:
			enc = _detect_encoding_from_meta(data, default="utf-8")
		try:
			return data.decode(enc, errors="ignore")
		except Exception:
			# Common Chinese encodings
			for fallback in ("utf-8", "gb18030", "gbk", "latin-1"):
				try:
					return data.decode(fallback, errors="ignore")
				except Exception:
					continue
			raise


# -------- Source A: 500.com HTML --------

def parse_500_history_html(html_text: str) -> List[DrawRecord]:
	"""Parse 500.com SSQ history table rows into DrawRecord list.

	The page `https://datachart.500.com/ssq/history/newinc/history.php?start=03001&end=99999`
	contains a table with id=tdata containing rows of draws.
	"""
	if BeautifulSoup is None:
		return parse_500_history_html_regex(html_text)

	soup = BeautifulSoup(html_text, "html.parser")  # type: ignore
	table = soup.find("table", {"id": "tdata"})
	if not table:
		return []

	records: List[DrawRecord] = []
	for row in table.find_all("tr"):
		cells = row.find_all("td")
		if not cells or len(cells) < 16:
			continue

		issue = cells[0].get_text(strip=True)
		red_nums = [cells[i].get_text(strip=True) for i in range(1, 7)]
		blue = cells[7].get_text(strip=True)
		date_text = cells[15].get_text(strip=True)
		if not issue or not blue or not all(red_nums):
			continue
		records.append(
			DrawRecord(
				issue=issue,
				red1=red_nums[0],
				red2=red_nums[1],
				red3=red_nums[2],
				red4=red_nums[3],
				red5=red_nums[4],
				red6=red_nums[5],
				blue=blue,
				date=date_text,
			)
		)
	return records


def parse_500_history_html_regex(html_text: str) -> List[DrawRecord]:
	# Narrow to the tdata table
	table_match = re.search(r"<table[^>]*id=\"tdata\"[^>]*>([\s\S]*?)</table>", html_text, flags=re.IGNORECASE)
	if not table_match:
		return []
	table_html = table_match.group(1)

	records: List[DrawRecord] = []
	for tr_match in re.finditer(r"<tr[^>]*>([\s\S]*?)</tr>", table_html, flags=re.IGNORECASE):
		tr_html = tr_match.group(1)
		# Extract all TDs as raw innerHTML
		td_values = [
			_strip_tags(m.group(1))
			for m in re.finditer(r"<td[^>]*>([\s\S]*?)</td>", tr_html, flags=re.IGNORECASE)
		]
		if len(td_values) < 16:
			continue
		issue = td_values[0]
		# Expect indices 1..6 red, 7 blue, 15 date
		red_nums = td_values[1:7]
		blue = td_values[7] if len(td_values) > 7 else ""
		date_text = td_values[15] if len(td_values) > 15 else ""
		# Basic validation
		if not (re.fullmatch(r"\d+", issue or "") and len(red_nums) == 6 and all(re.fullmatch(r"\d{1,2}", x or "") for x in red_nums) and re.fullmatch(r"\d{1,2}", blue or "")):
			continue
		records.append(
			DrawRecord(
				issue=issue,
				red1=f"{int(red_nums[0]):02d}",
				red2=f"{int(red_nums[1]):02d}",
				red3=f"{int(red_nums[2]):02d}",
				red4=f"{int(red_nums[3]):02d}",
				red5=f"{int(red_nums[4]):02d}",
				red6=f"{int(red_nums[5]):02d}",
				blue=f"{int(blue):02d}",
				date=date_text,
			)
		)
	return records


def fetch_from_500_with_requests(client: HttpClient, start_issue: str = "03001", end_issue: str = "99999") -> List[DrawRecord]:
	url = "https://datachart.500.com/ssq/history/newinc/history.php"
	params = {"start": start_issue, "end": end_issue}
	text = client.get_text(url, params=params, force_encoding="gb18030")
	return parse_500_history_html(text)


def fetch_from_500_with_urllib(start_issue: str = "03001", end_issue: str = "99999") -> List[DrawRecord]:
	url = "https://datachart.500.com/ssq/history/newinc/history.php"
	params = {"start": start_issue, "end": end_issue}
	text = _urllib_get(
		url,
		params=params,
		force_encoding="gb18030",
		extra_headers={
			"Referer": "https://datachart.500.com/ssq/history/newinc/history.php",
		},
	)
	return parse_500_history_html(text)


# -------- Source B: sporttery JSON (no-deps version) --------

def fetch_from_sporttery_with_urllib(max_pages: int = 200) -> List[DrawRecord]:
	records: List[DrawRecord] = []
	base_url = "https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry"
	for page_no in range(1, max_pages + 1):
		params = {
			"gameNo": "85",
			"provinceId": "0",
			"pageSize": "50",
			"isVerify": "1",
			"pageNo": str(page_no),
		}
		text = _urllib_get(
			base_url,
			params=params,
			extra_headers={
				"Accept": "application/json, text/plain, */*",
				"Origin": "https://www.sporttery.cn",
				"Referer": "https://www.sporttery.cn/",
			},
		)
		try:
			import json
			data = json.loads(text)
		except Exception:
			break
		items = (data.get("value") or {}).get("list") or []
		if not items:
			break
		for it in items:
			issue = str(it.get("lotteryDrawNum") or it.get("issue") or "")
			code = (it.get("lotteryDrawResult") or "").strip()
			open_time = str(it.get("lotteryDrawTime") or it.get("date") or "")
			if not issue or not code:
				continue
			nums = [n for n in re.split(r"[ ,;+]", code) if n]
			if len(nums) >= 7:
				red = nums[:6]
				blue = nums[6]
				records.append(
					DrawRecord(
						issue=issue,
						red1=red[0],
						red2=red[1],
						red3=red[2],
						red4=red[3],
						red5=red[4],
						red6=red[5],
						blue=blue,
						date=open_time,
					)
				)
		# be polite
		time.sleep(0.15)
	return records


# -------- Source C: cwl JSON via urllib (best-effort) --------

def fetch_from_cwl_with_urllib(issue_count: int = 200) -> List[DrawRecord]:
	url = "https://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice"
	params = {"name": "ssq", "issueCount": str(issue_count)}
	text = _urllib_get(url, params=params, extra_headers={"Accept": "application/json"})
	try:
		import json
		data = json.loads(text)
	except Exception:
		return []
	result_list = data.get("result") or data.get("list") or data.get("value") or []
	if isinstance(result_list, dict) and "list" in result_list:
		result_list = result_list["list"]
	records: List[DrawRecord] = []
	for it in result_list:
		issue = str(it.get("code") or it.get("issue") or it.get("lotteryDrawNum") or "")
		reds = it.get("reds") or it.get("red") or ""
		blue = it.get("blue") or it.get("blueBall") or ""
		open_time = it.get("date") or it.get("lotteryDrawTime") or ""
		if isinstance(reds, str):
			red_list = [n for n in re.split(r"[ ,;|+]", reds) if n]
		else:
			red_list = list(reds) if reds else []
		if not (issue and blue and len(red_list) >= 6):
			continue
		records.append(
			DrawRecord(
				issue=issue,
				red1=red_list[0],
				red2=red_list[1],
				red3=red_list[2],
				red4=red_list[3],
				red5=red_list[4],
				red6=red_list[5],
				blue=str(blue),
				date=str(open_time),
			)
		)
	return records


def try_fetch_from_cwl(client: HttpClient, max_pages: int = 200) -> List[DrawRecord]:
	"""Attempt to fetch via known endpoints if available (requires requests)."""
	candidate_urls: List[Tuple[str, Dict[str, str]]] = [
		(
			"https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry",
			{"gameNo": "85", "provinceId": "0", "pageSize": "50", "isVerify": "1", "pageNo": "1"},
		),
		(
			"https://www.cwl.gov.cn/cwl_admin/kjxx/findDrawNotice",
			{"name": "ssq", "issueCount": "100"},
		),
	]

	records: List[DrawRecord] = []
	for base_url, base_params in candidate_urls:
		try:
			if "pageNo" in base_params:
				for page_no in range(1, max_pages + 1):
					params = dict(base_params)
					params["pageNo"] = str(page_no)
					data = client.get_json(base_url, params=params)
					items = data.get("value", {}).get("list", [])
					if not items:
						break
					for it in items:
						issue = str(it.get("lotteryDrawNum") or it.get("issue") or "")
						code = (it.get("lotteryDrawResult") or "").strip()
						open_time = str(it.get("lotteryDrawTime") or it.get("date") or "")
						if not issue or not code:
							continue
						nums = [n for n in re.split(r"[ ,;+]", code) if n]
						if len(nums) >= 7:
							red = nums[:6]
							blue = nums[6]
							records.append(
								DrawRecord(
									issue=issue,
									red1=red[0],
									red2=red[1],
									red3=red[2],
									red4=red[3],
									red5=red[4],
									red6=red[5],
									blue=blue,
									date=str(open_time),
								)
							)
					continue
			# Non-paged
			data = client.get_json(base_url, params=base_params)
			result_list = data.get("result") or data.get("list") or data.get("value") or []
			if isinstance(result_list, dict) and "list" in result_list:
				result_list = result_list["list"]
			for it in result_list:
				issue = str(it.get("code") or it.get("issue") or it.get("lotteryDrawNum") or "")
				reds = it.get("reds") or it.get("red") or ""
				blue = it.get("blue") or it.get("blueBall") or ""
				open_time = it.get("date") or it.get("lotteryDrawTime") or ""
				if isinstance(reds, str):
					red_list = [n for n in re.split(r"[ ,;|+]", reds) if n]
				else:
					red_list = list(reds) if reds else []
				if not (issue and blue and len(red_list) >= 6):
					continue
				records.append(
					DrawRecord(
						issue=issue,
						red1=red_list[0],
						red2=red_list[1],
						red3=red_list[2],
						red4=red_list[3],
						red5=red_list[4],
						red6=red_list[5],
						blue=str(blue),
						date=str(open_time),
					)
				)
		except Exception:
			continue
	return records


def dedupe_and_sort(records: List[DrawRecord]) -> List[DrawRecord]:
	by_issue: Dict[str, DrawRecord] = {}
	for r in records:
		if not r.issue:
			continue
		by_issue[r.issue] = r
	return [by_issue[k] for k in sorted(by_issue.keys())]


def save_to_csv(records: List[DrawRecord], output_path: str = DEFAULT_OUTPUT) -> None:
	os.makedirs(os.path.dirname(output_path), exist_ok=True)
	with open(output_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f)
		writer.writerow(["issue", "red1", "red2", "red3", "red4", "red5", "red6", "blue", "date"])
		for rec in records:
			writer.writerow(rec.to_row())


def run_selftest(output_path: str) -> int:
	# Minimal embedded samples
	sample_sporttery_json = {
		"value": {
			"list": [
				{
					"lotteryDrawNum": "2023001",
					"lotteryDrawResult": "01 02 03 04 05 06+07",
					"lotteryDrawTime": "2023-01-03",
				}
			]
		}
	}
	import json
	items = (sample_sporttery_json.get("value") or {}).get("list") or []
	records: List[DrawRecord] = []
	for it in items:
		issue = str(it.get("lotteryDrawNum") or "")
		code = (it.get("lotteryDrawResult") or "").strip()
		open_time = str(it.get("lotteryDrawTime") or "")
		nums = [n for n in re.split(r"[ ,;+]", code) if n]
		red = nums[:6]
		blue = nums[6]
	records.append(
		DrawRecord(
			issue=issue,
			red1=red[0], red2=red[1], red3=red[2], red4=red[3], red5=red[4], red6=red[5],
			blue=blue,
			date=open_time,
		)
	)
	# Sample 500.com-like HTML with required columns (>=16 tds)
	sample_500_html = (
		"<table id=\"tdata\"><tr>"
		"<td>2023002</td>"
		"<td>08</td><td>09</td><td>10</td><td>11</td><td>12</td><td>13</td>"
		"<td>16</td>"  # blue
		"<td></td><td></td><td></td><td></td><td></td><td></td><td></td>"
		"<td>2023-01-05</td>"
		"</tr></table>"
	)
	records.extend(parse_500_history_html(sample_500_html))
	final_records = dedupe_and_sort(records)
	save_to_csv(final_records, output_path)
	print(f"Selftest saved {len(final_records)} records to {output_path}")
	return 0


def build_arg_parser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(description="Fetch SSQ history and save as CSV")
	p.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path")
	p.add_argument("--source", choices=["all", "sporttery", "500", "cwl"], default="all", help="Which source(s) to use")
	p.add_argument("--start", dest="start_issue", default="03001", help="500.com start issue (e.g., 03001)")
	p.add_argument("--end", dest="end_issue", default="99999", help="500.com end issue")
	p.add_argument("--max-pages", type=int, default=200, help="Sporttery max pages to fetch")
	p.add_argument("--issue-count", type=int, default=200, help="CWL issueCount when using CWL endpoint")
	p.add_argument("--selftest", action="store_true", help="Run offline self test and exit")
	return p


def main() -> int:
	parser = build_arg_parser()
	args = parser.parse_args()

	if args.selftest:
		return run_selftest(args.output)

	all_records: List[DrawRecord] = []

	if args.source in ("all", "sporttery"):
		if requests:
			try:
				client = HttpClient()
				# sporttery with requests path shares logic in try_fetch_from_cwl
				all_records.extend(try_fetch_from_cwl(client, max_pages=args.max_pages))
				time.sleep(0.3)
			except Exception:
				pass
		else:
			try:
				all_records.extend(fetch_from_sporttery_with_urllib(max_pages=args.max_pages))
			except Exception:
				pass

	if args.source in ("all", "500"):
		try:
			if requests:
				client = HttpClient()
				all_records.extend(fetch_from_500_with_requests(client, args.start_issue, args.end_issue))
			else:
				all_records.extend(fetch_from_500_with_urllib(args.start_issue, args.end_issue))
		except Exception:
			pass

	if args.source in ("all", "cwl") and not requests:
		try:
			all_records.extend(fetch_from_cwl_with_urllib(issue_count=args.issue_count))
		except Exception:
			pass

	final_records = dedupe_and_sort(all_records)
	if not final_records:
		print("No records fetched. Please check network or source endpoints.", file=sys.stderr)
		return 2

	save_to_csv(final_records, args.output)
	print(f"Saved {len(final_records)} records to {args.output}")
	return 0


if __name__ == "__main__":
	sys.exit(main())