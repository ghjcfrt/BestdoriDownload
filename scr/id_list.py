"""从 _failures_cn_*.json 中汇总所有失败歌曲 ID。

用法示例：
	python ./scr/id_list.py
	python ./scr/id_list.py --latest
	python ./scr/id_list.py --file output/musiccore/_failures_cn_20260111_033831.json
	python ./scr/id_list.py --glob output/musiccore/_failures_cn_*.json --out failures_ids.txt
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable


def _iter_failure_items(data: Any) -> Iterable[Any]:
	"""Yield candidate failure items from several possible JSON shapes."""
	if isinstance(data, dict):
		failures = data.get("failures")
		if isinstance(failures, list):
			yield from failures
			return
		if isinstance(failures, dict):
			for k, v in failures.items():
				if isinstance(v, dict):
					yield {"_key": k, **v}
				else:
					yield {"_key": k, "value": v}
			return
		# fallback: some formats might be a mapping keyed by id
		for k, v in data.items():
			if k == "failures":
				continue
			if isinstance(v, dict):
				yield {"_key": k, **v}
			else:
				yield {"_key": k, "value": v}
		return

	if isinstance(data, list):
		yield from data


def _extract_id(item: Any) -> str | None:
	if isinstance(item, dict):
		val = item.get("id")
		if val is None:
			val = item.get("score_id")
		if val is None:
			val = item.get("_key")
		if val is None:
			return None
		return str(val)

	# rare case: list of raw ids
	if isinstance(item, (int, str)):
		return str(item)
	return None


def extract_ids_from_file(path: Path) -> list[str]:
	data = json.loads(path.read_text(encoding="utf-8"))
	ids: list[str] = []
	seen: set[str] = set()
	for it in _iter_failure_items(data):
		id_str = _extract_id(it)
		if not id_str:
			continue
		if id_str in seen:
			continue
		seen.add(id_str)
		ids.append(id_str)
	return ids


def _pick_latest(paths: list[Path]) -> Path:
	# Sort by mtime, then name for determinism
	return sorted(paths, key=lambda p: (p.stat().st_mtime, p.name))[-1]


def main() -> int:
	parser = argparse.ArgumentParser(description="从 _failures_cn_*.json 中汇总所有失败歌曲 ID")
	parser.add_argument(
		"--file",
		type=str,
		default=None,
		help="指定单个 failures JSON 文件路径。",
	)
	parser.add_argument(
		"--glob",
		type=str,
		default="output/musiccore/_failures_cn_*.json",
		help="用于搜索 failures JSON 文件的 glob（指定 --file 时忽略）。",
	)
	parser.add_argument(
		"--latest",
		action="store_true",
		help="当 --glob 匹配到多个文件时，只使用最新的一个。",
	)
	parser.add_argument(
		"--out",
		type=str,
		default=None,
		help="可选：输出到文本文件路径（每行一个 id）。",
	)
	args = parser.parse_args()

	if args.file:
		paths = [Path(args.file)]
	else:
		paths = sorted(Path().glob(args.glob))

	if not paths:
		raise SystemExit(f"未找到文件。file={args.file!r}, glob={args.glob!r}")

	if args.latest and len(paths) > 1:
		paths = [_pick_latest(paths)]

	all_ids: list[str] = []
	seen: set[str] = set()
	for p in paths:
		ids = extract_ids_from_file(p)
		for x in ids:
			if x in seen:
				continue
			seen.add(x)
			all_ids.append(x)

	text = "\n".join(all_ids)
	print(f"数量={len(all_ids)}")
	if text:
		print(text)

	if args.out:
		Path(args.out).write_text(text + ("\n" if text else ""), encoding="utf-8")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
