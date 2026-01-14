import argparse
import json
import pathlib
import socket
import ssl
import time
import unicodedata
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, cast
from urllib.parse import quote, unquote

BASE_INFO = "https://bestdori.com/info/songs/{id}"
SONGS_ALL_API = "https://bestdori.com/api/songs/all.7.json"
ASSETS_BASE = "https://bestdori.com/assets/{region}/musicscore/{bundle}/{filename}"
REGION = "cn"

# 黑名单：Bestdori 的占位/站点名条目（非真实歌曲），不应进入下载流程。
BLACKLIST_TITLES = {"bestdori", "bestdori!"}

# 个别歌曲的 assets 文件名例外：按 song_id 强制使用指定标题。
# 说明：这些例外用于修正 bestdori 资源命名与常规规则不一致的情况。
# 注意：assets 服务器对大小写敏感；例如 song_id=46 的文件名为 littleBusters_*.txt。
# https://bestdori.com/tool/explorer/asset/cn/musicscore
# https://bestdori.com/info/songs/
ASSET_TITLE_OVERRIDES: Dict[int, str] = {
    4:"teardrop",
    5:"sunsunseven",
    12:"hashikimi",
    13:"senkai",
    19:"drepare",
    21:"i-aru",
    24:"miracle",
    25:"kirayume",
    30:"re_birthday",
    40:"hapipa",
    46:"littleBusters",
    51: "singout",
    531: "ave_mujica",
    659: "kiLL_kiSS",
    
    10003:"fangzhou",
}
"""
失败id列表，以供测试
[
]

cn未上线
741, 742, 743, 746, 747, 748, 749, 750
special未上线
56, 186, 189, 344, 413,
"""

# Song IDs (deduped, sorted for stability)
# 遍历 0-750 以及 10001-10010（含端点）
SONG_IDS = list(range(1, 751)) + list(range(10001, 10011))
SONG_IDS = sorted(set(SONG_IDS))

DIFFICULTIES = ["easy", "normal", "hard", "expert", "special"]

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent.parent / "output/musiccore"
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent

# 下载记录（持久化）：保存已成功下载的歌曲 id 与名称等信息
DOWNLOAD_RECORD_PATH = OUTPUT_DIR / f"_downloaded_songs_{REGION}.json"

# 下载失败记录（仅记录“文件内容校验失败”等需要人工关注的情况）
DOWNLOAD_FAILURE_BASENAME = f"_failures_{REGION}"

# 谱面文件应有的开头标记（用于校验下载内容是否正确）
SCORE_HEADER_MARKER = "*---------------------- HEADER FIELD".encode("utf-8")


_SSL_CONTEXT = ssl.create_default_context()


def build_assets_url(*, bundle: str, filename: str) -> str:
    """构造 Bestdori assets URL，并对 filename 做 URL 编码。

    说明：all.7.json 的 jacketImage 偶尔会带内部空格（例如 "236_ aquarion"），
    若不编码，urllib 会把它当作非法 URL，导致 probe/download 失败。
    
    - 仅编码路径段 `filename`，避免影响 bundle/region。
    - safe 中保留常见字符与 '%'，避免对已编码片段二次编码。
    """
    encoded_filename = quote(filename, safe="_.-%")
    return ASSETS_BASE.format(region=REGION, bundle=bundle, filename=encoded_filename)


def _log(msg: str, quiet: bool) -> None:
    if not quiet:
        print(msg)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _now_stamp_for_filename() -> str:
    # 文件名安全时间戳
    return datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")


def resolve_failure_record_path() -> pathlib.Path:
    """失败记录 JSON 路径：

    - 每次运行使用独立文件，文件名带时间戳（便于回溯当次运行的失败与下载链接）。
    """
    return OUTPUT_DIR / f"{DOWNLOAD_FAILURE_BASENAME}_{_now_stamp_for_filename()}.json"


def _resolve_output_dir_from_record(output_dir: Optional[str]) -> Optional[pathlib.Path]:
    if not output_dir:
        return None
    try:
        p = pathlib.Path(output_dir)
        # 记录里尽量用相对路径（相对于项目根目录）
        if not p.is_absolute():
            p = (ROOT_DIR / p).resolve()
        return p
    except Exception:
        return None


JSONDict = Dict[str, Any]


def _strip_utf8_bom(blob: bytes) -> bytes:
    # UTF-8 BOM: EF BB BF
    if blob.startswith(b"\xef\xbb\xbf"):
        return blob[3:]
    return blob


def is_valid_score_bytes(blob: bytes) -> bool:
    """校验谱面内容：只要包含 HEADER FIELD 标记即视为有效。

    说明：个别谱面（例如 id684）在文件开头会有 ASCII 标题块，
    `*---------------------- HEADER FIELD` 不一定是第一条非空行。
    """
    if not blob:
        return False

    content = _strip_utf8_bom(blob)
    return SCORE_HEADER_MARKER in content


def _looks_like_html(blob: bytes) -> bool:
    stripped = (blob or b"").lstrip().lower()
    return stripped.startswith(b"<!doctype html") or stripped.startswith(b"<html")


def _infer_filename_base_from_url(url: str, diff: str) -> Optional[str]:
    """从已成功难度的 url 推断出谱面文件名 base。

    例：
    - url=.../604_toridori_palette_easy.txt, diff=easy -> base="604_toridori_palette"
    """
    try:
        name = url.rsplit("/", 1)[-1]
        name = unquote(name)
        suffix = f"_{diff}.txt"
        if not name.endswith(suffix):
            return None
        base = name[: -len(suffix)]
        return base or None
    except Exception:
        return None


def _infer_bases_from_record_entry(entry: Any, already_saved: set[str]) -> List[str]:
    """从下载记录中推断该曲的 assets base（优先用于补齐缺失难度）。"""
    if not isinstance(entry, dict):
        return []
    diffs = entry.get("difficulties")
    if not isinstance(diffs, dict):
        return []

    bases: List[str] = []
    seen: set[str] = set()
    for diff, meta_any in diffs.items():
        if not isinstance(diff, str) or diff not in already_saved:
            continue
        if not isinstance(meta_any, dict):
            continue
        if meta_any.get("status") != "ok":
            continue
        url = meta_any.get("url")
        if not isinstance(url, str) or not url:
            continue
        base = _infer_filename_base_from_url(url, diff)
        if not base or base in seen:
            continue
        bases.append(base)
        seen.add(base)
    return bases



def is_valid_score_file(path: pathlib.Path) -> bool:
    """仅读取文件开头少量字节判断是否为有效谱面文件。"""
    try:
        with path.open("rb") as f:
            # 部分谱面 HEADER FIELD 不在文件开头（有前置 ASCII 标题块），
            # 因此读取更多字节以避免误判。
            head = f.read(16 * 1024)
        return is_valid_score_bytes(head)
    except Exception:
        return False


def load_failure_record(path: pathlib.Path) -> JSONDict:
    if not path.exists():
        return cast(JSONDict, {
            "schema_version": 1,
            "region": REGION,
            "updated_at": _now_iso(),
            "failures": {},
        })
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("失败记录不是对象(dict)")
    except Exception as e:
        print(f"[Warn] 读取失败记录失败，将新建记录：{path} -> {e}")
        return cast(JSONDict, {
            "schema_version": 1,
            "region": REGION,
            "updated_at": _now_iso(),
            "failures": {},
        })
    data.setdefault("schema_version", 1)
    data.setdefault("region", REGION)
    data.setdefault("failures", {})
    if not isinstance(data.get("failures"), dict):
        data["failures"] = {}
    data["updated_at"] = _now_iso()
    return cast(JSONDict, data)


def save_failure_record(path: pathlib.Path, data: JSONDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data["updated_at"] = _now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


def _record_failure(
    failure_record: JSONDict,
    *,
    song_id: int,
    diff: str,
    title_en: Optional[str],
    title_jp: Optional[str],
    url: Optional[str],
    reason: str,
) -> None:
    failures_any = failure_record.get("failures")
    if not isinstance(failures_any, dict):
        failures_any = {}
        failure_record["failures"] = failures_any
    failures = cast(JSONDict, failures_any)

    key = str(song_id)
    entry_any = failures.get(key)
    if not isinstance(entry_any, dict):
        entry_any = {"id": song_id, "difficulties": {}}
        failures[key] = entry_any
    entry = cast(JSONDict, entry_any)
    entry["id"] = song_id
    entry["title_en"] = title_en
    entry["title_jp"] = title_jp
    entry.setdefault("first_failed_at", _now_iso())

    diffs_any = entry.get("difficulties")
    if not isinstance(diffs_any, dict):
        diffs_any = {}
        entry["difficulties"] = diffs_any
    diffs = cast(JSONDict, diffs_any)

    diffs[diff] = {
        "status": "failed",
        "reason": reason,
        "url": url,
        "failed_at": _now_iso(),
    }
    entry["difficulties"] = diffs
    entry["updated_at"] = _now_iso()


def _mark_difficulty_failed(
    record: JSONDict,
    song_id: int,
    diff: str,
    *,
    title_en: Optional[str],
    title_jp: Optional[str],
    dest_dir: pathlib.Path,
    url: Optional[str],
    reason: str,
) -> None:
    entry = _ensure_song_entry(record, song_id)
    entry["title_en"] = title_en
    entry["title_jp"] = title_jp
    try:
        entry["output_dir"] = str(dest_dir.relative_to(ROOT_DIR))
    except Exception:
        entry["output_dir"] = str(dest_dir)
    entry.setdefault("first_seen_at", _now_iso())

    diffs_any = entry.get("difficulties")
    if not isinstance(diffs_any, dict):
        diffs_any = {}
        entry["difficulties"] = diffs_any
    diffs = cast(JSONDict, diffs_any)

    diffs[diff] = {
        "status": "failed",
        "reason": reason,
        "url": url,
        "failed_at": _now_iso(),
    }
    entry["difficulties"] = diffs
    entry["updated_at"] = _now_iso()


def _handle_invalid_score_file(
    *,
    song_id: int,
    diff: str,
    title_en: Optional[str],
    title_jp: Optional[str],
    dest_dir: pathlib.Path,
    out_path: pathlib.Path,
    download_record: JSONDict,
    failure_record: JSONDict,
    url: Optional[str],
    reason: str,
    allow_record_failure: bool,
) -> None:
    # 删除无效内容
    try:
        if out_path.exists():
            out_path.unlink()
    except Exception:
        pass

    # special：允许删除但不记录失败
    if not allow_record_failure:
        # 保持为 pending，便于下次重试/或缺失即跳过
        entry = _ensure_song_entry(download_record, song_id)
        cast(JSONDict, entry.get("difficulties", {}))[diff] = {
            "status": "pending",
            "note": "内容无效但已忽略",
            "checked_at": _now_iso(),
        }
        entry["updated_at"] = _now_iso()
        return

    _mark_difficulty_failed(
        download_record,
        song_id,
        diff,
        title_en=title_en,
        title_jp=title_jp,
        dest_dir=dest_dir,
        url=url,
        reason=reason,
    )
    _record_failure(
        failure_record,
        song_id=song_id,
        diff=diff,
        title_en=title_en,
        title_jp=title_jp,
        url=url,
        reason=reason,
    )


def _ensure_song_entry(record: JSONDict, song_id: int) -> JSONDict:
    songs_any = record.get("songs")
    if not isinstance(songs_any, dict):
        songs_any = {}
        record["songs"] = songs_any
    songs = cast(JSONDict, songs_any)
    key = str(song_id)
    entry_any = songs.get(key)
    if not isinstance(entry_any, dict):
        entry_any = {"id": song_id}
        songs[key] = entry_any
    entry = cast(JSONDict, entry_any)

    entry.setdefault("id", song_id)
    entry.setdefault("region", REGION)

    diffs_any = entry.get("difficulties")
    if not isinstance(diffs_any, dict):
        diffs_any = {}
        entry["difficulties"] = diffs_any
    diffs = cast(JSONDict, diffs_any)

    # 确保所有难度 key 都存在
    for d in DIFFICULTIES:
        diffs.setdefault(d, {"status": "pending"})
        if not isinstance(diffs.get(d), dict):
            diffs[d] = {"status": "pending"}
        cast(JSONDict, diffs[d]).setdefault("status", "pending")
    return entry


def _mark_difficulty_ok(
    record: JSONDict,
    song_id: int,
    diff: str,
    *,
    title_en: Optional[str],
    title_jp: Optional[str],
    dest_dir: pathlib.Path,
    out_path: pathlib.Path,
    source: str,
    url: Optional[str] = None,
) -> None:
    entry = _ensure_song_entry(record, song_id)
    entry["title_en"] = title_en
    entry["title_jp"] = title_jp
    try:
        entry["output_dir"] = str(dest_dir.relative_to(ROOT_DIR))
    except Exception:
        entry["output_dir"] = str(dest_dir)
    entry.setdefault("first_seen_at", _now_iso())

    # 仅在确认文件存在且非空时才记录 ok
    try:
        if not out_path.exists() or out_path.stat().st_size <= 0:
            return
    except Exception:
        return

    diffs = cast(JSONDict, entry.get("difficulties", {}))
    diffs[diff] = {
        "status": "ok",
        "file": str(out_path.name),
        "bytes": int(out_path.stat().st_size),
        "saved_at": _now_iso(),
        "source": source,
        "url": url,
    }
    entry["difficulties"] = diffs

    expected = _get_expected_difficulties_from_entry(entry)
    ok_count = sum(
        1 for d in expected
        if isinstance(entry.get("difficulties", {}).get(d), dict)
        and entry["difficulties"][d].get("status") == "ok"
    )
    entry["complete"] = ok_count == len(expected)
    entry["updated_at"] = _now_iso()


def _mark_difficulty_not_available(
    record: JSONDict,
    song_id: int,
    diff: str,
    *,
    title_en: Optional[str],
    title_jp: Optional[str],
    dest_dir: pathlib.Path,
    url: Optional[str],
    note: str,
) -> None:
    entry = _ensure_song_entry(record, song_id)
    entry["title_en"] = title_en
    entry["title_jp"] = title_jp
    try:
        entry["output_dir"] = str(dest_dir.relative_to(ROOT_DIR))
    except Exception:
        entry["output_dir"] = str(dest_dir)
    entry.setdefault("first_seen_at", _now_iso())

    diffs_any = entry.get("difficulties")
    if not isinstance(diffs_any, dict):
        diffs_any = {}
        entry["difficulties"] = diffs_any
    diffs = cast(JSONDict, diffs_any)

    diffs[diff] = {
        "status": "n/a",
        "note": note,
        "url": url,
        "checked_at": _now_iso(),
    }
    entry["updated_at"] = _now_iso()


def _validate_and_collect_already_saved(
    record: JSONDict,
    song_id: int,
    dest_dir: pathlib.Path,
    *,
    title_en: Optional[str],
    title_jp: Optional[str],
    failure_record: JSONDict,
    failure_record_path: pathlib.Path,
    expected_difficulties: Optional[List[str]] = None,
    record_special_failures: bool = False,
) -> set[str]:
    """校验 JSON 记录与本地文件，并返回可跳过的难度集合。

    规则：
    - 本地存在且非空：视为可跳过，并写回记录为 ok（source=disk）。
    - JSON 标记 ok 但本地缺失/空文件：回退为 pending（需要重新下载）。
    """
    entry = _ensure_song_entry(record, song_id)
    if title_en is not None:
        entry["title_en"] = title_en
    if title_jp is not None:
        entry["title_jp"] = title_jp
    try:
        entry["output_dir"] = str(dest_dir.relative_to(ROOT_DIR))
    except Exception:
        entry["output_dir"] = str(dest_dir)

    already: set[str] = set()
    dirty = False
    expected_set = set(expected_difficulties) if expected_difficulties else None
    for diff in DIFFICULTIES:
        # 若 API 明确说明该难度不存在：不校验/不下载/不计入失败
        if expected_set is not None and diff not in expected_set:
            diffs_any = entry.get("difficulties")
            if not isinstance(diffs_any, dict):
                diffs_any = {}
                entry["difficulties"] = diffs_any
            diffs = cast(JSONDict, diffs_any)
            prev = diffs.get(diff)
            prev_status = prev.get("status") if isinstance(prev, dict) else None
            if prev_status not in ("ok", "n/a"):
                diffs[diff] = {
                    "status": "n/a",
                    "note": "API未声明存在",
                    "checked_at": _now_iso(),
                }
                dirty = True
            continue

        out_path = dest_dir / f"{diff}.txt"
        exists_nonempty = False
        try:
            exists_nonempty = out_path.exists() and out_path.stat().st_size > 0
        except Exception:
            exists_nonempty = False

        # 若文件存在但内容无效：删除并视为未保存（尽量保留“当时的下载链接”）
        if exists_nonempty and not is_valid_score_file(out_path):
            diffs_any0 = entry.get("difficulties")
            prev_url = None
            if isinstance(diffs_any0, dict):
                prev_meta = diffs_any0.get(diff)
                if isinstance(prev_meta, dict):
                    prev_url = prev_meta.get("url")
            _handle_invalid_score_file(
                song_id=song_id,
                diff=diff,
                title_en=entry.get("title_en"),
                title_jp=entry.get("title_jp"),
                dest_dir=dest_dir,
                out_path=out_path,
                download_record=record,
                failure_record=failure_record,
                url=cast(Optional[str], prev_url) if isinstance(prev_url, str) else None,
                reason="disk文件标头无效",
                allow_record_failure=(diff != "special") or record_special_failures,
            )
            dirty = True
            if diff != "special" or record_special_failures:
                save_failure_record(failure_record_path, failure_record)
            exists_nonempty = False

        diffs = entry.get("difficulties", {})
        dmeta = diffs.get(diff) if isinstance(diffs, dict) else None
        status = dmeta.get("status") if isinstance(dmeta, dict) else None

        if exists_nonempty:
            already.add(diff)
            if status != "ok":
                _mark_difficulty_ok(
                    record,
                    song_id,
                    diff,
                    title_en=entry.get("title_en"),
                    title_jp=entry.get("title_jp"),
                    dest_dir=dest_dir,
                    out_path=out_path,
                    source="disk",
                )
                dirty = True
        else:
            # 记录说 ok，但disk没有：回退为 pending
            if status == "ok":
                entry["difficulties"][diff] = {
                    "status": "pending",
                    "note": "记录为成功但disk缺失",
                    "checked_at": _now_iso(),
                }
                dirty = True

    # 基于“期望难度集合”刷新 complete，避免无 special 的曲目永远 complete=false。
    expected_for_complete = expected_difficulties or _get_expected_difficulties_from_entry(entry)
    try:
        ok_count = sum(
            1 for d in expected_for_complete
            if isinstance(entry.get("difficulties", {}).get(d), dict)
            and entry["difficulties"][d].get("status") == "ok"
        )
        new_complete = ok_count == len(expected_for_complete)
        if entry.get("complete") != new_complete:
            entry["complete"] = new_complete
            entry["updated_at"] = _now_iso()
            dirty = True
    except Exception:
        pass

    if dirty:
        save_download_record(DOWNLOAD_RECORD_PATH, record)
    return already


def load_download_record(path: pathlib.Path) -> JSONDict:
    """读取下载记录 JSON。

    结构示例：
    {
      "schema_version": 1,
      "region": "cn",
      "updated_at": "...",
      "songs": {
        "489": {"id": 489, "title_en": "...", "title_jp": "...", ...}
      }
    }
    """
    if not path.exists():
        return cast(JSONDict, {
            "schema_version": 2,
            "region": REGION,
            "updated_at": _now_iso(),
            "songs": {},
        })

    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("下载记录不是对象(dict)")
    except Exception as e:
        print(f"[Warn] 读取下载记录失败，将新建记录：{path} -> {e}")
        return cast(JSONDict, {
            "schema_version": 2,
            "region": REGION,
            "updated_at": _now_iso(),
            "songs": {},
        })

    data.setdefault("schema_version", 2)
    data.setdefault("region", REGION)
    data.setdefault("songs", {})
    if not isinstance(data.get("songs"), dict):
        data["songs"] = {}

    # 兼容旧结构：saved_difficulties 列表 -> difficulties 映射
    if data.get("schema_version") == 1:
        songs = data.get("songs", {})
        if isinstance(songs, dict):
            for sid, entry in list(songs.items()):
                if not isinstance(entry, dict):
                    continue
                if "difficulties" in entry:
                    continue
                diffs = entry.get("saved_difficulties")
                if isinstance(diffs, list):
                    dmap: Dict[str, dict] = {}
                    for d in DIFFICULTIES:
                        dmap[d] = {"status": "ok" if d in diffs else "pending"}
                    entry["difficulties"] = dmap
        data["schema_version"] = 2

    data["updated_at"] = _now_iso()
    return cast(JSONDict, data)


def save_download_record(path: pathlib.Path, data: JSONDict) -> None:
    """原子写入下载记录，避免中途退出导致 JSON 损坏。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    data["updated_at"] = _now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


def http_get(url: str, timeout: int = 20, *, quiet: bool = False) -> Optional[bytes]:
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
            try:
                return resp.read()
            except (TimeoutError, socket.timeout):
                _log(f"[Timeout] {url}", quiet)
                return None
    except urllib.error.HTTPError as e:
        _log(f"[HTTPError] {url} -> {e.code}", quiet)
    except urllib.error.URLError as e:
        _log(f"[URLError] {url} -> {e}", quiet)
    except (TimeoutError, socket.timeout):
        _log(f"[Timeout] {url}", quiet)
    except Exception as e:
        _log(f"[Error] {url} -> {e}", quiet)
    return None


def http_get_with_retry(url: str, timeout: int = 20, retries: int = 2, backoff: float = 0.6) -> Optional[bytes]:
    """简单重试：用于网络偶发超时/握手超时时提高成功率。"""
    for attempt in range(retries + 1):
        # 只在最终失败时输出错误，避免“第 1 次失败但第 2 次成功”仍打印 [URLError] 的困惑。
        quiet = attempt < retries
        data = http_get(url, timeout=timeout, quiet=quiet)
        if data is not None:
            return data
        if attempt < retries:
            time.sleep(backoff * (2 ** attempt))
    # 最后一次已经按 quiet=False 记录过错误
    return None


_SONGS_ALL_CACHE: Optional[dict] = None


def load_songs_all() -> Optional[dict]:
    """加载 Bestdori songs/all JSON（用于获取中/日/英标题等元数据）。

    注意：bestdori 的 info 页面是 SPA，直接抓 HTML 往往只拿到壳，拿不到“标题”。
    因此这里优先用官方 JSON 数据源。
    """
    global _SONGS_ALL_CACHE
    if _SONGS_ALL_CACHE is not None:
        return _SONGS_ALL_CACHE
    blob = http_get_with_retry(SONGS_ALL_API, timeout=25, retries=2)
    if not blob:
        return None
    try:
        _SONGS_ALL_CACHE = json.loads(blob)
    except Exception as e:
        print(f"[Warn] 解析 songs/all JSON 失败: {e}")
        _SONGS_ALL_CACHE = None
    return _SONGS_ALL_CACHE


def get_titles_from_api(song_id: int) -> Tuple[Optional[str], Optional[str]]:
    """从 songs/all JSON 获取 (罗马音, 日文名)。"""
    data = load_songs_all()
    if not data:
        return None, None
    entry = data.get(str(song_id))
    if not isinstance(entry, dict):
        return None, None
    titles = entry.get("musicTitle")
    if isinstance(titles, list):
        # 常见顺序：JP, EN, TW, CN, ...
        jp = titles[0] if len(titles) >= 1 else None
        en = titles[1] if len(titles) >= 2 else None
        return (en or None), (jp or None)
    return None, None


def get_available_difficulties_from_api(song_id: int) -> Optional[set[str]]:
    """从 songs/all.7.json 的 difficulty 字段推断该曲实际存在的难度。

    约定：difficulty 的 key 通常为 "0".."4"：
    - 0,1,2,3 -> easy,normal,hard,expert
    - 4 -> special

    返回：
    - set[str]: 若能解析到至少一个难度 key
    - None: 无法获取/无法解析（不改变既有行为，仍尝试全部 DIFFICULTIES）
    """
    data = load_songs_all()
    if not data:
        return None
    entry = data.get(str(song_id))
    if not isinstance(entry, dict):
        return None
    diff_any = entry.get("difficulty")
    if not isinstance(diff_any, dict):
        return None

    indices: List[int] = []
    for k in diff_any.keys():
        if isinstance(k, str) and k.isdigit():
            indices.append(int(k))
        elif isinstance(k, int):
            indices.append(int(k))

    if not indices:
        return None

    out: set[str] = set()
    for idx in indices:
        if 0 <= idx < len(DIFFICULTIES):
            out.add(DIFFICULTIES[idx])
    return out or None


def _ordered_difficulties_from_available(available: Optional[set[str]]) -> List[str]:
    if not available:
        return list(DIFFICULTIES)
    return [d for d in DIFFICULTIES if d in available]


def _get_expected_difficulties_from_entry(entry: JSONDict) -> List[str]:
    av = entry.get("available_difficulties")
    if isinstance(av, list) and av:
        out: List[str] = []
        seen: set[str] = set()
        for d in av:
            if not isinstance(d, str):
                continue
            if d not in DIFFICULTIES:
                continue
            if d in seen:
                continue
            out.append(d)
            seen.add(d)
        if out:
            return out
    return list(DIFFICULTIES)


def get_jacket_image_bases_from_api(song_id: int) -> List[str]:
    """从 songs/all JSON 获取 jacketImage 候选。

    Bestdori 的 all.7.json 里通常包含 `jacketImage` 字段，其内容往往就是 assets 文件名的
    “正确 base”（不含难度、不含扩展名），例如：
    - 489: ["489_mayoiuta"]
    - 1: ["yes_bang_dream"]
    - 51: ["051_singout"]

    注意：该字段可能包含多个候选；也可能不准确，因此只作为“优先候选”，仍保留旧的拼接回退逻辑。
    """
    data = load_songs_all()
    if not data:
        return []
    entry = data.get(str(song_id))
    if not isinstance(entry, dict):
        return []
    jacket = entry.get("jacketImage")
    if not isinstance(jacket, list):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for x in jacket:
        if not isinstance(x, str):
            continue
        s = x.strip()
        if not s or s in seen:
            continue
        out.append(s)
        seen.add(s)
    return out


def _probes_from_jacket_base(jacket_base: str, diff: str) -> List[str]:
    """由 jacketImage 的 base 生成谱面文件名候选。"""
    base = jacket_base.strip()
    if not base:
        return []

    probes: List[str] = []
    # 极少数情况下可能已经包含 _{diff}；优先尝试不重复拼 diff。
    suffix = f"_{diff}"
    if base.endswith(suffix):
        probes.append(f"{base}.txt")
    probes.append(f"{base}_{diff}.txt")

    # 去重（保序）
    out: List[str] = []
    seen: set[str] = set()
    for p in probes:
        if p in seen:
            continue
        out.append(p)
        seen.add(p)
    return out


def _normalize_title_for_compare(title: str) -> str:
    """用于比较 EN/JP 是否“相同”的轻量规范化。

    目标：在不改变实际展示含义的前提下，忽略多余空白与 unicode 表现差异。
    """
    t = unicodedata.normalize("NFKC", title).strip()
    # 折叠空白
    t = " ".join(t.split())
    return t


def _normalize_title_for_compare_ignore_punct(title: str) -> str:
    """更宽松的标题比较：忽略空白与标点符号。

    主要用于 assets 文件名推断阶段的“JP 与 EN 是否相同”判断。
    """
    t = unicodedata.normalize("NFKC", title).strip().lower()
    out: List[str] = []
    for ch in t:
        cat = unicodedata.category(ch)
        # P*: punctuation, Z*: separators
        if cat.startswith("P") or cat.startswith("Z"):
            continue
        out.append(ch)
    return "".join(out)


def _is_legacy_no_id_filename(song_id: int) -> bool:
    # 早期 assets 命名：1-50 的谱面文件名不带 "{id}_" 前缀。
    return 1 <= song_id <= 50


def pick_asset_title(song_id: int, title_en: str, title_jp: Optional[str]) -> str:
    """用于生成 assets 文件名的标题。

    规则（按你的要求）：
    - 若日文名与英文名（官方罗马音）相同：
      - 对 1-50：使用完整英文名（忽略标点后相同也算相同）
      - 其他：只取英文名的第一个单词
      例："Imprisoned XII" -> "Imprisoned"；"Georgette Me, Georgette You" -> "Georgette"。
    - 否则使用完整英文名。
    """
    if title_jp:
        try:
            if _is_legacy_no_id_filename(song_id):
                # 1-50：比较时忽略标点；相同则不截断（用完整标题）
                if _normalize_title_for_compare_ignore_punct(title_en) == _normalize_title_for_compare_ignore_punct(title_jp):
                    return title_en
            else:
                if _normalize_title_for_compare(title_en) == _normalize_title_for_compare(title_jp):
                    parts = title_en.strip().split()
                    if parts:
                        first = parts[0].strip(",.;:!?\"'()[]{}")
                        if first:
                            return first
        except Exception:
            # 任何比较失败都回退到原始英文名
            pass
    return title_en


def http_probe_exists(url: str, timeout: int = 10) -> Optional[bool]:
    """用 Range 进行轻量探测：
    - True: 明确存在 (200/206)
    - False: 明确不存在 (404/403 等)
    - None: 网络问题/超时，不作判断
    """
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        # 站点对不存在资源可能返回 200 + HTML；probe 读一点内容做校验
        # 部分谱面 HEADER FIELD 不在文件开头，因此读更多字节降低误判概率
        "Range": "bytes=0-8191",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
            if resp.status not in (200, 206):
                return False
            try:
                head = resp.read() or b""
            except Exception:
                return None

            stripped = head.lstrip()
            if stripped.startswith(b"<!DOCTYPE html") or stripped.startswith(b"<html"):
                return False
            if SCORE_HEADER_MARKER in head:
                return True
            return is_valid_score_bytes(head)
    except urllib.error.HTTPError:
        # 404/403 直接视为不存在
        return False
    except (TimeoutError, socket.timeout):
        return None
    except Exception:
        return None


def local_existing_difficulties(dest_dir: pathlib.Path, expected_difficulties: Optional[List[str]] = None) -> set[str]:
    """仅基于disk判断哪些难度文件已存在且非空。"""
    existing: set[str] = set()
    expected = expected_difficulties or list(DIFFICULTIES)
    for diff in expected:
        p = dest_dir / f"{diff}.txt"
        try:
            if p.exists() and p.stat().st_size > 0 and is_valid_score_file(p):
                existing.add(diff)
        except Exception:
            pass
    return existing


def folder_bucket(song_id: int) -> int:
    """按 10 分桶：1-10->10, 11-20->20, ... 481-490->490。"""
    return ((song_id + 9) // 10) * 10


def _strip_tags(s: str) -> str:
    import re
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def extract_titles_from_info_html(html: bytes) -> Tuple[Optional[str], Optional[str]]:
    """从 info 页面提取 (英文名, 日文名)。

    该页通常在 Title 行同时展示英文和日文：
    例如 "Mayoiuta 迷星叫" / "Deathly Loneliness Attacks 猛独が襲う"。
    """
    import html as _html
    import re
    text = html.decode('utf-8', errors='ignore')

    # 最稳的方式：直接在“可见文本”里匹配渲染出来的表格行。
    # 例如："| Title | Mayoiuta 迷星叫 |" / "| 标题 | ... |"
    # 这能避开 tooltip/div 结构差异。
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = _html.unescape(plain)
    plain = re.sub(r"\s+", " ", plain)
    m = re.search(r"\|\s*(?:Title|标题)\s*\|\s*([^|]+?)\s*\|", plain, re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        # 用 CJK/日文字符作为分界点，切分 EN 与 JP
        cjk = re.search(r"[\u3040-\u30ff\u3400-\u9fff]", raw)
        if cjk:
            idx = cjk.start()
            en = raw[:idx].strip() or None
            jp = raw[idx:].strip() or None
            if en or jp:
                return en, jp
        if raw:
            return raw, None

    # 先从表格的 Title/标题 行抓取（最可靠）
    # 该站点会随 UI 语言显示为 Title 或 标题
    patterns = [
        r"<tr[^>]*>\s*(?:.|\n)*?<t[dh][^>]*>\s*(?:Title|标题)\s*</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*</tr>",
        r"<t[dh][^>]*>\s*(?:Title|标题)\s*</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if not m:
            continue

        cell_html = m.group(1)

        # 优先解析你贴的结构：
        # - 日文在普通 div
        # - 英文在 class 含 fg-grey-light 的 div
        en = None
        jp = None

        m_en = re.search(r"class=\"[^\"]*fg-grey-light[^\"]*\"[^>]*>\s*([^<]+)\s*<", cell_html, re.IGNORECASE)
        if m_en:
            en = _strip_tags(m_en.group(1)) or None

        # 抓取所有 div 的文本（去掉空白），取第一个非英文的作为 jp
        div_texts = []
        for dm in re.finditer(r"<div[^>]*>(.*?)</div>", cell_html, re.IGNORECASE | re.DOTALL):
            t = _strip_tags(dm.group(1))
            if t:
                div_texts.append(t)
        if div_texts:
            if en and len(div_texts) >= 2:
                # 常见：第一个是 jp，第二个是 en
                jp = div_texts[0]
            else:
                # 没抓到 en 时，就按“第一个含非 ASCII”的策略
                for t in div_texts:
                    if any(ord(ch) > 127 for ch in t):
                        jp = t
                        break

        # 最后回退：按第一个非 ASCII 字符切分
        if not en or not jp:
            raw = _strip_tags(cell_html)
            if raw:
                split_at = None
                for i, ch in enumerate(raw):
                    if ord(ch) > 127:
                        split_at = i
                        break
                if split_at is None:
                    en = en or raw
                else:
                    en = en or raw[:split_at].strip()
                    jp = jp or raw[split_at:].strip()

        return (en or None), (jp or None)

    # 回退：<title>
    m = re.search(r"<title>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    if m:
        title = _strip_tags(m.group(1))
        for sep in (" - ", " | "):
            if sep in title:
                title = title.split(sep, 1)[0].strip()
        if title:
            return title, None

    # og:title
    m = re.search(r"property=\"og:title\"\s+content=\"([^\"]+)\"", text, re.IGNORECASE)
    if m:
        t = m.group(1).strip()
        if t:
            return t, None

    # h1
    m = re.search(r"<h1[^>]*>(.*?)</h1>", text, re.IGNORECASE | re.DOTALL)
    if m:
        t = _strip_tags(m.group(1))
        if t:
            return t, None
    return None, None


def slugify_song_title(title: str) -> str:
    """将英文歌名转换为 assets 文件名用的 song_name：小写+下划线，仅保留字母数字。"""
    t = title.strip()
    # 统一unicode（移除音标等）
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.lower()

    # 处理缩写：不要把 don't 变成 don_t（assets 常用 dont）
    for ap in ("'", "’", "‘", "`", "´"):
        t = t.replace(ap, "")

    # 常见符号统一
    t = t.replace("&", " and ")

    out = []
    prev_us = False
    for ch in t:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug


def sanitize_filename(name: str, max_len: int = 120) -> str:
    """Windows 安全文件名：替换非法字符，去除尾部点/空格，并限制长度。"""
    bad = '<>:"/\\|?*'
    cleaned = "".join("_" if ch in bad else ch for ch in name)
    cleaned = cleaned.strip().rstrip(".")
    if not cleaned:
        cleaned = "untitled"
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip().rstrip(".")
    return cleaned


def get_song_output_dir(song_id: int, title_jp: Optional[str], title_en: Optional[str]) -> pathlib.Path:
    """为每首歌创建输出目录：优先使用日文名作为文件夹名。

    若文件夹已存在且标记的 song_id 不同，则自动追加 _{song_id} 避免冲突。
    """
    base_name = sanitize_filename(title_jp or title_en or str(song_id))
    dest_dir = OUTPUT_DIR / base_name

    # 处理：路径已存在但不是目录
    if dest_dir.exists() and not dest_dir.is_dir():
        dest_dir = OUTPUT_DIR / f"{base_name}_{song_id}"

    # 处理：目录同名冲突（不同 song_id）
    marker_name = ".bestdori_song_id"
    if dest_dir.exists() and dest_dir.is_dir():
        marker = dest_dir / marker_name
        if marker.exists():
            try:
                existing = marker.read_text(encoding="utf-8").strip()
            except Exception:
                existing = ""
            if existing and existing != str(song_id):
                dest_dir = OUTPUT_DIR / f"{base_name}_{song_id}"

    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        (dest_dir / marker_name).write_text(str(song_id), encoding="utf-8")
    except Exception:
        pass
    return dest_dir


def get_song_output_dir_no_create(song_id: int, title_jp: Optional[str], title_en: Optional[str]) -> pathlib.Path:
    """仅计算输出目录路径（不创建目录、不写 marker），用于 dry-run。"""
    base_name = sanitize_filename(title_jp or title_en or str(song_id))
    dest_dir = OUTPUT_DIR / base_name
    if dest_dir.exists() and not dest_dir.is_dir():
        dest_dir = OUTPUT_DIR / f"{base_name}_{song_id}"
    return dest_dir


def generate_slug_candidates(slug: str) -> List[str]:
    """基于 slug 生成少量分隔符变体（underscore/hyphen）。

    旧版本在某些情况下可能会把 hyphen 版本放在优先位置，导致在 --no-probe 或网络不稳时
    更容易选中错误文件名（例如 540_silhouette-dance_easy.txt 实际不存在，而 underscore 版本存在）。

    这里强制把 underscore 形式作为第一优先（即便输入已经包含 hyphen），hyphen 仅作为后备。
    """
    raw = slug.strip()
    if not raw:
        return []

    # 规范化：优先使用 underscore 形式
    base_us = raw.replace("-", "_")
    while "__" in base_us:
        base_us = base_us.replace("__", "_")
    base_us = base_us.strip("_")

    base_hy = base_us.replace("_", "-")

    # 去重保持顺序
    seen: set[str] = set()
    out: List[str] = []
    for c in (base_us, base_hy, raw):
        if not c:
            continue
        if c in seen:
            continue
        out.append(c)
        seen.add(c)
    return out


def generate_song_name_candidates(asset_title: str) -> List[str]:
    """为 assets 文件名生成 song_name（slug）候选。

    目标：在保持现有规则优先级的前提下，补充少量“拼接/截断”变体。

    例如："Hare Hare Yukai" ->
    - hare_hare_yukai (现有)
    - harehareyukai
    - hare
    - hare_hare
    - harehare
    """
    base = slugify_song_title(asset_title)
    tokens = [t for t in base.split("_") if t]

    cands: List[str] = []
    cands.append(base)

    # 1) 全拼接：hare_hare_yukai -> harehareyukai
    if tokens:
        joined_all = "".join(tokens)
        if joined_all:
            cands.append(joined_all)

    # 2) 截断：优先前 1/2 个 token 的几种组合
    if len(tokens) >= 1:
        cands.append(tokens[0])
    if len(tokens) >= 2:
        cands.append("_".join(tokens[:2]))
        cands.append("".join(tokens[:2]))

    # 去重保持顺序
    seen: set[str] = set()
    out: List[str] = []
    for c in cands:
        if not c:
            continue
        if c in seen:
            continue
        out.append(c)
        seen.add(c)
    return out


def generate_override_slug_candidates(override: str) -> List[str]:
    """为 ASSET_TITLE_OVERRIDES 生成候选（尽量保留大小写）。

    背景：slugify_song_title() 会强制 lower()，但 Bestdori 的 assets 文件名在少数情况下
    存在大小写敏感的混合写法（例如 littleBusters）。这些 override 通常已经接近最终的
    assets base，因此这里优先把原样字符串作为候选。
    """
    raw = unicodedata.normalize("NFKC", override).strip()
    if not raw:
        return []

    # 用户要求：override 只允许“强制指定原样”，不允许修改大小写或符号（如 '-'）。
    # 因此这里只返回原样候选。
    return [raw]


def download_score(
    song_id: int,
    asset_map: Dict[str, Dict[str, str]],
    dest_dir: pathlib.Path,
    already_saved: Optional[set[str]] = None,
    *,
    title_en: Optional[str] = None,
    title_jp: Optional[str] = None,
    download_record: Optional[JSONDict] = None,
    failure_record: Optional[JSONDict] = None,
    failure_record_path: Optional[pathlib.Path] = None,
    download_timeout: int = 25,
    retries: int = 2,
    expected_difficulties: Optional[List[str]] = None,
    record_special_failures: bool = False,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    saved: Dict[str, str] = {}
    saved_urls: Dict[str, str] = {}

    expected = expected_difficulties or list(DIFFICULTIES)
    for diff in expected:
        if already_saved and diff in already_saved:
            continue
        meta = asset_map.get(diff)
        if not meta:
            print(f"[Skip] {song_id} 缺少 {diff} 的元数据")
            continue
        bundle = meta['bundle']
        filename = meta['filename']
        url = build_assets_url(bundle=bundle, filename=filename)
        content = http_get_with_retry(url, timeout=download_timeout, retries=retries)
        if not content:
            # 下载失败也应保留当时的下载链接，便于回溯/手动重试。
            if download_record is not None and failure_record is not None and failure_record_path is not None:
                if diff == "special" and (not record_special_failures):
                    # special 未在 API 中声明存在：不写入 failures 文件，但仍在下载记录里保留 URL
                    _mark_difficulty_not_available(
                        download_record,
                        song_id,
                        diff,
                        title_en=title_en,
                        title_jp=title_jp,
                        dest_dir=dest_dir,
                        url=url,
                        note="API未声明存在或已忽略",
                    )
                    save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                else:
                    _mark_difficulty_failed(
                        download_record,
                        song_id,
                        diff,
                        title_en=title_en,
                        title_jp=title_jp,
                        dest_dir=dest_dir,
                        url=url,
                        reason="下载失败或文件不存在",
                    )
                    _record_failure(
                        failure_record,
                        song_id=song_id,
                        diff=diff,
                        title_en=title_en,
                        title_jp=title_jp,
                        url=url,
                        reason="下载失败或内容为空",
                    )
                    save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                    save_failure_record(failure_record_path, failure_record)
            if diff == "special" and (not record_special_failures):
                print(f"[Skip] {song_id} {diff} API未声明存在 -> {url}")
            else:
                print(f"[Fail] {song_id} {diff} -> {url}")
            continue

        # 内容校验：文件开头必须有 HEADER FIELD
        if not is_valid_score_bytes(content):
            out_path = dest_dir / f"{diff}.txt"
            if download_record is not None and failure_record is not None and failure_record_path is not None:
                _handle_invalid_score_file(
                    song_id=song_id,
                    diff=diff,
                    title_en=title_en,
                    title_jp=title_jp,
                    dest_dir=dest_dir,
                    out_path=out_path,
                    download_record=download_record,
                    failure_record=failure_record,
                    url=url,
                    reason="下载后无有效标头",
                    allow_record_failure=(diff != "special") or record_special_failures,
                )
                save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                if diff != "special" or record_special_failures:
                    save_failure_record(failure_record_path, failure_record)
            else:
                # 没有记录对象就仅提示（不写盘）
                print(f"[Invalid] {song_id} {diff} -> {url}")
            continue
        out_path = dest_dir / f"{diff}.txt"
        out_path.write_bytes(content)
        saved[diff] = str(out_path)
        saved_urls[diff] = url
        print(f"[OK] {song_id} {diff} -> {out_path}")
        time.sleep(0.2)
    return saved, saved_urls


def main():
    parser = argparse.ArgumentParser(description="下载 Bestdori 歌曲谱面")
    parser.add_argument(
        "ids",
        nargs="*",
        default=[],
        help="歌曲 ID 列表，支持空格分隔或逗号分隔，例如：489 545 或 489,545",
    )
    parser.add_argument(
        "--print-urls",
        action="store_true",
        help="打印解析出来的各难度下载链接（不一定存在）。",
    )
    parser.add_argument(
        "--no-probe",
        action="store_true",
        help="配合 --print-urls 使用：不做网络探测，只输出构造的链接（可能包含不存在的资源）。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅演练：不下载/不写文件/不更新 JSON，可与 --print-urls 组合使用。",
    )
    parser.add_argument(
        "--probe-timeout",
        type=int,
        default=6,
        help="探测资源是否存在的超时时间（秒，Range 请求），默认 6。",
    )
    parser.add_argument(
        "--download-timeout",
        type=int,
        default=25,
        help="下载谱面文件的超时时间（秒），默认 25。",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="网络请求重试次数，默认 2。",
    )
    args = parser.parse_args()

    # dry-run 本身不做网络探测（避免卡在 probe 超时上）
    effective_no_probe = bool(args.no_probe or args.dry_run)

    # 可选：命令行参数传入 ID 列表，例如：python download_bestdori_scores.py 489,545
    ids = SONG_IDS
    if args.ids:
        raw_tokens: List[str] = [str(x).strip() for x in args.ids if str(x).strip()]
        if raw_tokens:
            try:
                parts: List[str] = []
                for tok in raw_tokens:
                    # 允许 token 内部带逗号，例如："489,545" 或 "489, 545"
                    parts.extend([p for p in tok.replace(" ", "").split(",") if p])
                ids = sorted({int(x) for x in parts})
            except Exception:
                print(f"[Warn] 无法解析命令行ID参数：{raw_tokens!r}，将使用内置 SONG_IDS")

    print(f"区域: {REGION}")
    print(f"歌曲ID: {ids}")
    if not args.dry_run:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 加载/创建下载记录（跨多次运行累积）
    download_record = load_download_record(DOWNLOAD_RECORD_PATH)
    failure_record_path = resolve_failure_record_path()
    failure_record = load_failure_record(failure_record_path)

    total_saved = 0
    info_missing: List[int] = []

    for sid in ids:
        # Bestdori 的 songs/all JSON 里常见存在 id=0 的占位/站点名条目（非真实歌曲）。
        # 避免把它当作歌曲去解析/下载。
        if sid == 0:
            print("[Skip] 歌曲ID=0 为占位条目（非真实歌曲）")
            continue
        # 若记录里已有 output_dir，则优先用它（避免再次解析标题导致目录名变化）
        entry0 = download_record.get("songs", {}).get(str(sid)) if isinstance(download_record.get("songs"), dict) else None
        dest_dir0: Optional[pathlib.Path] = None
        if isinstance(entry0, dict):
            dest_dir0 = _resolve_output_dir_from_record(entry0.get("output_dir"))
            if dest_dir0 and not dest_dir0.exists():
                dest_dir0 = None

        bucket = folder_bucket(sid)
        bundle = f"musicscore{bucket}_rip"

        # 优先从记录中取标题，减少网络请求；需要下载缺失难度时再回退到 API
        title_en = entry0.get("title_en") if isinstance(entry0, dict) else None
        title_jp = entry0.get("title_jp") if isinstance(entry0, dict) else None
        if not title_en:
            # 优先从 songs/all JSON 获取标题（info 页是 SPA，HTML 往往只有壳）
            title_en, title_jp = get_titles_from_api(sid)
        if not title_en:
            # 回退：抓 info 页（通常只能拿到 og:title）
            html = http_get_with_retry(BASE_INFO.format(id=sid), timeout=25, retries=2)
            if not html:
                print(f"[Warn] 歌曲 {sid} 无法获取信息页，跳过")
                info_missing.append(sid)
                continue
            title_en2, title_jp2 = extract_titles_from_info_html(html)
            title_en = title_en or title_en2
            title_jp = title_jp or title_jp2
        if not title_en:
            print(f"[Warn] 歌曲 {sid} 未能解析英文歌名，跳过")
            continue

        # 歌名黑名单：过滤掉 Bestdori 的占位/站点名条目。
        # 该条目不会有谱面资源，继续流程只会制造噪音（失败记录/无效下载）。
        if isinstance(title_en, str) and title_en.strip().lower() in BLACKLIST_TITLES:
            print(f"[Skip] {sid} 标题={title_en!r} 为黑名单占位条目")
            continue

        print(f"[Meta] {sid} EN={title_en!r} JP={title_jp!r}")

        # 基于 all.7.json 的 difficulty 字段推断实际存在的难度。
        available_diffs = get_available_difficulties_from_api(sid)
        expected_diffs = _ordered_difficulties_from_available(available_diffs)
        # special：只有当 API 明确声明存在 special 时，才将 special 的失败计入 failures。
        # 若 API 暂时不可用（available_diffs=None），按“可能存在”处理，避免误跳过。
        record_special_failures = True if (available_diffs is None) else ("special" in available_diffs)

        # 写入记录：保存该曲“实际存在的难度”，用于 complete 计算与后续跳过。
        if not args.dry_run:
            entry_expected = _ensure_song_entry(download_record, sid)
            entry_expected["available_difficulties"] = expected_diffs
            try:
                ok_count = sum(
                    1 for d in expected_diffs
                    if isinstance(entry_expected.get("difficulties", {}).get(d), dict)
                    and entry_expected["difficulties"][d].get("status") == "ok"
                )
                entry_expected["complete"] = ok_count == len(expected_diffs)
            except Exception:
                pass
            entry_expected["updated_at"] = _now_iso()
            save_download_record(DOWNLOAD_RECORD_PATH, download_record)

        # 新路径：优先使用 songs/all.7.json 的 jacketImage 作为正确文件名 base。
        # 该字段可能包含多个候选，且不保证一定正确，因此只是优先候选；失败后仍回退旧的拼接逻辑。
        jacket_bases = get_jacket_image_bases_from_api(sid)

        # 生成 song_name 候选：仅使用英文名（官方罗马音）
        # 例外：对少数 song_id 使用强制标题覆盖（ASSET_TITLE_OVERRIDES）。
        # 补充规则：若 JP 与 EN 相同，则只取 EN 的第一个单词（见 pick_asset_title）。
        asset_title = ASSET_TITLE_OVERRIDES.get(sid) or pick_asset_title(sid, title_en, title_jp)

        # 保持现有规则优先：先用 pick_asset_title 的结果。
        # 但当 JP 与 EN 相同导致被截断为“首词”时，把完整英文名也作为后备候选，提升命中率。
        titles_for_assets: List[str] = [asset_title]
        try:
            if title_jp and (not _is_legacy_no_id_filename(sid)):
                if _normalize_title_for_compare(title_en) == _normalize_title_for_compare(title_jp):
                    if asset_title != title_en:
                        titles_for_assets.append(title_en)
        except Exception:
            pass

        slug_candidates: List[str] = []
        seen_slug: set[str] = set()

        # 常规候选：从标题 slugify（会 lower），作为回退。
        for t in titles_for_assets:
            for s in generate_song_name_candidates(t):
                if not s or s in seen_slug:
                    continue
                slug_candidates.append(s)
                seen_slug.add(s)

        if not slug_candidates or not slug_candidates[0]:
            print(f"[Warn] 歌曲 {sid} 无法生成 song_name 候选，跳过")
            continue

        # 输出目录：若记录已有则复用，否则按标题生成
        if dest_dir0 is not None:
            dest_dir = dest_dir0
        else:
            dest_dir = (
                get_song_output_dir_no_create(sid, title_jp, title_en)
                if args.dry_run
                else get_song_output_dir(sid, title_jp, title_en)
            )

        # 校验记录与disk：对已存在的难度直接跳过
        if args.dry_run:
            already_saved = local_existing_difficulties(dest_dir, expected_diffs)
        else:
            already_saved = _validate_and_collect_already_saved(
                download_record,
                sid,
                dest_dir,
                title_en=title_en,
                title_jp=title_jp,
                failure_record=failure_record,
                failure_record_path=failure_record_path,
                expected_difficulties=expected_diffs,
                record_special_failures=record_special_failures,
            )

        # 注意：即使本地全存在，若 --print-urls 也应继续解析并打印链接
        if already_saved == set(expected_diffs) and not args.print_urls:
            print(f"[Skip] {sid} 所有难度文件均已存在")
            continue

        asset_map: Dict[str, Dict[str, str]] = {}
        cached_blobs: Dict[str, bytes] = {}

        diffs_to_resolve = expected_diffs if args.print_urls else [d for d in expected_diffs if d not in already_saved]
        # special 特例：即使 API 未声明 special，也至少尝试一次；若第一次失败，再用 all.7.json 判断
        # special 是否真实存在：不存在则直接跳过（不报 Fail、不写 failures），存在则继续尝试其他候选。
        runtime_expected_diffs = list(expected_diffs)
        if (not args.print_urls) and ("special" not in diffs_to_resolve) and ("special" not in already_saved):
            diffs_to_resolve.append("special")
            runtime_expected_diffs.append("special")

        for diff in diffs_to_resolve:
            filename = None
            last_tried_url: Optional[str] = None
            inferred_bases = _infer_bases_from_record_entry(entry0, already_saved)
            probe_candidates: List[Tuple[str, str]] = []  # (filename, source)

            # - inferred：若该曲已有其他难度成功，则复用其 base 构造缺失难度（最高优先级）
            for b in inferred_bases:
                probe_candidates.append((f"{b}_{diff}.txt", "inferred"))

            # 0) 若存在 override：按原样优先尝试（保留大小写与符号，不做任何变体）
            override_raw = ASSET_TITLE_OVERRIDES.get(sid)
            if isinstance(override_raw, str) and override_raw.strip():
                for ov in generate_override_slug_candidates(override_raw):
                    # 允许 override 已经包含 _{diff}
                    suffix = f"_{diff}"
                    if ov.endswith(suffix):
                        probe_candidates.append((f"{ov}.txt", "override"))
                    if _is_legacy_no_id_filename(sid):
                        probe_candidates.append((f"{ov}_{diff}.txt", "override"))
                    else:
                        probe_candidates.append((f"{sid}_{ov}_{diff}.txt", "override"))

            # 1) 再用 jacketImage 提供的 base（多候选，逐个尝试；同样保留原样）
            for jb in jacket_bases:
                for p in _probes_from_jacket_base(jb, diff):
                    probe_candidates.append((p, "jacket"))

            # 2) 回退：沿用既有“根据标题生成 slug”的拼接规则（会 lower，并生成少量分隔符变体）
            for slug in slug_candidates:
                for cand in generate_slug_candidates(slug):
                    if _is_legacy_no_id_filename(sid):
                        probe_candidates.append((f"{cand}_{diff}.txt", "slug"))
                    else:
                        probe_candidates.append((f"{sid}_{cand}_{diff}.txt", "slug"))

            # 去重（保序），避免重复探测/下载；同名文件保留“最先出现”的来源标签
            deduped: List[Tuple[str, str]] = []
            seen_probe: set[str] = set()
            for p, src in probe_candidates:
                if p in seen_probe:
                    continue
                deduped.append((p, src))
                seen_probe.add(p)
            probe_candidates = deduped

            skipped_special_due_to_api = False
            for probe, src in probe_candidates:
                url = build_assets_url(bundle=bundle, filename=probe)
                last_tried_url = url

                if effective_no_probe:
                    # 不进行网络探测：直接用构造的 URL（速度最快，但可能不存在）
                    filename = probe
                    asset_map[diff] = {"bundle": bundle, "filename": filename}
                    break

                if args.dry_run or args.print_urls:
                    verdict = http_probe_exists(url, timeout=args.probe_timeout)
                    if verdict is True:
                        filename = probe
                        asset_map[diff] = {"bundle": bundle, "filename": filename}
                        break
                    # 第一次失败就检查 special 是否存在：若不存在直接跳过
                    if diff == "special" and (not record_special_failures) and verdict is False:
                        skipped_special_due_to_api = True
                        break
                else:
                    # 高置信候选（inferred/jacket/override）：即使 probe 判定“不存在”，也会尝试下载一次，
                    # 避免 Bestdori 偶发返回 200+HTML（被误判为不存在）导致“单难度缺失”。
                    high_confidence = src in {"inferred", "jacket", "override"}
                    verdict = http_probe_exists(url, timeout=args.probe_timeout)
                    blob: Optional[bytes]
                    if verdict is True or verdict is None:
                        blob = http_get_with_retry(url, timeout=args.download_timeout, retries=args.retries)
                    else:
                        blob = http_get_with_retry(url, timeout=args.download_timeout, retries=args.retries) if high_confidence else None

                    # 若下载到了 HTML（站点壳/错误页），对高置信候选做额外重试，不直接切换命名规则。
                    if blob is not None and (not is_valid_score_bytes(blob)) and _looks_like_html(blob) and high_confidence:
                        for attempt in range(2):
                            time.sleep(0.8 * (2 ** attempt))
                            blob2 = http_get_with_retry(url, timeout=args.download_timeout, retries=args.retries)
                            if blob2 is None:
                                continue
                            blob = blob2
                            if is_valid_score_bytes(blob):
                                break

                    if blob is not None and is_valid_score_bytes(blob):
                        # 这里先缓存内容，避免后面 download_score 再下载一遍
                        filename = probe
                        asset_map[diff] = {"bundle": bundle, "filename": filename}
                        cached_blobs[diff] = blob
                        break
                    if diff == "special" and (not record_special_failures):
                        # 第一次尝试（探测/下载）失败后，API 未声明 special：直接跳过
                        skipped_special_due_to_api = True
                        break

            if diff == "special" and skipped_special_due_to_api:
                if (not args.dry_run) and (not args.print_urls):
                    _mark_difficulty_not_available(
                        download_record,
                        sid,
                        diff,
                        title_en=title_en,
                        title_jp=title_jp,
                        dest_dir=dest_dir,
                        url=last_tried_url if isinstance(last_tried_url, str) else None,
                        note="API未声明存在",
                    )
                    save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                continue

            if filename:
                # 若上面没有缓存内容，则只记录文件名
                asset_map.setdefault(diff, {"bundle": bundle, "filename": filename})
            else:
                # 只有在“真实下载路径”下，才在所有候选方案都失败后写入失败记录。
                # print-urls / dry-run 只能基于 HTTP 状态探测，无法校验内容标头。
                if (not args.dry_run) and (not args.print_urls) and (not effective_no_probe):
                    if diff == "special" and (not record_special_failures):
                        # API 未声明存在 special：视为不适用，不写 failures，也不打印 Fail。
                        _mark_difficulty_not_available(
                            download_record,
                            sid,
                            diff,
                            title_en=title_en,
                            title_jp=title_jp,
                            dest_dir=dest_dir,
                            url=last_tried_url if isinstance(last_tried_url, str) else None,
                            note="API未声明存在",
                        )
                        save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                        continue
                    else:
                        _mark_difficulty_failed(
                            download_record,
                            sid,
                            diff,
                            title_en=title_en,
                            title_jp=title_jp,
                            dest_dir=dest_dir,
                            url=last_tried_url,
                            reason="所有构筑方案均无有效标头",
                        )
                        _record_failure(
                            failure_record,
                            song_id=sid,
                            diff=diff,
                            title_en=title_en,
                            title_jp=title_jp,
                            url=last_tried_url,
                            reason="所有候选均未获得有效标头",
                        )
                        save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                        save_failure_record(failure_record_path, failure_record)
                    print(f"[Fail] {sid} {diff} 所有候选均未获得有效标头")

        if not asset_map:
            print(f"[Warn] 歌曲 {sid} info无法解析")
            continue

        # 可选：打印将要下载的 URL（不会写文件）
        if args.print_urls:
            for diff in expected_diffs:
                meta = asset_map.get(diff)
                if not meta:
                    continue
                url = build_assets_url(bundle=meta["bundle"], filename=meta["filename"])
                tag = "URL" if not effective_no_probe else "URL?"
                print(f"[{tag}] {sid} {diff} -> {url}")
            if args.dry_run:
                # dry-run 时仅打印探测到的链接，不下载不写入
                continue

        # 若前面探测阶段缓存了内容，则直接写文件，避免重复下载
        pre_saved: Dict[str, str] = {}
        pre_saved_urls: Dict[str, str] = {}
        for diff in expected_diffs:
            if diff in already_saved:
                continue
            cached = cached_blobs.get(diff)
            if cached is not None:
                # cached 内容也要校验
                if not is_valid_score_bytes(cached):
                    out_path = dest_dir / f"{diff}.txt"
                    meta0 = asset_map.get(diff)
                    url0 = None
                    if isinstance(meta0, dict):
                        b0 = meta0.get("bundle")
                        f0 = meta0.get("filename")
                        if isinstance(b0, str) and isinstance(f0, str):
                            url0 = build_assets_url(bundle=b0, filename=f0)
                    _handle_invalid_score_file(
                        song_id=sid,
                        diff=diff,
                        title_en=title_en,
                        title_jp=title_jp,
                        dest_dir=dest_dir,
                        out_path=out_path,
                        download_record=download_record,
                        failure_record=failure_record,
                        url=url0 if isinstance(url0, str) else None,
                        reason="缓存内容标头无效",
                        allow_record_failure=(diff != "special") or record_special_failures,
                    )
                    save_download_record(DOWNLOAD_RECORD_PATH, download_record)
                    if diff != "special" or record_special_failures:
                        save_failure_record(failure_record_path, failure_record)
                    continue
                out_path = dest_dir / f"{diff}.txt"
                out_path.write_bytes(cached)
                pre_saved[diff] = str(out_path)
                meta = asset_map.get(diff)
                if isinstance(meta, dict):
                    try:
                        pre_saved_urls[diff] = build_assets_url(bundle=meta["bundle"], filename=meta["filename"])
                    except Exception:
                        pass
                print(f"[OK] {sid} {diff}（缓存） -> {out_path}")

                _mark_difficulty_ok(
                    download_record,
                    sid,
                    diff,
                    title_en=title_en,
                    title_jp=title_jp,
                    dest_dir=dest_dir,
                    out_path=out_path,
                    source="download",
                    url=pre_saved_urls.get(diff),
                )
                save_download_record(DOWNLOAD_RECORD_PATH, download_record)

        # 对未缓存的难度再走正常下载
        saved = pre_saved
        saved_urls = dict(pre_saved_urls)
        if len(saved) < len(asset_map):
            saved2, saved2_urls = download_score(
                sid,
                asset_map,
                dest_dir,
                already_saved=set(pre_saved.keys()) | already_saved,
                title_en=title_en,
                title_jp=title_jp,
                download_record=download_record,
                failure_record=failure_record,
                failure_record_path=failure_record_path,
                download_timeout=args.download_timeout,
                retries=args.retries,
                expected_difficulties=runtime_expected_diffs,
                record_special_failures=record_special_failures,
            )
            saved.update(saved2)
            saved_urls.update(saved2_urls)
        total_saved += len(saved)

        # 只在确认文件已写入且存在时，逐难度更新记录
        if saved:
            for diff, p in saved.items():
                out_path = pathlib.Path(p)
                _mark_difficulty_ok(
                    download_record,
                    sid,
                    diff,
                    title_en=title_en,
                    title_jp=title_jp,
                    dest_dir=dest_dir,
                    out_path=out_path,
                    source="download",
                    url=saved_urls.get(diff),
                )
            save_download_record(DOWNLOAD_RECORD_PATH, download_record)

    print(f"\n下载完成，共保存 {total_saved} 个难度文件。")
    # 1) info 页面不可用（仅当 songs/all 也无法提供标题时才会尝试 info）
    if info_missing:
        info_ids = sorted({int(x) for x in info_missing})
        print(f"info 不存在/不可用的 id：{info_ids}")
    else:
        print("info 不存在/不可用的 id：[]")

    # 2) 本次运行写入 failures 文件的 id（special 仅在 API 声明存在时才写入 failures）
    failures_any = failure_record.get("failures") if isinstance(failure_record, dict) else None
    failure_ids: List[int] = []
    if isinstance(failures_any, dict):
        for k in failures_any.keys():
            try:
                failure_ids.append(int(str(k)))
            except Exception:
                continue
    failure_ids = sorted(set(failure_ids))
    print(f"写入 failures 的 id：{failure_ids}")
    if failure_ids:
        print(f"failures 文件：{failure_record_path}")
    if args.dry_run:
        print("dry-run：未写入文件，未更新下载记录 JSON")
    else:
        print(f"下载记录已更新：{DOWNLOAD_RECORD_PATH}")


if __name__ == "__main__":
    main()
