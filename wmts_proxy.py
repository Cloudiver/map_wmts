#!/usr/bin/env python3
import collections
import json
import os
from pathlib import Path
import sqlite3
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from xml.sax.saxutils import escape as xml_escape


DEFAULT_PORT = 8787
LAYER_NAME = "authorized_tiles"
MATRIX_SET = "GoogleMapsCompatible"
MATRIX_MIN = int(os.getenv("WMTS_MIN_ZOOM", "0"))
MATRIX_MAX = int(os.getenv("WMTS_MAX_ZOOM", "22"))
UPSTREAM_TEMPLATE = os.getenv("UPSTREAM_TILE_URL_TEMPLATE", "").strip()
WMTS_LAYERS_JSON = os.getenv("WMTS_LAYERS_JSON", "").strip()
UPSTREAM_HEADERS = os.getenv("UPSTREAM_HEADERS_JSON", "").strip()
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "20"))
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "1").strip() != "0"
CACHE_MAX_ITEMS = int(os.getenv("CACHE_MAX_ITEMS", "3000"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))
# 负缓存 TTL：404 等空瓦片使用更短的过期时间，避免长期占用缓存槽位
NEG_CACHE_TTL_SECONDS = int(os.getenv("NEG_CACHE_TTL_SECONDS", "60"))
WMTS_STRICT_MODE = os.getenv("WMTS_STRICT_MODE", "0").strip() != "0"
DISK_CACHE_ENABLED = os.getenv("DISK_CACHE_ENABLED", "1").strip() != "0"
DISK_CACHE_DIR = os.getenv("DISK_CACHE_DIR", ".wmts_cache").strip()
DISK_CACHE_MAX_FILES = int(os.getenv("DISK_CACHE_MAX_FILES", "20000"))
REQUEST_LOG_ENABLED = os.getenv("REQUEST_LOG_ENABLED", "1").strip() != "0"
USE_REQUESTS_SESSION = os.getenv("USE_REQUESTS_SESSION", "1").strip() != "0"
REQUESTS_POOL_MAXSIZE = int(os.getenv("REQUESTS_POOL_MAXSIZE", "64"))
REQUESTS_POOL_CONNECTIONS = int(os.getenv("REQUESTS_POOL_CONNECTIONS", "64"))

try:
    import requests
except Exception:
    requests = None


def _make_transparent_png() -> bytes:
    """生成最小的 1×1 全透明 RGBA PNG，用于负缓存（空瓦片占位）。"""

    def _chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)

    # IHDR: width=1, height=1, bit_depth=8, color_type=6(RGBA), compression=0, filter=0, interlace=0
    ihdr = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)
    # IDAT: filter_byte=0 + RGBA(0,0,0,0)
    raw = b"\x00\x00\x00\x00\x00"
    return (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", ihdr)
        + _chunk(b"IDAT", zlib.compress(raw, level=9))
        + _chunk(b"IEND", b"")
    )


TRANSPARENT_TILE_PNG = _make_transparent_png()


def parse_headers(raw: str) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("UPSTREAM_HEADERS_JSON must be a JSON object")
        return {str(k): str(v) for k, v in data.items()}
    except Exception as exc:
        raise ValueError(f"Invalid UPSTREAM_HEADERS_JSON: {exc}") from exc


def qv(query: dict, key: str, default: str = "") -> str:
    return query.get(key, [default])[0]


def cache_max_age(entry: dict, now_ts: int) -> int:
    base_ttl = NEG_CACHE_TTL_SECONDS if entry.get("neg") else CACHE_TTL_SECONDS
    remaining = int(entry.get("exp", now_ts) - now_ts)
    return max(0, min(base_ttl, remaining))


def parse_tile_int(value: str) -> int:
    value = value.strip()
    if ":" in value:
        value = value.rsplit(":", 1)[-1]
    return int(value)


def split_cache_key(key: tuple) -> tuple:
    if len(key) == 4:
        layer, z, x, y = key
        return str(layer), int(z), int(x), int(y)
    if len(key) == 3:
        z, x, y = key
        return "", int(z), int(x), int(y)
    raise ValueError(f"Unsupported cache key: {key!r}")


def parse_layer_configs(raw: str, fallback_template: str) -> dict:
    if raw:
        try:
            data = json.loads(raw)
        except Exception as exc:
            raise ValueError(f"Invalid WMTS_LAYERS_JSON: {exc}") from exc
        if not isinstance(data, list):
            raise ValueError("WMTS_LAYERS_JSON must be a JSON array")
    else:
        if not fallback_template:
            raise ValueError("UPSTREAM_TILE_URL_TEMPLATE or WMTS_LAYERS_JSON is required")
        data = [
            {
                "id": LAYER_NAME,
                "title": "Authorized Tile Layer",
                "template": fallback_template,
                "format": "image/png",
            }
        ]

    layers = {}
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("Each WMTS_LAYERS_JSON item must be a JSON object")
        layer_id = str(item.get("id", "")).strip()
        template = str(item.get("template", "")).strip()
        # Support both custom {y_inv} and common {-y} placeholder.
        template = template.replace("{-y}", "{y_inv}")
        if not layer_id:
            raise ValueError("Layer id is required")
        if not template:
            raise ValueError(f"Layer template is required for id={layer_id}")
        title = str(item.get("title", layer_id)).strip() or layer_id
        img_format = str(item.get("format", "image/png")).strip() or "image/png"
        layers[layer_id] = {
            "id": layer_id,
            "title": title,
            "template": template,
            "format": img_format,
        }
    return layers


LAYER_CONFIGS = parse_layer_configs(WMTS_LAYERS_JSON, UPSTREAM_TEMPLATE)
DEFAULT_LAYER_ID = next(iter(LAYER_CONFIGS))


def matrix_set_xml() -> str:
    entries = []
    for z in range(MATRIX_MIN, MATRIX_MAX + 1):
        scale = 559082264.0287178 / (2 ** z)
        matrix_width = 2 ** z
        matrix_height = 2 ** z
        entries.append(
            f"""
      <TileMatrix>
        <ows:Identifier>{z}</ows:Identifier>
        <ScaleDenominator>{scale}</ScaleDenominator>
        <TopLeftCorner>-20037508.3427892 20037508.3427892</TopLeftCorner>
        <TileWidth>256</TileWidth>
        <TileHeight>256</TileHeight>
        <MatrixWidth>{matrix_width}</MatrixWidth>
        <MatrixHeight>{matrix_height}</MatrixHeight>
      </TileMatrix>"""
        )
    return "".join(entries)


def capabilities_xml(base_url: str) -> str:
    layer_blocks = []
    for layer in LAYER_CONFIGS.values():
        layer_id = xml_escape(layer["id"], {'"': "&quot;"})
        title = xml_escape(layer["title"], {'"': "&quot;"})
        img_format = xml_escape(layer["format"], {'"': "&quot;"})
        template = (
            f"{base_url}/wmts?SERVICE=WMTS&amp;REQUEST=GetTile&amp;VERSION=1.0.0"
            f"&amp;LAYER={layer_id}&amp;STYLE=default&amp;TILEMATRIXSET={MATRIX_SET}"
            "&amp;TILEMATRIX={TileMatrix}&amp;TILEROW={TileRow}&amp;TILECOL={TileCol}"
        )
        layer_blocks.append(
            f"""
    <Layer>
      <ows:Title>{title}</ows:Title>
      <ows:Identifier>{layer_id}</ows:Identifier>
      <Style isDefault="true">
        <ows:Identifier>default</ows:Identifier>
      </Style>
      <Format>{img_format}</Format>
      <TileMatrixSetLink>
        <TileMatrixSet>{MATRIX_SET}</TileMatrixSet>
      </TileMatrixSetLink>
      <ResourceURL format="{img_format}" resourceType="tile"
        template="{template}" />
    </Layer>"""
        )

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Capabilities xmlns="http://www.opengis.net/wmts/1.0"
  xmlns:ows="http://www.opengis.net/ows/1.1"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/wmts/1.0
  http://schemas.opengis.net/wmts/1.0/wmtsGetCapabilities_response.xsd"
  version="1.0.0">
  <ows:ServiceIdentification>
    <ows:Title>Local WMTS Proxy</ows:Title>
    <ows:ServiceType>OGC WMTS</ows:ServiceType>
    <ows:ServiceTypeVersion>1.0.0</ows:ServiceTypeVersion>
  </ows:ServiceIdentification>
  <Contents>{''.join(layer_blocks)}
    <TileMatrixSet>
      <ows:Identifier>{MATRIX_SET}</ows:Identifier>
      <ows:SupportedCRS>urn:ogc:def:crs:EPSG::3857</ows:SupportedCRS>{matrix_set_xml()}
    </TileMatrixSet>
  </Contents>
  <ServiceMetadataURL xlink:href="{base_url}/wmts?SERVICE=WMTS&amp;REQUEST=GetCapabilities"/>
</Capabilities>
"""


class SQLiteDiskCache:
    def __init__(self, base_dir: str, max_items: int):
        self.base_dir = Path(base_dir)
        self.max_items = max_items
        self.lock = threading.Lock()
        self.write_count = 0
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.base_dir / "tiles_cache.sqlite3"
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # WAL 模式：读写并发，高并发下写操作不再阻塞读请求
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA wal_autocheckpoint=1000")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tiles_v2 (
                layer TEXT NOT NULL,
                z INTEGER NOT NULL,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                exp INTEGER NOT NULL,
                ctype TEXT NOT NULL,
                body BLOB NOT NULL,
                atime INTEGER NOT NULL,
                PRIMARY KEY (layer, z, x, y)
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_v2_exp ON tiles_v2(exp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_v2_atime ON tiles_v2(atime)")
        self.conn.commit()

    def get(self, key: tuple, now_ts: int):
        layer, z, x, y = split_cache_key(key)
        row = self.conn.execute(
            "SELECT exp, ctype, body, atime FROM tiles_v2 WHERE layer=? AND z=? AND x=? AND y=?",
            (layer, z, x, y),
        ).fetchone()
        if row is None:
            return None
        exp, ctype, body, atime = int(row[0]), str(row[1]), row[2], int(row[3])
        if exp <= now_ts:
            self.conn.execute("DELETE FROM tiles_v2 WHERE layer=? AND z=? AND x=? AND y=?", (layer, z, x, y))
            self.conn.commit()
            return None
        # 懒更新 atime：仅当距上次记录超过 TTL/4 时才写库，避免每次读都触发写放大
        if now_ts - atime > max(CACHE_TTL_SECONDS // 4, 60):
            self.conn.execute(
                "UPDATE tiles_v2 SET atime=? WHERE layer=? AND z=? AND x=? AND y=?",
                (now_ts, layer, z, x, y),
            )
            self.conn.commit()
        return {"body": body, "ctype": ctype, "exp": exp}

    def set(self, key: tuple, entry: dict, now_ts: int):
        layer, z, x, y = split_cache_key(key)
        self.conn.execute(
            """
            INSERT INTO tiles_v2 (layer, z, x, y, exp, ctype, body, atime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(layer, z, x, y) DO UPDATE SET
                exp=excluded.exp,
                ctype=excluded.ctype,
                body=excluded.body,
                atime=excluded.atime
            """,
            (layer, z, x, y, int(entry["exp"]), str(entry["ctype"]), entry["body"], now_ts),
        )
        self.conn.commit()
        # 每 50 次写才 prune 一次，与 FileDiskCache 的节流策略对齐
        self.write_count += 1
        if self.write_count % 50 == 0:
            self.prune_if_needed(now_ts)

    def count_items(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM tiles_v2").fetchone()
        return int(row[0]) if row else 0

    def prune_if_needed(self, now_ts: int):
        self.conn.execute("DELETE FROM tiles_v2 WHERE exp <= ?", (now_ts,))
        over = self.count_items() - self.max_items
        if over > 0:
            self.conn.execute(
                """
                DELETE FROM tiles_v2
                WHERE rowid IN (
                    SELECT rowid FROM tiles_v2
                    ORDER BY atime ASC
                    LIMIT ?
                )
                """,
                (over,),
            )
        self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class FileDiskCache:
    def __init__(self, base_dir: str, max_items: int):
        self.base_dir = Path(base_dir)
        self.max_items = max_items
        self.lock = threading.Lock()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.writes = 0

    @staticmethod
    def _name(key: tuple) -> str:
        layer, z, x, y = split_cache_key(key)
        safe_layer = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in layer)
        return f"{safe_layer}_{z}_{x}_{y}" if safe_layer else f"{z}_{x}_{y}"

    def _paths(self, key: tuple) -> tuple:
        name = self._name(key)
        return self.base_dir / f"{name}.bin", self.base_dir / f"{name}.meta.json"

    def get(self, key: tuple, now_ts: int):
        data_path, meta_path = self._paths(key)
        if not data_path.exists() or not meta_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            exp = int(meta.get("exp", 0))
            if exp <= now_ts:
                self.delete(key)
                return None
            body = data_path.read_bytes()
            ctype = str(meta.get("ctype", "image/png"))
            os.utime(data_path, None)
            return {"body": body, "ctype": ctype, "exp": exp}
        except Exception:
            self.delete(key)
            return None

    def set(self, key: tuple, entry: dict, now_ts: int):
        data_path, meta_path = self._paths(key)
        data_path.write_bytes(entry["body"])
        meta_path.write_text(
            json.dumps({"exp": int(entry["exp"]), "ctype": str(entry["ctype"]), "atime": now_ts}),
            encoding="utf-8",
        )
        self.writes += 1
        if self.writes % 100 == 0:
            self.prune_if_needed(now_ts)

    def delete(self, key: tuple):
        data_path, meta_path = self._paths(key)
        for p in (data_path, meta_path):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass

    def count_items(self) -> int:
        return sum(1 for _ in self.base_dir.glob("*.bin"))

    def prune_if_needed(self, now_ts: int):
        metas = list(self.base_dir.glob("*.meta.json"))
        records = []
        for meta_path in metas:
            try:
                info = json.loads(meta_path.read_text(encoding="utf-8"))
                exp = int(info.get("exp", 0))
                atime = int(info.get("atime", 0))
                # 修复：正确截取基名，去掉 ".meta.json" 后缀（不能用 .stem，它只去掉最后一个 .json）
                stem = meta_path.name[: -len(".meta.json")]
                if exp <= now_ts:
                    (self.base_dir / f"{stem}.bin").unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                else:
                    records.append((atime, stem))
            except Exception:
                continue
        over = len(records) - self.max_items
        if over > 0:
            records.sort(key=lambda item: item[0])
            for _, stem in records[:over]:
                (self.base_dir / f"{stem}.bin").unlink(missing_ok=True)
                (self.base_dir / f"{stem}.meta.json").unlink(missing_ok=True)

    def close(self):
        pass  # 文件缓存无需显式关闭


def build_requests_session():
    if not (USE_REQUESTS_SESSION and requests is not None):
        return None
    try:
        from requests.adapters import HTTPAdapter
    except Exception:
        return requests.Session()

    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=REQUESTS_POOL_CONNECTIONS,
        pool_maxsize=REQUESTS_POOL_MAXSIZE,
        max_retries=0,
        pool_block=False,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def build_disk_cache():
    if not DISK_CACHE_ENABLED:
        return None
    try:
        return SQLiteDiskCache(DISK_CACHE_DIR, DISK_CACHE_MAX_FILES)
    except Exception as exc:
        print(f"SQLite disk cache init failed, fallback to file cache: {exc}")
        try:
            return FileDiskCache(DISK_CACHE_DIR, DISK_CACHE_MAX_FILES)
        except Exception as exc2:
            print(f"File disk cache init failed, fallback to disabled: {exc2}")
            return None


class Handler(BaseHTTPRequestHandler):
    server_version = "LocalWmtsProxy/1.0"
    protocol_version = "HTTP/1.1"
    upstream_headers = parse_headers(UPSTREAM_HEADERS)
    cache_lock = threading.Lock()
    cache = collections.OrderedDict()
    disk_cache = build_disk_cache()
    stats_lock = threading.Lock()
    stats = {
        "tile_requests": 0,
        "mem_cache_hits": 0,
        "mem_cache_misses": 0,
        "disk_cache_hits": 0,
        "disk_cache_misses": 0,
        "upstream_errors": 0,
        "neg_cache_hits": 0,
    }
    session = build_requests_session()
    # 缓存击穿防护（thundering herd）：同一 tile 只允许一个线程向上游发请求
    _inflight: dict[tuple, threading.Event] = {}
    _inflight_lock = threading.Lock()

    def log_message(self, format, *args):
        # 屏蔽 BaseHTTPRequestHandler 的默认 stderr 日志，避免与自定义日志重复
        pass

    def _write(
        self,
        status: int,
        body: bytes,
        content_type: str = "text/plain; charset=utf-8",
        *,
        max_age: int = None,
    ):
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if max_age is not None and status == 200:
                self.send_header("Cache-Control", f"public, max-age={max_age}")
            else:
                self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError) as exc:
            # Client closed connection while server was responding.
            # This is common when ArcMap exits with in-flight requests.
            winerr = getattr(exc, "winerror", None)
            err = getattr(exc, "errno", None)
            if winerr in (64, 10053, 10054) or err in (32, 104):
                return
            raise

    def do_HEAD(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower()
        if path in ("/health", "/stats"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        if path == "/wmts":
            self.send_response(200)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self.send_response(404)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower()
        query = {k.upper(): v for k, v in parse_qs(parsed.query).items()}

        if path == "/health":
            self._write(200, b"ok")
            return

        if path == "/stats":
            with self.stats_lock:
                payload = dict(self.stats)
            with self.cache_lock:
                payload["cache_items"] = len(self.cache)
            payload["cache_enabled"] = CACHE_ENABLED
            payload["disk_cache_enabled"] = self.disk_cache is not None
            payload["disk_cache_dir"] = str(Path(DISK_CACHE_DIR).resolve())
            # count_items 需加锁，避免与并发写操作竞争
            if self.disk_cache is not None:
                with self.disk_cache.lock:
                    payload["disk_cache_items"] = self.disk_cache.count_items()
            else:
                payload["disk_cache_items"] = 0
            payload["disk_cache_backend"] = (
                "sqlite" if isinstance(self.disk_cache, SQLiteDiskCache) else "file" if self.disk_cache else "disabled"
            )
            payload["transport"] = "requests_session" if self.session is not None else "urllib"
            payload["requests_pool_maxsize"] = REQUESTS_POOL_MAXSIZE
            payload["requests_pool_connections"] = REQUESTS_POOL_CONNECTIONS
            payload["layer_count"] = len(LAYER_CONFIGS)
            payload["layer_ids"] = list(LAYER_CONFIGS.keys())
            body = json.dumps(payload).encode("utf-8")
            self._write(200, body, "application/json; charset=utf-8")
            return

        if path != "/wmts":
            self._write(404, b"not found")
            return

        req = qv(query, "REQUEST", "").upper()
        if req in ("", "GETCAPABILITIES"):
            host = self.headers.get("Host", f"127.0.0.1:{self.server.server_port}")
            # 动态检测 scheme，兼容反向代理 HTTPS 场景
            scheme = self.headers.get("X-Forwarded-Proto", "http")
            xml = capabilities_xml(f"{scheme}://{host}")
            self._write(200, xml.encode("utf-8"), "application/xml; charset=utf-8")
            return

        if req != "GETTILE":
            if WMTS_STRICT_MODE:
                self._write(400, f"Unsupported REQUEST: {req}".encode("utf-8"))
            else:
                if REQUEST_LOG_ENABLED:
                    print(f"WMTS COMPAT REQUEST={req!r} -> GetCapabilities")
                host = self.headers.get("Host", f"127.0.0.1:{self.server.server_port}")
                scheme = self.headers.get("X-Forwarded-Proto", "http")
                xml = capabilities_xml(f"{scheme}://{host}")
                self._write(200, xml.encode("utf-8"), "application/xml; charset=utf-8")
            return

        with self.stats_lock:
            self.stats["tile_requests"] += 1

        layer_id = qv(query, "LAYER", DEFAULT_LAYER_ID).strip() or DEFAULT_LAYER_ID
        if layer_id not in LAYER_CONFIGS:
            if WMTS_STRICT_MODE:
                self._write(400, f"Unsupported LAYER: {layer_id}".encode("utf-8"))
                return
            if REQUEST_LOG_ENABLED:
                print(f"WMTS COMPAT LAYER={layer_id!r} -> {DEFAULT_LAYER_ID!r}")
            layer_id = DEFAULT_LAYER_ID
        layer_cfg = LAYER_CONFIGS[layer_id]

        try:
            z = parse_tile_int(qv(query, "TILEMATRIX"))
            x = parse_tile_int(qv(query, "TILECOL"))
            y = parse_tile_int(qv(query, "TILEROW"))
        except Exception:
            if REQUEST_LOG_ENABLED:
                print(
                    "BAD TILE PARAMS "
                    f"TILEMATRIX={qv(query, 'TILEMATRIX')!r} "
                    f"TILECOL={qv(query, 'TILECOL')!r} "
                    f"TILEROW={qv(query, 'TILEROW')!r}"
                )
            self._write(200, TRANSPARENT_TILE_PNG, "image/png", max_age=NEG_CACHE_TTL_SECONDS)
            return

        if z < MATRIX_MIN or z > MATRIX_MAX:
            if REQUEST_LOG_ENABLED:
                print(f"ZOOM OUT OF RANGE layer={layer_id} z={z} -> transparent tile")
            self._write(200, TRANSPARENT_TILE_PNG, "image/png", max_age=NEG_CACHE_TTL_SECONDS)
            return

        try:
            y_inv = (1 << z) - 1 - y
            upstream_url = layer_cfg["template"].format(z=z, x=x, y=y, y_inv=y_inv)
        except Exception as exc:
            self._write(500, f"Bad layer template ({layer_id}): {exc}".encode("utf-8"))
            return

        cache_key = (layer_id, z, x, y)
        now = int(time.time())

        # --- 内存缓存查找 ---
        if CACHE_ENABLED:
            with self.cache_lock:
                entry = self.cache.get(cache_key)
                if entry and entry["exp"] > now:
                    self.cache.move_to_end(cache_key)
                    with self.stats_lock:
                        if entry.get("neg"):
                            self.stats["neg_cache_hits"] += 1
                        else:
                            self.stats["mem_cache_hits"] += 1
                    if REQUEST_LOG_ENABLED:
                        tag = "NEG-HIT" if entry.get("neg") else "MEM-HIT"
                        print(f"{tag} layer={layer_id} z={z} x={x} y={y}")
                    self._write(200, entry["body"], entry["ctype"], max_age=cache_max_age(entry, now))
                    return
                if entry:
                    self.cache.pop(cache_key, None)
            with self.stats_lock:
                self.stats["mem_cache_misses"] += 1
            if REQUEST_LOG_ENABLED:
                print(f"MEM-MISS layer={layer_id} z={z} x={x} y={y}")

        # --- 磁盘缓存查找 ---
        if self.disk_cache is not None:
            with self.disk_cache.lock:
                disk_entry = self.disk_cache.get(cache_key, now)
            if disk_entry:
                with self.stats_lock:
                    self.stats["disk_cache_hits"] += 1
                if CACHE_ENABLED:
                    with self.cache_lock:
                        self.cache[cache_key] = disk_entry
                        self.cache.move_to_end(cache_key)
                        while len(self.cache) > CACHE_MAX_ITEMS:
                            self.cache.popitem(last=False)
                if REQUEST_LOG_ENABLED:
                    print(f"DISK-HIT layer={layer_id} z={z} x={x} y={y}")
                self._write(200, disk_entry["body"], disk_entry["ctype"], max_age=cache_max_age(disk_entry, now))
                return
            with self.stats_lock:
                self.stats["disk_cache_misses"] += 1
            if REQUEST_LOG_ENABLED:
                print(f"DISK-MISS layer={layer_id} z={z} x={x} y={y}")

        # --- 缓存击穿防护：in-flight 去重 ---
        # 仅在至少一层缓存可用时启用，否则 follower 无法从缓存获取 leader 结果
        _has_cache = CACHE_ENABLED or self.disk_cache is not None
        evt = None
        leader = True

        if _has_cache:
            with self._inflight_lock:
                if cache_key in self._inflight:
                    evt = self._inflight[cache_key]
                    leader = False
                else:
                    evt = threading.Event()
                    self._inflight[cache_key] = evt

        if not leader:
            # follower 等待 leader 完成后，直接从缓存读取结果
            evt.wait(timeout=UPSTREAM_TIMEOUT + 5)
            now2 = int(time.time())
            if CACHE_ENABLED:
                with self.cache_lock:
                    entry = self.cache.get(cache_key)
                    if entry and entry["exp"] > now2:
                        with self.stats_lock:
                            if entry.get("neg"):
                                self.stats["neg_cache_hits"] += 1
                            else:
                                self.stats["mem_cache_hits"] += 1
                        if REQUEST_LOG_ENABLED:
                            tag = "NEG-HIT" if entry.get("neg") else "MEM-HIT"
                            print(f"{tag} follower layer={layer_id} z={z} x={x} y={y}")
                        self._write(200, entry["body"], entry["ctype"], max_age=cache_max_age(entry, now2))
                        return
            if self.disk_cache is not None:
                with self.disk_cache.lock:
                    disk_entry = self.disk_cache.get(cache_key, now2)
                if disk_entry:
                    with self.stats_lock:
                        self.stats["disk_cache_hits"] += 1
                    if REQUEST_LOG_ENABLED:
                        print(f"DISK-HIT follower layer={layer_id} z={z} x={x} y={y}")
                    self._write(200, disk_entry["body"], disk_entry["ctype"], max_age=cache_max_age(disk_entry, now2))
                    return
            self._write(502, b"Upstream fetch failed (concurrent request timed out)")
            return

        # --- 从上游获取瓦片（leader 线程，或无缓存时所有线程独立请求）---
        try:
            if self.session is not None:
                resp = self.session.get(
                    upstream_url,
                    headers=self.upstream_headers,
                    timeout=UPSTREAM_TIMEOUT,
                    stream=False,
                )
                resp.raise_for_status()
                data = resp.content
                ctype = resp.headers.get("Content-Type", "image/png")
            else:
                req_obj = urllib.request.Request(upstream_url, headers=self.upstream_headers, method="GET")
                with urllib.request.urlopen(req_obj, timeout=UPSTREAM_TIMEOUT) as resp:
                    data = resp.read()
                    ctype = resp.headers.get("Content-Type", "image/png")

            entry = {"body": data, "ctype": ctype, "exp": now + CACHE_TTL_SECONDS}
            if CACHE_ENABLED:
                with self.cache_lock:
                    self.cache[cache_key] = entry
                    self.cache.move_to_end(cache_key)
                    while len(self.cache) > CACHE_MAX_ITEMS:
                        self.cache.popitem(last=False)
            if self.disk_cache is not None:
                with self.disk_cache.lock:
                    self.disk_cache.set(cache_key, entry, now)

            if REQUEST_LOG_ENABLED:
                print(f"UPSTREAM 200 layer={layer_id} z={z} x={x} y={y}")
            self._write(200, data, ctype, max_age=CACHE_TTL_SECONDS)

        except urllib.error.HTTPError as exc:
            with self.stats_lock:
                self.stats["upstream_errors"] += 1
            if REQUEST_LOG_ENABLED:
                print(f"UPSTREAM HTTP {exc.code} layer={layer_id} z={z} x={x} y={y}")
            if exc.code == 404:
                # 负缓存：将 404 转为透明瓦片存入内存缓存，阻断同坐标重复无效请求
                self._store_neg_cache(cache_key, now)
                if REQUEST_LOG_ENABLED:
                    print(f"NEG-CACHE layer={layer_id} z={z} x={x} y={y} (upstream 404)")
                self._write(200, TRANSPARENT_TILE_PNG, "image/png", max_age=NEG_CACHE_TTL_SECONDS)
            else:
                self._write(502, f"Upstream HTTP {exc.code}".encode("utf-8"))

        except Exception as exc:
            status_code = getattr(exc, "response", None)
            if status_code is not None:
                status_code = getattr(exc.response, "status_code", None)
            with self.stats_lock:
                self.stats["upstream_errors"] += 1
            if REQUEST_LOG_ENABLED:
                if status_code is not None:
                    print(f"UPSTREAM HTTP {status_code} z={z} x={x} y={y}")
                else:
                    print(f"UPSTREAM ERR layer={layer_id} z={z} x={x} y={y} err={exc}")
            if status_code == 404:
                self._store_neg_cache(cache_key, now)
                if REQUEST_LOG_ENABLED:
                    print(f"NEG-CACHE layer={layer_id} z={z} x={x} y={y} (upstream 404 via requests)")
                self._write(200, TRANSPARENT_TILE_PNG, "image/png", max_age=NEG_CACHE_TTL_SECONDS)
            elif status_code is not None:
                self._write(502, f"Upstream HTTP {status_code}".encode("utf-8"))
            else:
                self._write(502, f"Upstream error: {exc}".encode("utf-8"))

        finally:
            # leader 完成后释放等待的 follower 线程（缓存全禁用时 evt 为 None，跳过）
            if evt is not None:
                with self._inflight_lock:
                    self._inflight.pop(cache_key, None)
                evt.set()

    def _store_neg_cache(self, cache_key: tuple, now: int):
        """将透明瓦片写入内存缓存（负缓存，TTL 较短，不写磁盘）。"""
        if not CACHE_ENABLED:
            return
        neg_entry = {
            "body": TRANSPARENT_TILE_PNG,
            "ctype": "image/png",
            "exp": now + NEG_CACHE_TTL_SECONDS,
            "neg": True,
        }
        with self.cache_lock:
            self.cache[cache_key] = neg_entry
            self.cache.move_to_end(cache_key)
            while len(self.cache) > CACHE_MAX_ITEMS:
                self.cache.popitem(last=False)


def main():
    port = int(os.getenv("PORT", str(DEFAULT_PORT)))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"WMTS proxy listening on http://127.0.0.1:{port}")
    print("Health: /health")
    print("Stats: /stats")
    print("Capabilities: /wmts?SERVICE=WMTS&REQUEST=GetCapabilities")
    print(
        f"Cache: enabled={CACHE_ENABLED} max_items={CACHE_MAX_ITEMS} ttl_seconds={CACHE_TTL_SECONDS}"
    )
    print(
        f"Disk cache: enabled={Handler.disk_cache is not None} dir={Path(DISK_CACHE_DIR).resolve()} max_items={DISK_CACHE_MAX_FILES}"
    )
    if Handler.disk_cache is not None:
        backend = "sqlite" if isinstance(Handler.disk_cache, SQLiteDiskCache) else "file"
        print(f"Disk cache backend: {backend}")
    print(
        f"Transport: {'requests_session' if (USE_REQUESTS_SESSION and requests is not None) else 'urllib'}"
    )
    print(f"Layers: {', '.join(LAYER_CONFIGS.keys())}")
    if USE_REQUESTS_SESSION and requests is not None:
        print(
            f"Requests pool: connections={REQUESTS_POOL_CONNECTIONS} maxsize={REQUESTS_POOL_MAXSIZE}"
        )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        # 优雅关闭：确保 SQLite WAL 文件正确 checkpoint 并关闭连接
        if Handler.disk_cache is not None:
            Handler.disk_cache.close()


if __name__ == "__main__":
    main()
