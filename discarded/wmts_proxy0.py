#!/usr/bin/env python3
import collections
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


DEFAULT_PORT = 8787
LAYER_NAME = "authorized_tiles"
MATRIX_SET = "GoogleMapsCompatible"
MATRIX_MIN = int(os.getenv("WMTS_MIN_ZOOM", "0"))
MATRIX_MAX = int(os.getenv("WMTS_MAX_ZOOM", "22"))
UPSTREAM_TEMPLATE = os.getenv("UPSTREAM_TILE_URL_TEMPLATE", "").strip()
UPSTREAM_HEADERS = os.getenv("UPSTREAM_HEADERS_JSON", "").strip()
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "20"))
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "1").strip() != "0"
CACHE_MAX_ITEMS = int(os.getenv("CACHE_MAX_ITEMS", "3000"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))
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
  <Contents>
    <Layer>
      <ows:Title>Authorized Tile Layer</ows:Title>
      <ows:Identifier>{LAYER_NAME}</ows:Identifier>
      <Style isDefault="true">
        <ows:Identifier>default</ows:Identifier>
      </Style>
      <Format>image/png</Format>
      <Format>image/jpeg</Format>
      <TileMatrixSetLink>
        <TileMatrixSet>{MATRIX_SET}</TileMatrixSet>
      </TileMatrixSetLink>
      <ResourceURL format="image/png" resourceType="tile"
        template="{base_url}/wmts?SERVICE=WMTS&amp;REQUEST=GetTile&amp;VERSION=1.0.0&amp;LAYER={LAYER_NAME}&amp;STYLE=default&amp;TILEMATRIXSET={MATRIX_SET}&amp;TILEMATRIX={{TileMatrix}}&amp;TILEROW={{TileRow}}&amp;TILECOL={{TileCol}}" />
    </Layer>
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
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.base_dir / "tiles_cache.sqlite3"
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # Use conservative pragmas for better Windows compatibility.
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tiles (
                z INTEGER NOT NULL,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                exp INTEGER NOT NULL,
                ctype TEXT NOT NULL,
                body BLOB NOT NULL,
                atime INTEGER NOT NULL,
                PRIMARY KEY (z, x, y)
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_exp ON tiles(exp)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_atime ON tiles(atime)")
        self.conn.commit()

    def get(self, key: tuple[int, int, int], now_ts: int):
        z, x, y = key
        row = self.conn.execute(
            "SELECT exp, ctype, body FROM tiles WHERE z=? AND x=? AND y=?",
            (z, x, y),
        ).fetchone()
        if row is None:
            return None
        exp, ctype, body = int(row[0]), str(row[1]), row[2]
        if exp <= now_ts:
            self.conn.execute("DELETE FROM tiles WHERE z=? AND x=? AND y=?", (z, x, y))
            self.conn.commit()
            return None
        self.conn.execute(
            "UPDATE tiles SET atime=? WHERE z=? AND x=? AND y=?",
            (now_ts, z, x, y),
        )
        self.conn.commit()
        return {"body": body, "ctype": ctype, "exp": exp}

    def set(self, key: tuple[int, int, int], entry: dict, now_ts: int):
        z, x, y = key
        self.conn.execute(
            """
            INSERT INTO tiles (z, x, y, exp, ctype, body, atime)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(z, x, y) DO UPDATE SET
                exp=excluded.exp,
                ctype=excluded.ctype,
                body=excluded.body,
                atime=excluded.atime
            """,
            (z, x, y, int(entry["exp"]), str(entry["ctype"]), entry["body"], now_ts),
        )
        self.conn.commit()
        self.prune_if_needed(now_ts)

    def count_items(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM tiles").fetchone()
        return int(row[0]) if row else 0

    def prune_if_needed(self, now_ts: int):
        self.conn.execute("DELETE FROM tiles WHERE exp <= ?", (now_ts,))
        over = self.count_items() - self.max_items
        if over > 0:
            self.conn.execute(
                """
                DELETE FROM tiles
                WHERE rowid IN (
                    SELECT rowid FROM tiles
                    ORDER BY atime ASC
                    LIMIT ?
                )
                """,
                (over,),
            )
        self.conn.commit()


class FileDiskCache:
    def __init__(self, base_dir: str, max_items: int):
        self.base_dir = Path(base_dir)
        self.max_items = max_items
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.writes = 0

    @staticmethod
    def _name(key: tuple[int, int, int]) -> str:
        z, x, y = key
        return f"{z}_{x}_{y}"

    def _paths(self, key: tuple[int, int, int]) -> tuple[Path, Path]:
        name = self._name(key)
        return self.base_dir / f"{name}.bin", self.base_dir / f"{name}.meta.json"

    def get(self, key: tuple[int, int, int], now_ts: int):
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

    def set(self, key: tuple[int, int, int], entry: dict, now_ts: int):
        data_path, meta_path = self._paths(key)
        data_path.write_bytes(entry["body"])
        meta_path.write_text(
            json.dumps({"exp": int(entry["exp"]), "ctype": str(entry["ctype"]), "atime": now_ts}),
            encoding="utf-8",
        )
        self.writes += 1
        if self.writes % 100 == 0:
            self.prune_if_needed(now_ts)

    def delete(self, key: tuple[int, int, int]):
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
        for meta in metas:
            try:
                info = json.loads(meta.read_text(encoding="utf-8"))
                exp = int(info.get("exp", 0))
                atime = int(info.get("atime", 0))
                stem = meta.stem
                if exp <= now_ts:
                    bin_path = self.base_dir / f"{stem}.bin"
                    bin_path.unlink(missing_ok=True)
                    meta.unlink(missing_ok=True)
                else:
                    records.append((atime, stem))
            except Exception:
                continue
        over = len(records) - self.max_items
        if over > 0:
            records.sort(key=lambda x: x[0])
            for _, stem in records[:over]:
                (self.base_dir / f"{stem}.bin").unlink(missing_ok=True)
                (self.base_dir / f"{stem}.meta.json").unlink(missing_ok=True)


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
    }
    session = build_requests_session()

    def _write(self, status: int, body: bytes, content_type: str = "text/plain; charset=utf-8"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

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
            payload["cache_items"] = len(self.cache)
            payload["cache_enabled"] = CACHE_ENABLED
            payload["disk_cache_enabled"] = self.disk_cache is not None
            payload["disk_cache_dir"] = str(Path(DISK_CACHE_DIR).resolve())
            payload["disk_cache_items"] = self.disk_cache.count_items() if self.disk_cache else 0
            payload["disk_cache_backend"] = (
                "sqlite" if isinstance(self.disk_cache, SQLiteDiskCache) else "file" if self.disk_cache else "disabled"
            )
            payload["transport"] = "requests_session" if self.session is not None else "urllib"
            payload["requests_pool_maxsize"] = REQUESTS_POOL_MAXSIZE
            payload["requests_pool_connections"] = REQUESTS_POOL_CONNECTIONS
            body = json.dumps(payload).encode("utf-8")
            self._write(200, body, "application/json; charset=utf-8")
            return

        if path != "/wmts":
            self._write(404, b"not found")
            return

        req = qv(query, "REQUEST", "").upper()
        if req == "GETCAPABILITIES":
            host = self.headers.get("Host", f"127.0.0.1:{self.server.server_port}")
            scheme = "http"
            xml = capabilities_xml(f"{scheme}://{host}")
            self._write(200, xml.encode("utf-8"), "application/xml; charset=utf-8")
            return

        if req != "GETTILE":
            self._write(400, b"REQUEST must be GetCapabilities or GetTile")
            return

        try:
            z = int(qv(query, "TILEMATRIX"))
            x = int(qv(query, "TILECOL"))
            y = int(qv(query, "TILEROW"))
        except Exception:
            self._write(400, b"Invalid TILEMATRIX/TILECOL/TILEROW")
            return

        if z < MATRIX_MIN or z > MATRIX_MAX:
            self._write(404, b"Zoom out of allowed range")
            return

        if not UPSTREAM_TEMPLATE:
            self._write(500, b"UPSTREAM_TILE_URL_TEMPLATE is required")
            return

        try:
            upstream_url = UPSTREAM_TEMPLATE.format(z=z, x=x, y=y)
        except Exception as exc:
            self._write(500, f"Bad UPSTREAM_TILE_URL_TEMPLATE: {exc}".encode("utf-8"))
            return

        cache_key = (z, x, y)
        now = int(time.time())
        with self.stats_lock:
            self.stats["tile_requests"] += 1
        if CACHE_ENABLED:
            with self.cache_lock:
                entry = self.cache.get(cache_key)
                if entry and entry["exp"] > now:
                    self.cache.move_to_end(cache_key)
                    with self.stats_lock:
                        self.stats["mem_cache_hits"] += 1
                    if REQUEST_LOG_ENABLED:
                        print(f"MEM-HIT z={z} x={x} y={y}")
                    self._write(200, entry["body"], entry["ctype"])
                    return
                if entry:
                    self.cache.pop(cache_key, None)
            with self.stats_lock:
                self.stats["mem_cache_misses"] += 1
            if REQUEST_LOG_ENABLED:
                print(f"MEM-MISS z={z} x={x} y={y}")

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
                    print(f"DISK-HIT z={z} x={x} y={y}")
                self._write(200, disk_entry["body"], disk_entry["ctype"])
                return
            with self.stats_lock:
                self.stats["disk_cache_misses"] += 1
            if REQUEST_LOG_ENABLED:
                print(f"DISK-MISS z={z} x={x} y={y}")

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
                print(f"UPSTREAM 200 z={z} x={x} y={y}")
            self._write(200, data, ctype)
        except urllib.error.HTTPError as exc:
            with self.stats_lock:
                self.stats["upstream_errors"] += 1
            if REQUEST_LOG_ENABLED:
                print(f"UPSTREAM HTTP {exc.code} z={z} x={x} y={y}")
            msg = f"Upstream HTTP {exc.code}".encode("utf-8")
            self._write(502, msg)
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
                    print(f"UPSTREAM ERR z={z} x={x} y={y} err={exc}")
            if status_code is not None:
                self._write(502, f"Upstream HTTP {status_code}".encode("utf-8"))
            else:
                self._write(502, f"Upstream error: {exc}".encode("utf-8"))


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
    if USE_REQUESTS_SESSION and requests is not None:
        print(
            f"Requests pool: connections={REQUESTS_POOL_CONNECTIONS} maxsize={REQUESTS_POOL_MAXSIZE}"
        )
    server.serve_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
