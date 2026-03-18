import asyncio
import aiohttp
import json
import time
import logging
import hmac
import hashlib
import requests as req_lib
from functools import wraps
from collections import defaultdict
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit
from threading import Thread, Lock
import socket
from datetime import datetime, timedelta
from collections import deque
from typing import Optional, Dict, Set, List, Tuple
import os
import re
from dataclasses import dataclass, asdict
from enum import Enum
import discord
from discord.ext import tasks

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# Configuration & Constants
# ============================================================================

class Config:
    DISCORD_TOKEN      = os.environ.get("DISCORD_TOKEN")
    SPECIAL_CHANNEL_ID = int(os.environ.get("SPECIAL_CHANNEL_ID", "1447173921261490187"))
    PORT               = int(os.environ.get("PORT", 5000))
    SECRET_KEY         = os.environ.get("SECRET_KEY", "your-secret-key-change-this")

    AUTH_TOKEN        = os.environ.get("SHADOW_AUTH_TOKEN", "CHANGE_THIS_SECRET_TOKEN")
    RESPONSE_SIG      = os.environ.get("SHADOW_SIG",        "shadow_valid_v2")

    RATE_LIMIT_MAX    = int(os.environ.get("RATE_LIMIT_MAX",    "60"))
    RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))

    MAX_QUEUE_SIZE         = 1000
    MAX_PROCESSED_SERVERS  = 10000
    MAX_MESSAGE_IDS        = 5000
    CLIENT_RESET_INTERVAL  = 1800
    PROCESSED_SERVER_TTL   = 3600

    DEFAULT_RETRIES        = 3
    DEFAULT_RETRY_DELAY    = 2
    GATEWAY_RECONNECT_DELAY = 5
    MAX_RECONNECT_DELAY    = 300

    # ── LUARMOR ──────────────────────────────────────────────────
    # Your Luarmor dashboard URL — used to validate script keys
    LUARMOR_DASHBOARD  = os.environ.get(
        "LUARMOR_DASHBOARD",
        "https://dashboard-for-luarmor-production.up.railway.app"
    )
    # How long to cache a validated key before re-checking (seconds)
    KEY_CACHE_TTL      = int(os.environ.get("KEY_CACHE_TTL", "300"))


PRIORITY_PETS = [
    "Strawberry Elephant",
    "Meowl",
    "Skibidi Toilet",
    "Dragon Gingerini",
    "Dragon Cannelloni",
    "Headless Horseman"
]

PRIORITY_PETS_SET   = set(p.lower() for p in PRIORITY_PETS)
PRIORITY_PETS_INDEX = {p.lower(): i for i, p in enumerate(PRIORITY_PETS)}


# ============================================================================
# Luarmor Key Validation
# ============================================================================

# Cache: key -> (is_valid: bool, cached_at: float)
_key_cache: Dict[str, Tuple[bool, float]] = {}
_key_cache_lock = Lock()

# Clients that have passed Luarmor validation
validated_clients: Set[str] = set()
validated_lock = Lock()


def validate_luarmor_key(key: str) -> bool:
    """
    Check whether a Luarmor key is valid by calling your dashboard API.
    Results are cached for KEY_CACHE_TTL seconds to avoid hammering the API.
    Returns True only if the key exists and is not banned/expired.
    """
    if not key or key in ("", "KEY_NOT_FOUND"):
        return False

    now = time.time()

    # Check cache first
    with _key_cache_lock:
        if key in _key_cache:
            is_valid, cached_at = _key_cache[key]
            if now - cached_at < Config.KEY_CACHE_TTL:
                return is_valid

    # Call Luarmor dashboard
    try:
        url = f"{Config.LUARMOR_DASHBOARD}/api/key-info?key={key}"
        r = req_lib.get(url, timeout=6)
        if r.status_code != 200:
            with _key_cache_lock:
                _key_cache[key] = (False, now)
            return False
        data = r.json()
        # Key must exist and not be banned
        is_valid = (
            data.get("key") is not None
            and not data.get("banned", False)
        )
    except Exception as e:
        logger.warning(f"⚠️  Luarmor validation error for key {key[:8]}...: {e}")
        # If Luarmor is unreachable, deny by default (safe fallback)
        with _key_cache_lock:
            _key_cache[key] = (False, now)
        return False

    with _key_cache_lock:
        _key_cache[key] = (is_valid, now)

    if is_valid:
        logger.info(f"✅ Luarmor key valid: {key[:8]}...")
    else:
        logger.warning(f"🔒 Luarmor key INVALID: {key[:8]}...")

    return is_valid


def mark_client_validated(client_id: str):
    with validated_lock:
        validated_clients.add(client_id)


def is_client_validated(client_id: str) -> bool:
    with validated_lock:
        return client_id in validated_clients


def remove_validated_client(client_id: str):
    with validated_lock:
        validated_clients.discard(client_id)


# ============================================================================
# Security Middleware
# ============================================================================

_raw_blacklist = os.environ.get("IP_BLACKLIST", "")
IP_BLACKLIST: Set[str] = set(x.strip() for x in _raw_blacklist.split(",") if x.strip())

_rate_store: Dict[str, list] = defaultdict(list)
_rate_lock  = Lock()


def _get_client_ip() -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _check_rate_limit(ip: str) -> bool:
    now = time.time()
    window = Config.RATE_LIMIT_WINDOW
    with _rate_lock:
        _rate_store[ip] = [t for t in _rate_store[ip] if now - t < window]
        if len(_rate_store[ip]) >= Config.RATE_LIMIT_MAX:
            return False
        _rate_store[ip].append(now)
        return True


def _validate_token(req) -> bool:
    token = (
        req.headers.get("X-Shadow-Token")
        or req.args.get("_t")
        or ""
    )
    return hmac.compare_digest(token, Config.AUTH_TOKEN)


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        ip = _get_client_ip()
        if ip in IP_BLACKLIST:
            logger.warning(f"🚫 BLOCKED blacklisted IP: {ip}")
            return jsonify({"error": "Forbidden"}), 403
        if not _check_rate_limit(ip):
            logger.warning(f"⚡ RATE LIMITED: {ip}")
            return jsonify({"error": "Too many requests"}), 429
        if not _validate_token(request):
            logger.warning(f"🔒 UNAUTHORIZED from {ip} — bad or missing token")
            return jsonify({"error": "Unauthorized"}), 403
        return f(*args, **kwargs)
    return decorated


def signed(data: dict) -> dict:
    data["__sig"] = Config.RESPONSE_SIG
    return data


def _ws_validate_token(data: dict) -> bool:
    token = data.get("_t") or data.get("token") or ""
    return hmac.compare_digest(token, Config.AUTH_TOKEN)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Job:
    server_id:     str
    pet_name:      str
    pet_value:     float
    pet_thumbnail: Optional[str]
    retries:       int
    retry_delay:   int
    created_time:  float
    is_priority:   bool = False
    duel_mode:     Optional[bool] = None

    def format_value(self) -> str:
        if self.pet_value >= 1_000_000_000:
            return f"${self.pet_value / 1_000_000_000:.2f}B/s"
        elif self.pet_value >= 1_000_000:
            return f"${self.pet_value / 1_000_000:.2f}M/s"
        elif self.pet_value >= 1_000:
            return f"${self.pet_value / 1_000:.2f}K/s"
        else:
            return f"${self.pet_value:.2f}/s"

    def format_duel(self) -> str:
        if self.duel_mode is True:
            return "Duel: ✅ Active"
        elif self.duel_mode is False:
            return "Duel: ❌ Inactive"
        return "Duel: ➖ N/A"

    def to_dict(self):
        data = asdict(self)
        data['pet_value_formatted'] = self.format_value()
        data['duel_mode_label']     = self.format_duel()
        return data


@dataclass
class PetInfo:
    name:        Optional[str]
    value:       float
    thumbnail:   Optional[str] = None
    is_priority: bool = False
    count:       int  = 1


# ============================================================================
# Pet Parsing
# ============================================================================

class PetParser:
    VALUE_PATTERN     = re.compile(
        r'([A-Za-z\s\-\'\.\(\)]+?)\s*[:\-]?\s*\$?\s*([\d\.,]+)\s*([MBK]?)\s*/s',
        re.IGNORECASE
    )
    SERVER_ID_PATTERN = re.compile(r'[a-f0-9]{32,}', re.IGNORECASE)

    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        return text.replace("```", "").replace("`", "").strip()

    @staticmethod
    def parse_value(value_str: str) -> float:
        try:
            value_str = value_str.replace(",", "").upper().strip()
            if "B" in value_str:
                return float(value_str.replace("B", "").strip()) * 1_000_000_000
            elif "M" in value_str:
                return float(value_str.replace("M", "").strip()) * 1_000_000
            elif "K" in value_str:
                return float(value_str.replace("K", "").strip()) * 1_000
            else:
                filtered = "".join(ch for ch in value_str if ch.isdigit() or ch == '.')
                return float(filtered) if filtered else 0
        except (ValueError, AttributeError):
            return 0

    @classmethod
    def extract_pet_from_line(cls, line: str) -> Tuple[Optional[str], float]:
        line = line.strip()
        if not line:
            return None, 0

        pet_name = None
        value    = 0

        if "$" in line:
            parts = line.split("$", 1)
            if len(parts) >= 2:
                pet_name = parts[0].strip().rstrip(":-").strip()
                value    = cls.parse_value(parts[1].replace("/s", ""))

        if not pet_name or value == 0:
            match = cls.VALUE_PATTERN.search(line)
            if match:
                pet_name  = match.group(1).strip().rstrip(":-").strip()
                num_part  = match.group(2).replace(",", "")
                suffix    = match.group(3).upper()
                try:
                    base_value  = float(num_part)
                    multipliers = {"B": 1e9, "M": 1e6, "K": 1e3}
                    value       = base_value * multipliers.get(suffix, 1)
                except ValueError:
                    value = 0

        return pet_name, value if pet_name else 0

    @classmethod
    def extract_priority_pets(cls, text: str) -> Optional[PetInfo]:
        text = cls.clean_text(text)
        if not text or text.lower() == "none":
            return None

        lines = [l.strip() for l in text.split("\n") if l.strip()]
        found: Dict[str, Dict] = {}

        for line in lines:
            if "-" not in line and "$" not in line:
                continue
            pet_name, value = cls.extract_pet_from_line(line)
            if not pet_name or value == 0:
                continue
            pet_name_lower = pet_name.lower()
            if pet_name_lower not in PRIORITY_PETS_SET:
                continue
            matched = next((p for p in PRIORITY_PETS if p.lower() == pet_name_lower), None)
            if not matched:
                continue
            if matched not in found:
                found[matched] = {"value": value, "count": 1}
            else:
                found[matched]["count"] += 1
                found[matched]["value"]  = max(found[matched]["value"], value)

        if not found:
            return None

        top = min(found.keys(), key=lambda p: PRIORITY_PETS_INDEX.get(p.lower(), len(PRIORITY_PETS)))
        d   = found[top]
        name = f"{d['count']}x {top}" if d["count"] > 1 else top
        return PetInfo(name=name, value=d["value"], is_priority=False, count=d["count"])

    @classmethod
    def parse_embed(cls, embed_dict: dict) -> Dict:
        server_id      = None
        pet_thumbnail  = None
        duel_mode      = None

        if embed_dict.get("thumbnail") and embed_dict["thumbnail"].get("url"):
            pet_thumbnail = embed_dict["thumbnail"]["url"]

        fields = embed_dict.get("fields", [])

        for field in fields:
            field_name  = field.get("name", "").lower()
            field_value = cls.clean_text(field.get("value", ""))
            if not server_id and (
                ("server" in field_name and "id" in field_name) or
                ("job"    in field_name and "id" in field_name)
            ):
                server_id = field_value
            if "duel" in field_name:
                fvl = field_value.lower()
                if "inactive" in fvl:
                    duel_mode = False
                elif "active" in fvl:
                    duel_mode = True

        if not server_id:
            for text in [embed_dict.get("description", ""), embed_dict.get("title", "")]:
                match = cls.SERVER_ID_PATTERN.search(text)
                if match:
                    server_id = match.group()
                    break

        all_pets_found: Dict[str, Dict] = {}

        for field in fields:
            field_name  = field.get("name", "").lower()
            if "highest" in field_name and "pet" in field_name:
                field_value = cls.clean_text(field.get("value", ""))
                name, value = cls.extract_pet_from_line(field_value)
                if name and value > 0:
                    if name not in all_pets_found:
                        all_pets_found[name] = {"value": value, "name": name}
                    else:
                        all_pets_found[name]["value"] = max(all_pets_found[name]["value"], value)
                break

        for field in fields:
            field_name = field.get("name", "").lower()
            if "other pet" in field_name:
                field_value = cls.clean_text(field.get("value", ""))
                for line in [l.strip() for l in field_value.split("\n") if l.strip()]:
                    if "-" not in line and "$" not in line:
                        continue
                    name, value = cls.extract_pet_from_line(line)
                    if not name or value == 0:
                        continue
                    if name not in all_pets_found:
                        all_pets_found[name] = {"value": value, "name": name}
                    else:
                        all_pets_found[name]["value"] = max(all_pets_found[name]["value"], value)
                break

        all_pets_list: List[PetInfo] = []
        if all_pets_found:
            priority_pets = []
            regular_pets  = []
            for pet_name, pet_data in all_pets_found.items():
                pet_name_lower = pet_name.lower()
                matched = next((p for p in PRIORITY_PETS if p.lower() == pet_name_lower), None)
                pet_info = PetInfo(name=pet_data["name"], value=pet_data["value"],
                                   thumbnail=pet_thumbnail, is_priority=False)
                if matched:
                    priority_pets.append((matched, pet_info))
                else:
                    regular_pets.append(pet_info)

            priority_pets.sort(key=lambda x: PRIORITY_PETS_INDEX.get(x[0].lower(), len(PRIORITY_PETS)))
            for _, pi in priority_pets:
                all_pets_list.append(pi)
            for pi in regular_pets:
                all_pets_list.append(pi)

        return {"server_id": server_id, "pet_info_list": all_pets_list, "duel_mode": duel_mode}


# ============================================================================
# Job Queue Manager
# ============================================================================

class JobQueueManager:
    def __init__(self, max_size: int = Config.MAX_QUEUE_SIZE):
        self.queue: deque[Job]                                 = deque(maxlen=max_size)
        self.lock                                              = Lock()
        self.job_sent_to_clients: Dict[Tuple[str,str], Set[str]] = {}
        self.sent_servers: Set[str]                            = set()

    def add_job(self, job: Job, unique_tracking_id: Optional[str] = None) -> bool:
        with self.lock:
            tracking_id = unique_tracking_id or job.server_id
            for idx, existing in enumerate(self.queue):
                if existing.server_id == job.server_id and existing.pet_name == job.pet_name:
                    if job.pet_value > existing.pet_value:
                        del self.queue[idx]
                        self.queue.append(job)
                        logger.info(f"🔄 Updated job {job.server_id[:8]}... {job.pet_name} ({job.format_value()})")
                        return True
                    return False
            self.queue.append(job)
            self.sent_servers.add(tracking_id)
            logger.info(f"🚀 NEW JOB: {job.server_id[:8]}... Pet: {job.pet_name} ({job.format_value()}) | {job.format_duel()} [Queue: {len(self.queue)}]")
            return True

    def get_job_for_client(self, client_id: str, processed_servers: Set[str]) -> Optional[Job]:
        with self.lock:
            client_connect_time = client_connection_times.get(client_id, 0)
            for job in self.queue:
                server_id = job.server_id
                pet_name  = job.pet_name
                job_key   = (server_id, pet_name)
                if server_id in processed_servers:
                    continue
                if job_key in self.job_sent_to_clients and client_id in self.job_sent_to_clients[job_key]:
                    continue
                if client_connect_time > 0 and job.created_time <= client_connect_time:
                    continue
                if job_key not in self.job_sent_to_clients:
                    self.job_sent_to_clients[job_key] = set()
                self.job_sent_to_clients[job_key].add(client_id)
                return job
            return None

    def remove_job(self, server_id: str, pet_name: Optional[str] = None):
        with self.lock:
            if pet_name:
                self.queue = deque(
                    [j for j in self.queue if not (j.server_id == server_id and j.pet_name == pet_name)],
                    maxlen=self.queue.maxlen
                )
            else:
                self.queue = deque(
                    [j for j in self.queue if j.server_id != server_id],
                    maxlen=self.queue.maxlen
                )

    def size(self) -> int:
        with self.lock:
            return len(self.queue)

    def get_stats(self) -> Dict:
        with self.lock:
            return {
                "size":         len(self.queue),
                "sent_servers": len(self.sent_servers),
                "tracked_jobs": len(self.job_sent_to_clients)
            }


# ============================================================================
# Processed Servers Manager
# ============================================================================

class ProcessedServersManager:
    def __init__(self, max_size: int = Config.MAX_PROCESSED_SERVERS,
                 ttl: int = Config.PROCESSED_SERVER_TTL):
        self.servers: Dict[str, Dict] = {}
        self.lock         = Lock()
        self.max_size     = max_size
        self.ttl          = ttl
        self.last_cleanup = time.time()

    def add(self, server_id: str, client_id: str):
        with self.lock:
            if server_id not in self.servers:
                self.servers[server_id] = {"clients": set(), "timestamp": datetime.now()}
            self.servers[server_id]["clients"].add(client_id)
            if len(self.servers) > self.max_size:
                self._cleanup()

    def contains(self, server_id: str) -> bool:
        with self.lock:
            if time.time() - self.last_cleanup > 300:
                self._cleanup()
            return server_id in self.servers

    def _cleanup(self):
        cutoff = datetime.now() - timedelta(seconds=self.ttl)
        self.servers = {sid: d for sid, d in self.servers.items() if d["timestamp"] > cutoff}
        self.last_cleanup = time.time()
        logger.info(f"🧹 Cleaned processed servers. Remaining: {len(self.servers)}")

    def size(self) -> int:
        with self.lock:
            return len(self.servers)


# ============================================================================
# Discord Bot
# ============================================================================

class DiscordBot:
    def __init__(self, token: str, channel_id: int,
                 job_queue: JobQueueManager, processed_servers: ProcessedServersManager):
        self.token             = token
        self.channel_id        = channel_id
        self.job_queue         = job_queue
        self.processed_servers = processed_servers
        self.processed_message_ids: Set[int] = set()

        intents = discord.Intents.default()
        intents.message_content = True
        intents.guild_messages  = True

        self.bot       = discord.Client(intents=intents)
        self.connected = False

        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)

    async def on_ready(self):
        self.connected = True
        logger.info(f"✅ Discord bot ready! Logged in as {self.bot.user}")
        logger.info(f"📡 Listening to channel ID: {self.channel_id}")

    async def on_message(self, message: discord.Message):
        if message.channel.id != self.channel_id:
            return
        if message.id in self.processed_message_ids:
            return
        if not message.embeds:
            return

        self.processed_message_ids.add(message.id)
        if len(self.processed_message_ids) > Config.MAX_MESSAGE_IDS:
            self.processed_message_ids = set(list(self.processed_message_ids)[-Config.MAX_MESSAGE_IDS:])

        logger.info(f"📨 Message with {len(message.embeds)} embed(s) (ID: {message.id})")

        for idx, embed in enumerate(message.embeds):
            try:
                embed_dict = {
                    "title":       embed.title,
                    "description": embed.description,
                    "thumbnail":   {"url": embed.thumbnail.url} if embed.thumbnail else None,
                    "fields":      [{"name": f.name, "value": f.value} for f in embed.fields]
                }
                result        = PetParser.parse_embed(embed_dict)
                server_id     = result["server_id"]
                pet_info_list = result["pet_info_list"]
                duel_mode     = result["duel_mode"]

                if not server_id or not pet_info_list:
                    continue

                for pet_info in pet_info_list:
                    uid = f"{server_id}_{pet_info.name}"
                    if self.processed_servers.contains(uid):
                        continue
                    job = Job(
                        server_id=server_id, pet_name=pet_info.name,
                        pet_value=pet_info.value, pet_thumbnail=pet_info.thumbnail,
                        retries=Config.DEFAULT_RETRIES, retry_delay=Config.DEFAULT_RETRY_DELAY,
                        created_time=time.time(), is_priority=pet_info.is_priority,
                        duel_mode=duel_mode
                    )
                    if self.job_queue.add_job(job, unique_tracking_id=uid):
                        broadcast_job_to_ws_clients(job)

            except Exception as e:
                logger.error(f"❌ Error processing embed {idx + 1}: {e}")

    async def start(self):
        try:
            await self.bot.start(self.token)
        except Exception as e:
            logger.error(f"❌ Discord bot error: {e}")
            self.connected = False

    async def close(self):
        await self.bot.close()
        self.connected = False


# ============================================================================
# Flask App & SocketIO
# ============================================================================

job_queue_manager         = JobQueueManager()
processed_servers_manager = ProcessedServersManager()

app = Flask(__name__)
app.config['SECRET_KEY'] = Config.SECRET_KEY

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='threading',
    logger=False,
    engineio_logger=False,
    transports=['polling', 'websocket'],
    allow_upgrades=True,
    ping_timeout=60,
    ping_interval=25
)

active_clients: Set[str]              = set()
client_lock                           = Lock()
client_connection_times: Dict[str, float] = {}


def _ws_auth(data) -> bool:
    if not isinstance(data, dict):
        return False
    token = data.get("_t") or data.get("token") or ""
    return hmac.compare_digest(str(token), Config.AUTH_TOKEN)


# ============================================================================
# WebSocket Handlers — all require valid Luarmor key on register
# ============================================================================

# ── /ws namespace ─────────────────────────────────────────────────
@socketio.on('connect', namespace='/ws')
def handle_raw_ws_connect(auth=None):
    client_id    = request.args.get('client_id') or request.sid
    connect_time = time.time()

    token = request.args.get("_t", "")
    if not hmac.compare_digest(token, Config.AUTH_TOKEN):
        logger.warning(f"🔒 WS /ws unauthorized connect from {_get_client_ip()}")
        return False

    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time

    with ws_clients_lock:
        ws_clients[client_id] = request.sid

    emit('connected', signed({'status': 'ok', 'client_id': client_id}), namespace='/ws')
    logger.info(f"🔌 WS /ws connected: {client_id[:8]}...")

    # Only send job if client has been validated via client_loaded
    if is_client_validated(client_id):
        job = job_queue_manager.get_job_for_client(client_id, set())
        emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}), namespace='/ws')
    else:
        emit('job', signed({"has_job": False}), namespace='/ws')


@socketio.on('disconnect', namespace='/ws')
def handle_raw_ws_disconnect():
    client_id = request.args.get('client_id') or request.sid
    with ws_clients_lock:
        ws_clients.pop(client_id, None)
    with client_lock:
        active_clients.discard(client_id)
        client_connection_times.pop(client_id, None)
    remove_validated_client(client_id)
    logger.info(f"🔌 WS /ws disconnected: {client_id[:8]}...")


@socketio.on('register', namespace='/ws')
def handle_raw_ws_register(data):
    if not _ws_auth(data):
        logger.warning(f"🔒 WS /ws register rejected — bad token")
        return

    client_id    = data.get('client_id', request.sid)
    key          = data.get('key', '')
    connect_time = time.time()

    # ── Luarmor key check ──────────────────────────────────────────
    if not validate_luarmor_key(key):
        logger.warning(f"🔒 WS /ws register rejected — invalid Luarmor key from {client_id[:8]}...")
        emit('error', {'message': 'Invalid key'}, namespace='/ws')
        return
    # ──────────────────────────────────────────────────────────────

    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    with ws_clients_lock:
        ws_clients[client_id] = request.sid

    mark_client_validated(client_id)

    emit('registered', signed({"status": "ok", "client_id": client_id}), namespace='/ws')
    job = job_queue_manager.get_job_for_client(client_id, set())
    emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}), namespace='/ws')


@socketio.on('clear_job', namespace='/ws')
def handle_raw_ws_clear_job(data):
    if not _ws_auth(data):
        return
    client_id = data.get('client_id', request.sid)
    server_id = data.get('server_id')
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared /ws: {server_id[:8]}... by {client_id[:8]}...")
        if is_client_validated(client_id):
            job = job_queue_manager.get_job_for_client(client_id, set())
            emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}), namespace='/ws')


@socketio.on('ping', namespace='/ws')
def handle_raw_ws_ping():
    emit('pong', signed({}), namespace='/ws')


# ── /jobs namespace ────────────────────────────────────────────────
@socketio.on('connect', namespace='/jobs')
def handle_jobs_connect(auth=None):
    token = request.args.get("_t", "")
    if not hmac.compare_digest(token, Config.AUTH_TOKEN):
        logger.warning(f"🔒 WS /jobs unauthorized connect from {_get_client_ip()}")
        return False

    client_id    = (auth.get('client_id', request.sid) if auth else None) or request.sid
    connect_time = time.time()

    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time

    with jobs_clients_lock:
        jobs_clients[client_id] = request.sid

    emit('connected', signed({'status': 'ok', 'client_id': client_id}), namespace='/jobs')
    logger.info(f"🔌 WS /jobs connected: {client_id[:8]}...")

    if is_client_validated(client_id):
        job = job_queue_manager.get_job_for_client(client_id, set())
        emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}), namespace='/jobs')
    else:
        emit('job', signed({"has_job": False}), namespace='/jobs')


@socketio.on('register', namespace='/jobs')
def handle_jobs_register(data):
    if not _ws_auth(data):
        return

    client_id    = data.get('client_id', request.sid)
    key          = data.get('key', '')
    connect_time = time.time()

    # ── Luarmor key check ──────────────────────────────────────────
    if not validate_luarmor_key(key):
        logger.warning(f"🔒 WS /jobs register rejected — invalid Luarmor key from {client_id[:8]}...")
        emit('error', {'message': 'Invalid key'}, namespace='/jobs')
        return
    # ──────────────────────────────────────────────────────────────

    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    with jobs_clients_lock:
        jobs_clients[client_id] = request.sid

    mark_client_validated(client_id)

    emit('registered', signed({"status": "ok", "client_id": client_id}), namespace='/jobs')
    job = job_queue_manager.get_job_for_client(client_id, set())
    emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}), namespace='/jobs')


@socketio.on('clear_job', namespace='/jobs')
def handle_jobs_clear_job(data):
    if not _ws_auth(data):
        return
    client_id = data.get('client_id', request.sid)
    server_id = data.get('server_id')
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared /jobs: {server_id[:8]}... by {client_id[:8]}...")
        if is_client_validated(client_id):
            job = job_queue_manager.get_job_for_client(client_id, set())
            emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}), namespace='/jobs')


# ── Default namespace ──────────────────────────────────────────────
@socketio.on('connect')
def handle_connect(auth=None):
    client_id    = (auth.get('client_id', request.sid) if auth and isinstance(auth, dict) else None) or request.sid
    connect_time = time.time()
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = connect_time
    emit('connected', signed({'status': 'ok', 'client_id': client_id}))
    logger.info(f"🔌 Client connected (default ns): {client_id[:8]}...")


@socketio.on('get_job')
def handle_get_job(data):
    if not _ws_auth(data):
        return
    client_id = data.get('client_id', 'unknown')
    if not is_client_validated(client_id):
        emit('job_response', signed({"has_job": False}))
        return
    connect_time = time.time()
    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    job = job_queue_manager.get_job_for_client(client_id, set())
    emit('job_response', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}))


@socketio.on('register')
def handle_register(data):
    if not _ws_auth(data):
        return

    client_id    = data.get('client_id', 'unknown')
    key          = data.get('key', '')
    connect_time = time.time()

    # ── Luarmor key check ──────────────────────────────────────────
    if not validate_luarmor_key(key):
        logger.warning(f"🔒 Default ns register rejected — invalid Luarmor key from {client_id[:8]}...")
        emit('error', {'message': 'Invalid key'})
        return
    # ──────────────────────────────────────────────────────────────

    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = connect_time

    mark_client_validated(client_id)

    emit('registered', signed({"status": "ok", "client_id": client_id}))
    job = job_queue_manager.get_job_for_client(client_id, set())
    emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}))


@socketio.on('clear_job')
def handle_clear_job(data):
    if not _ws_auth(data):
        return
    client_id = data.get('client_id', 'unknown')
    server_id = data.get('server_id')
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared (default ns): {server_id[:8]}... by {client_id[:8]}...")
        if is_client_validated(client_id):
            job = job_queue_manager.get_job_for_client(client_id, set())
            emit('job', signed({"has_job": True, **job.to_dict()}) if job else signed({"has_job": False}))


# ============================================================================
# HTTP Routes
# ============================================================================

@app.route('/ws', methods=['GET'])
def ws_upgrade():
    return jsonify(signed({
        "status":  "websocket_endpoint",
        "message": "Use SocketIO endpoint with ?_t=YOUR_TOKEN"
    })), 426


@app.route('/get_job', methods=['GET'])
@require_auth
def get_job():
    client_id    = request.args.get('client_id', 'unknown')
    since        = float(request.args.get('since', 0))
    connect_time = time.time()

    with client_lock:
        active_clients.add(client_id)
        if since > 0:
            client_connection_times[client_id] = since
        elif client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time

    # ── Only serve jobs to validated clients ───────────────────────
    if not is_client_validated(client_id):
        logger.warning(f"🔒 /get_job blocked unvalidated client: {client_id[:8]}...")
        return jsonify(signed({"has_job": False}))
    # ──────────────────────────────────────────────────────────────

    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        logger.info(f"📤 Sending job to {client_id[:8]}...: {job.pet_name} | {job.format_duel()}")
        return jsonify(signed({"has_job": True, **job.to_dict()}))
    return jsonify(signed({"has_job": False}))


@app.route('/clear_job', methods=['POST'])
@require_auth
def clear_job():
    client_id = request.args.get('client_id', 'unknown')
    server_id = request.args.get('server_id')
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared: {server_id[:8]}... by {client_id[:8]}...")
    return jsonify(signed({"status": "cleared", "queue_size": job_queue_manager.size()}))


@app.route('/client_loaded', methods=['POST'])
@require_auth
def client_loaded():
    client_id = request.args.get('client_id', 'unknown')
    key       = request.args.get('key', '')

    # ── Luarmor key check ──────────────────────────────────────────
    if not validate_luarmor_key(key):
        logger.warning(f"🔒 /client_loaded rejected — invalid Luarmor key from {client_id[:8]}...")
        return jsonify({"error": "Invalid key"}), 403
    # ──────────────────────────────────────────────────────────────

    load_time = time.time()
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = load_time

    mark_client_validated(client_id)
    logger.info(f"✅ Client loaded + Luarmor validated: {client_id[:8]}...")
    return jsonify(signed({"status": "ok", "load_time": load_time}))


@app.route('/get_client_ids', methods=['GET'])
@require_auth
def get_client_ids():
    # Only return validated clients so unvalidated leechers don't show up in ESP
    with validated_lock:
        ids = list(validated_clients)
    return jsonify(signed({"client_ids": ids, "count": len(ids)}))


@app.route('/register_peer', methods=['POST'])
@require_auth
def register_peer():
    client_id = request.args.get('client_id', 'unknown')
    job_id    = request.args.get('job_id', '')

    # Only allow validated clients to register as peers
    if not is_client_validated(client_id):
        logger.warning(f"🔒 /register_peer blocked unvalidated client: {client_id[:8]}...")
        return jsonify({"error": "Not validated"}), 403

    with client_lock:
        active_clients.add(client_id)
    logger.info(f"👥 Peer registered: {client_id[:8]}... job: {job_id[:8] if job_id else 'N/A'}...")
    return jsonify(signed({"status": "ok"}))


@app.route('/get_peers', methods=['GET'])
@require_auth
def get_peers():
    job_id    = request.args.get('job_id', '')
    client_id = request.args.get('client_id', '')
    # Only return validated peers
    with validated_lock:
        peers = [uid for uid in validated_clients if uid != client_id]
    return jsonify(signed({"peers": peers}))


@app.route('/status', methods=['GET'])
def status():
    queue_stats = job_queue_manager.get_stats()
    with client_lock:
        client_count = len(active_clients)
    with validated_lock:
        validated_count = len(validated_clients)
    return jsonify({
        "status":                 "online",
        "discord_bot_connected":  discord_bot.connected if discord_bot else False,
        "queue_size":             queue_stats["size"],
        "processed_servers":      processed_servers_manager.size(),
        "active_clients":         client_count,
        "validated_clients":      validated_count,
        **queue_stats
    })


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status":                "online",
        "service":               "Shadow Notifier API",
        "discord_bot_connected": discord_bot.connected if discord_bot else False
    })


@app.route('/admin/blacklist', methods=['POST'])
@require_auth
def admin_blacklist():
    ip = request.args.get('ip', '').strip()
    if not ip:
        return jsonify({"error": "ip param required"}), 400
    IP_BLACKLIST.add(ip)
    logger.warning(f"🚫 IP manually blacklisted: {ip}")
    return jsonify(signed({"status": "blacklisted", "ip": ip}))


@app.route('/admin/unblacklist', methods=['POST'])
@require_auth
def admin_unblacklist():
    ip = request.args.get('ip', '').strip()
    IP_BLACKLIST.discard(ip)
    return jsonify(signed({"status": "removed", "ip": ip}))


# ============================================================================
# WebSocket Broadcasting — only to validated clients
# ============================================================================

ws_event_loop:    Optional[asyncio.AbstractEventLoop] = None
ws_clients:       Dict[str, str] = {}
jobs_clients:     Dict[str, str] = {}
ws_clients_lock   = Lock()
jobs_clients_lock = Lock()


def broadcast_job_to_ws_clients(job: Job):
    job_data = signed({"has_job": True, **job.to_dict()})

    with client_lock:
        eligible = [
            cid for cid, lt in client_connection_times.items()
            if lt > 0 and lt < job.created_time
        ]

    # ── Only broadcast to Luarmor-validated clients ────────────────
    with validated_lock:
        eligible = [cid for cid in eligible if cid in validated_clients]
    # ──────────────────────────────────────────────────────────────

    if not eligible:
        logger.info(f"⏭️ No eligible validated clients for broadcast")
        return

    sent_ws, sent_jobs = 0, 0

    with ws_clients_lock:
        for cid in eligible:
            if cid in ws_clients:
                try:
                    socketio.emit('job', job_data, namespace='/ws', room=ws_clients[cid])
                    sent_ws += 1
                except Exception as e:
                    logger.warning(f"WS /ws send failed for {cid[:8]}...: {e}")

    with jobs_clients_lock:
        for cid in eligible:
            if cid in jobs_clients:
                try:
                    socketio.emit('job', job_data, namespace='/jobs', room=jobs_clients[cid])
                    sent_jobs += 1
                except Exception as e:
                    logger.warning(f"WS /jobs send failed for {cid[:8]}...: {e}")

    logger.info(f"📤 Broadcasted to {sent_ws} (/ws) + {sent_jobs} (/jobs): {job.pet_name} | {job.format_duel()}")


def run_websocket_server():
    global ws_event_loop
    ws_event_loop = asyncio.new_event_loop()

    def run_loop():
        asyncio.set_event_loop(ws_event_loop)
        ws_event_loop.run_until_complete(asyncio.sleep(0))

    Thread(target=run_loop, daemon=True).start()


# ============================================================================
# Main
# ============================================================================

discord_bot: Optional[DiscordBot] = None


def run_flask():
    socketio.run(
        app,
        host='0.0.0.0',
        port=Config.PORT,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
        log_output=False
    )


def run_discord_bot():
    global discord_bot
    discord_bot = DiscordBot(
        token=Config.DISCORD_TOKEN,
        channel_id=Config.SPECIAL_CHANNEL_ID,
        job_queue=job_queue_manager,
        processed_servers=processed_servers_manager
    )
    asyncio.run(discord_bot.start())


if __name__ == "__main__":
    if not Config.DISCORD_TOKEN:
        logger.error("❌ DISCORD_TOKEN environment variable not set!")
        exit(1)

    if Config.AUTH_TOKEN == "CHANGE_THIS_SECRET_TOKEN":
        logger.warning("⚠️  WARNING: You are using the default AUTH_TOKEN — set SHADOW_AUTH_TOKEN in env!")

    run_websocket_server()

    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()

    logger.info(f"🌐 Flask API started on port {Config.PORT}")
    logger.info(f"🔒 Auth token protection: ENABLED")
    logger.info(f"⚡ Rate limiting: {Config.RATE_LIMIT_MAX} req/{Config.RATE_LIMIT_WINDOW}s per IP")
    logger.info(f"🚫 IP blacklist loaded: {len(IP_BLACKLIST)} entries")
    logger.info(f"✍️  Response signing: ENABLED (sig={Config.RESPONSE_SIG})")
    logger.info(f"🔑 Luarmor key validation: ENABLED (cache TTL: {Config.KEY_CACHE_TTL}s)")
    logger.info("⏳ Starting Discord Bot...\n")

    run_discord_bot()
