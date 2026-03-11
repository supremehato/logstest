"""
Improved Discord Gateway WebSocket Implementation
Key improvements:
- Better error handling and logging
- Connection pooling and retry logic
- Memory management for tracking structures
- Optimized pet parsing with regex compilation
- Async-first design
- Better separation of concerns
"""
import asyncio
import aiohttp
import json
import time
import logging
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# Configuration & Constants
# ============================================================================

class Config:
    """Centralized configuration"""
    DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
    SPECIAL_CHANNEL_ID = int(os.environ.get("SPECIAL_CHANNEL_ID", "1447173921261490187"))
    PORT = int(os.environ.get("PORT", 5000))
    SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key-change-this")
    
    # Limits
    MAX_QUEUE_SIZE = 1000
    MAX_PROCESSED_SERVERS = 10000
    MAX_MESSAGE_IDS = 5000
    CLIENT_RESET_INTERVAL = 1800  # 30 minutes
    PROCESSED_SERVER_TTL = 3600  # 1 hour
    
    # Retry settings
    DEFAULT_RETRIES = 3
    DEFAULT_RETRY_DELAY = 2
    GATEWAY_RECONNECT_DELAY = 5
    MAX_RECONNECT_DELAY = 300  # 5 minutes


# Priority pets (ordered highest to lowest)
PRIORITY_PETS = [
    "Strawberry Elephant",
    "Meowl",
    "Skibidi Toilet",
    "Dragon Gingerini",
    "Dragon Cannelloni",
    "Headless Horseman"
]

PRIORITY_PETS_SET = set(p.lower() for p in PRIORITY_PETS)
PRIORITY_PETS_INDEX = {p.lower(): i for i, p in enumerate(PRIORITY_PETS)}


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Job:
    """Job data structure"""
    server_id: str
    pet_name: str
    pet_value: float
    pet_thumbnail: Optional[str]
    retries: int
    retry_delay: int
    created_time: float
    is_priority: bool = False
    duel_mode: Optional[bool] = None  # True = Active, False = Inactive, None = not present
    
    def format_value(self) -> str:
        """Format pet value as K/M/B with /s"""
        if self.pet_value >= 1_000_000_000:
            return f"${self.pet_value / 1_000_000_000:.2f}B/s"
        elif self.pet_value >= 1_000_000:
            return f"${self.pet_value / 1_000_000:.2f}M/s"
        elif self.pet_value >= 1_000:
            return f"${self.pet_value / 1_000:.2f}K/s"
        else:
            return f"${self.pet_value:.2f}/s"
    
    def format_duel(self) -> str:
        """Format duel mode for logging"""
        if self.duel_mode is True:
            return "Duel: ✅ Active"
        elif self.duel_mode is False:
            return "Duel: ❌ Inactive"
        else:
            return "Duel: ➖ N/A"
    
    def to_dict(self):
        data = asdict(self)
        data['pet_value_formatted'] = self.format_value()
        data['duel_mode_label'] = self.format_duel()
        return data


@dataclass
class PetInfo:
    """Pet information structure"""
    name: Optional[str]
    value: float
    thumbnail: Optional[str] = None
    is_priority: bool = False
    count: int = 1


# ============================================================================
# Pet Parsing (Optimized with compiled regex)
# ============================================================================

class PetParser:
    """Optimized pet parsing with compiled regex patterns"""
    
    # Compile regex patterns once
    VALUE_PATTERN = re.compile(
        r'([A-Za-z\s\-\'\.\(\)]+?)\s*[:\-]?\s*\$?\s*([\d\.,]+)\s*([MBK]?)\s*/s',
        re.IGNORECASE
    )
    SERVER_ID_PATTERN = re.compile(r'[a-f0-9]{32,}', re.IGNORECASE)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Remove code blocks and backticks"""
        if not text:
            return ""
        return text.replace("```", "").replace("`", "").strip()
    
    @staticmethod
    def parse_value(value_str: str) -> float:
        """Parse value string with K/M/B suffixes"""
        try:
            value_str = value_str.replace(",", "").upper().strip()
            
            if "B" in value_str:
                return float(value_str.replace("B", "").strip()) * 1_000_000_000
            elif "M" in value_str:
                return float(value_str.replace("M", "").strip()) * 1_000_000
            elif "K" in value_str:
                return float(value_str.replace("K", "").strip()) * 1_000
            else:
                # Extract digits and decimal point
                filtered = "".join(ch for ch in value_str if ch.isdigit() or ch == '.')
                return float(filtered) if filtered else 0
        except (ValueError, AttributeError):
            return 0
    
    @classmethod
    def extract_pet_from_line(cls, line: str) -> Tuple[Optional[str], float]:
        """Extract pet name and value from a single line"""
        line = line.strip()
        if not line:
            return None, 0
        
        pet_name = None
        value = 0
        
        # Try dollar sign split first
        if "$" in line:
            parts = line.split("$", 1)
            if len(parts) >= 2:
                pet_name = parts[0].strip().rstrip(":-").strip()
                value = cls.parse_value(parts[1].replace("/s", ""))
        
        # Try regex pattern if not found
        if not pet_name or value == 0:
            match = cls.VALUE_PATTERN.search(line)
            if match:
                pet_name = match.group(1).strip().rstrip(":-").strip()
                num_part = match.group(2).replace(",", "")
                suffix = match.group(3).upper()
                
                try:
                    base_value = float(num_part)
                    multipliers = {"B": 1e9, "M": 1e6, "K": 1e3}
                    value = base_value * multipliers.get(suffix, 1)
                except ValueError:
                    value = 0
        
        return pet_name, value if pet_name else 0
    
    @classmethod
    def extract_priority_pets(cls, text: str) -> Optional[PetInfo]:
        """Parse priority pets and return highest priority one"""
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
            
            # Check if it's a priority pet (case-insensitive)
            pet_name_lower = pet_name.lower()
            if pet_name_lower not in PRIORITY_PETS_SET:
                continue
            
            # Find the actual priority pet name (with correct casing)
            matched_priority = next(
                (p for p in PRIORITY_PETS if p.lower() == pet_name_lower),
                None
            )
            
            if not matched_priority:
                continue
            
            if matched_priority not in found:
                found[matched_priority] = {"value": value, "count": 1}
            else:
                found[matched_priority]["count"] += 1
                found[matched_priority]["value"] = max(
                    found[matched_priority]["value"], value
                )
        
        if not found:
            return None
        
        # Find highest priority pet (lowest index)
        highest_priority_pet = min(
            found.keys(),
            key=lambda p: PRIORITY_PETS_INDEX.get(p.lower(), len(PRIORITY_PETS))
        )
        
        pet_data = found[highest_priority_pet]
        count = pet_data["count"]
        name = f"{count}x {highest_priority_pet}" if count > 1 else highest_priority_pet
        
        return PetInfo(
            name=name,
            value=pet_data["value"],
            is_priority=False,  # Don't mark as priority - let client decide based on GUI selection
            count=count
        )
    
    @classmethod
    def parse_embed(cls, embed_dict: dict) -> Dict:
        """Parse embed from Gateway JSON format - returns ALL pets found (priority and non-priority) as separate entries"""
        server_id = None
        pet_thumbnail = None
        duel_mode = None  # None = not present, True = Active, False = Inactive
        
        # Extract thumbnail
        if embed_dict.get("thumbnail") and embed_dict["thumbnail"].get("url"):
            pet_thumbnail = embed_dict["thumbnail"]["url"]
        
        # Extract fields
        fields = embed_dict.get("fields", [])
        
        # Find server ID and duel mode in fields
        for field in fields:
            field_name = field.get("name", "").lower()
            field_value = cls.clean_text(field.get("value", ""))
            
            # Server ID
            if not server_id and (
                ("server" in field_name and "id" in field_name) or
                ("job" in field_name and "id" in field_name)
            ):
                server_id = field_value

            # Duel Mode
            if "duel" in field_name:
                field_value_lower = field_value.lower()
                if "active" in field_value_lower:
                    duel_mode = True
                elif "inactive" in field_value_lower:
                    duel_mode = False
        
        # Check description and title for server ID if not found
        if not server_id:
            for text in [embed_dict.get("description", ""), embed_dict.get("title", "")]:
                match = cls.SERVER_ID_PATTERN.search(text)
                if match:
                    server_id = match.group()
                    break
        
        # Find ALL pets (both priority and non-priority) - send each as separate job
        all_pets_found: Dict[str, Dict] = {}  # pet_name -> {value, name}
        
        # Check "highest pet" field
        for field in fields:
            field_name = field.get("name", "").lower()
            if "highest" in field_name and "pet" in field_name:
                field_value = cls.clean_text(field.get("value", ""))
                name, value = cls.extract_pet_from_line(field_value)
                if name and value > 0:
                    if name not in all_pets_found:
                        all_pets_found[name] = {"value": value, "name": name}
                    else:
                        all_pets_found[name]["value"] = max(
                            all_pets_found[name]["value"], value
                        )
                break
        
        # Check "other pet" field for ALL pets
        for field in fields:
            field_name = field.get("name", "").lower()
            if "other pet" in field_name:
                field_value = cls.clean_text(field.get("value", ""))
                lines = [l.strip() for l in field_value.split("\n") if l.strip()]
                
                for line in lines:
                    if "-" not in line and "$" not in line:
                        continue
                    
                    name, value = cls.extract_pet_from_line(line)
                    if not name or value == 0:
                        continue
                    
                    if name not in all_pets_found:
                        all_pets_found[name] = {"value": value, "name": name}
                    else:
                        all_pets_found[name]["value"] = max(
                            all_pets_found[name]["value"], value
                        )
                break
        
        # Return ALL pets found (both priority and non-priority) as separate entries
        all_pets_list: List[PetInfo] = []
        
        if all_pets_found:
            priority_pets = []
            regular_pets = []
            
            for pet_name, pet_data in all_pets_found.items():
                pet_name_lower = pet_name.lower()
                matched_priority = next(
                    (p for p in PRIORITY_PETS if p.lower() == pet_name_lower),
                    None
                )
                
                pet_info = PetInfo(
                    name=pet_data["name"],
                    value=pet_data["value"],
                    thumbnail=pet_thumbnail,
                    is_priority=False
                )
                
                if matched_priority:
                    priority_pets.append((matched_priority, pet_info))
                else:
                    regular_pets.append(pet_info)
            
            priority_pets.sort(key=lambda x: PRIORITY_PETS_INDEX.get(x[0].lower(), len(PRIORITY_PETS)))
            
            for _, pet_info in priority_pets:
                all_pets_list.append(pet_info)
            
            for pet_info in regular_pets:
                all_pets_list.append(pet_info)
        
        return {
            "server_id": server_id,
            "pet_info_list": all_pets_list,
            "duel_mode": duel_mode  # True / False / None
        }


# ============================================================================
# Job Queue Manager
# ============================================================================

class JobQueueManager:
    """Thread-safe job queue with LRU limits"""
    
    def __init__(self, max_size: int = Config.MAX_QUEUE_SIZE):
        self.queue: deque[Job] = deque(maxlen=max_size)
        self.lock = Lock()
        # Track jobs sent to clients: (server_id, pet_name) -> set of client_ids
        self.job_sent_to_clients: Dict[Tuple[str, str], Set[str]] = {}
        self.sent_servers: Set[str] = set()
    
    def add_job(self, job: Job, unique_tracking_id: Optional[str] = None) -> bool:
        """Add job to queue, updating if better value exists for same pet+server combo"""
        with self.lock:
            tracking_id = unique_tracking_id or job.server_id
            
            for idx, existing_job in enumerate(self.queue):
                if existing_job.server_id == job.server_id and existing_job.pet_name == job.pet_name:
                    if job.pet_value > existing_job.pet_value:
                        del self.queue[idx]
                        self.queue.append(job)
                        logger.info(
                            f"🔄 Updated job {job.server_id[:8]}... "
                            f"{existing_job.pet_name} → {job.pet_name} "
                            f"({job.format_value()}) | {job.format_duel()}"
                        )
                        return True
                    return False
            
            # Add new job
            self.queue.append(job)
            self.sent_servers.add(tracking_id)
            logger.info(
                f"🚀 NEW JOB: {job.server_id[:8]}... "
                f"Pet: {job.pet_name} ({job.format_value()}) | "
                f"{job.format_duel()} "
                f"[Queue: {len(self.queue)}]"
            )
            return True
    
    def get_job_for_client(self, client_id: str, processed_servers: Set[str]) -> Optional[Job]:
        """Get next available job for client (only jobs created AFTER client loaded)"""
        with self.lock:
            client_connect_time = client_connection_times.get(client_id, 0)
            
            for job in self.queue:
                server_id = job.server_id
                pet_name = job.pet_name
                job_key = (server_id, pet_name)
                
                if server_id in processed_servers:
                    continue
                
                if job_key in self.job_sent_to_clients:
                    if client_id in self.job_sent_to_clients[job_key]:
                        continue
                
                if client_connect_time > 0 and job.created_time <= client_connect_time:
                    continue
                
                if job_key not in self.job_sent_to_clients:
                    self.job_sent_to_clients[job_key] = set()
                self.job_sent_to_clients[job_key].add(client_id)
                
                return job
            
            return None
    
    def remove_job(self, server_id: str, pet_name: Optional[str] = None):
        """Remove job from queue."""
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
                "size": len(self.queue),
                "sent_servers": len(self.sent_servers),
                "tracked_jobs": len(self.job_sent_to_clients)
            }


# ============================================================================
# Processed Servers Manager (with TTL)
# ============================================================================

class ProcessedServersManager:
    """Manage processed servers with automatic cleanup"""
    
    def __init__(self, max_size: int = Config.MAX_PROCESSED_SERVERS, 
                 ttl: int = Config.PROCESSED_SERVER_TTL):
        self.servers: Dict[str, Dict] = {}
        self.lock = Lock()
        self.max_size = max_size
        self.ttl = ttl
        self.last_cleanup = time.time()
    
    def add(self, server_id: str, client_id: str):
        with self.lock:
            if server_id not in self.servers:
                self.servers[server_id] = {
                    "clients": set(),
                    "timestamp": datetime.now()
                }
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
        self.servers = {
            sid: data for sid, data in self.servers.items()
            if data["timestamp"] > cutoff
        }
        self.last_cleanup = time.time()
        logger.info(f"🧹 Cleaned up processed servers. Remaining: {len(self.servers)}")
    
    def size(self) -> int:
        with self.lock:
            return len(self.servers)


# ============================================================================
# Discord Bot (using discord.py)
# ============================================================================

class DiscordBot:
    """Simple Discord bot using discord.py to listen for messages"""
    
    def __init__(self, token: str, channel_id: int, job_queue: JobQueueManager,
                 processed_servers: ProcessedServersManager):
        self.token = token
        self.channel_id = channel_id
        self.job_queue = job_queue
        self.processed_servers = processed_servers
        self.processed_message_ids: Set[int] = set()
        
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guild_messages = True
        
        self.bot = discord.Client(intents=intents)
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
                    "title": embed.title,
                    "description": embed.description,
                    "thumbnail": {"url": embed.thumbnail.url} if embed.thumbnail else None,
                    "fields": [
                        {
                            "name": field.name,
                            "value": field.value
                        }
                        for field in embed.fields
                    ]
                }
                
                result = PetParser.parse_embed(embed_dict)
                server_id = result["server_id"]
                pet_info_list = result["pet_info_list"]
                duel_mode = result["duel_mode"]  # True / False / None
                
                if not server_id or not pet_info_list:
                    continue
                
                for pet_info in pet_info_list:
                    unique_tracking_id = f"{server_id}_{pet_info.name}"
                    
                    if self.processed_servers.contains(unique_tracking_id):
                        continue
                    
                    job = Job(
                        server_id=server_id,
                        pet_name=pet_info.name,
                        pet_value=pet_info.value,
                        pet_thumbnail=pet_info.thumbnail,
                        retries=Config.DEFAULT_RETRIES,
                        retry_delay=Config.DEFAULT_RETRY_DELAY,
                        created_time=time.time(),
                        is_priority=pet_info.is_priority,
                        duel_mode=duel_mode  # Pass duel mode into the job
                    )
                    
                    if self.job_queue.add_job(job, unique_tracking_id=unique_tracking_id):
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
# Flask API & SocketIO
# ============================================================================

job_queue_manager = JobQueueManager()
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

active_clients: Set[str] = set()
client_lock = Lock()
client_connection_times: Dict[str, float] = {}

@socketio.on('connect', namespace='/ws')
def handle_raw_ws_connect(auth=None):
    client_id = request.args.get('client_id') or request.sid
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with ws_clients_lock:
        ws_clients[client_id] = request.sid
    
    emit('connected', {'status': 'ok', 'client_id': client_id}, namespace='/ws')
    logger.info(f"🔌 Raw WebSocket client connected: {client_id[:8]}... (load time: {client_connection_times.get(client_id, connect_time):.2f})")
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {"has_job": True, **job.to_dict()}, namespace='/ws')
    else:
        emit('job', {"has_job": False}, namespace='/ws')


@socketio.on('disconnect', namespace='/ws')
def handle_raw_ws_disconnect():
    client_id = request.args.get('client_id') or request.sid
    
    with ws_clients_lock:
        ws_clients.pop(client_id, None)
    
    with client_lock:
        active_clients.discard(client_id)
        client_connection_times.pop(client_id, None)
    
    logger.info(f"🔌 Raw WebSocket client disconnected: {client_id[:8]}...")


@socketio.on('register', namespace='/ws')
def handle_raw_ws_register(data):
    client_id = data.get('client_id', request.sid)
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with ws_clients_lock:
        ws_clients[client_id] = request.sid
    
    emit('registered', {"status": "ok", "client_id": client_id}, namespace='/ws')
    logger.info(f"🔌 Raw WebSocket client registered: {client_id[:8]}... (load time: {client_connection_times.get(client_id, connect_time):.2f})")
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {"has_job": True, **job.to_dict()}, namespace='/ws')
    else:
        emit('job', {"has_job": False}, namespace='/ws')


@socketio.on('clear_job', namespace='/ws')
def handle_raw_ws_clear_job(data):
    client_id = data.get('client_id', request.sid)
    server_id = data.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared via raw WS: {server_id[:8]}... by {client_id[:8]}...")
        
        job = job_queue_manager.get_job_for_client(client_id, set())
        if job:
            emit('job', {"has_job": True, **job.to_dict()}, namespace='/ws')
        else:
            emit('job', {"has_job": False}, namespace='/ws')


@socketio.on('ping', namespace='/ws')
def handle_raw_ws_ping():
    emit('pong', {}, namespace='/ws')


@socketio.on('connect', namespace='/jobs')
def handle_jobs_connect(auth):
    client_id = auth.get('client_id', request.sid) if auth else request.sid
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with jobs_clients_lock:
        jobs_clients[client_id] = request.sid
    
    emit('connected', {'status': 'ok', 'client_id': client_id}, namespace='/jobs')
    logger.info(f"🔌 Client connected to /jobs namespace: {client_id[:8]}... (load time: {client_connection_times.get(client_id, connect_time):.2f})")
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {"has_job": True, **job.to_dict()}, namespace='/jobs')
    else:
        emit('job', {"has_job": False}, namespace='/jobs')


@socketio.on('register', namespace='/jobs')
def handle_jobs_register(data):
    client_id = data.get('client_id', request.sid)
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with jobs_clients_lock:
        jobs_clients[client_id] = request.sid
    
    emit('registered', {"status": "ok", "client_id": client_id}, namespace='/jobs')
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {"has_job": True, **job.to_dict()}, namespace='/jobs')
    else:
        emit('job', {"has_job": False}, namespace='/jobs')


@socketio.on('clear_job', namespace='/jobs')
def handle_jobs_clear_job(data):
    client_id = data.get('client_id', request.sid)
    server_id = data.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared via WS: {server_id[:8]}... by {client_id[:8]}...")
        
        job = job_queue_manager.get_job_for_client(client_id, set())
        if job:
            emit('job', {"has_job": True, **job.to_dict()}, namespace='/jobs')
        else:
            emit('job', {"has_job": False}, namespace='/jobs')


@app.route('/ws', methods=['GET'])
def ws_upgrade():
    return jsonify({
        "status": "websocket_endpoint",
        "message": "Use SocketIO endpoint: /socket.io/?transport=websocket&EIO=4&namespace=/ws"
    }), 426


@app.route('/get_job', methods=['GET'])
def get_job():
    client_id = request.args.get('client_id', 'unknown')
    since = float(request.args.get('since', 0))
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if since > 0:
            client_connection_times[client_id] = since
        elif client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    
    if job:
        logger.info(f"📤 Sending job to {client_id[:8]}...: {job.pet_name} | {job.format_duel()}")
        return jsonify({"has_job": True, **job.to_dict()})
    
    return jsonify({"has_job": False})


@app.route('/clear_job', methods=['POST'])
def clear_job():
    client_id = request.args.get('client_id', 'unknown')
    server_id = request.args.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared: {server_id[:8]}... by {client_id[:8]}...")
    
    return jsonify({"status": "cleared", "queue_size": job_queue_manager.size()})


@app.route('/client_loaded', methods=['POST'])
def client_loaded():
    client_id = request.args.get('client_id', 'unknown')
    load_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = load_time
    
    logger.info(f"✅ Client loaded: {client_id[:8]}... (load time: {load_time:.2f}) - Will skip ALL jobs created before this time")
    return jsonify({"status": "ok", "load_time": load_time})


@app.route('/get_client_ids', methods=['GET'])
def get_client_ids():
    with client_lock:
        return jsonify({"client_ids": list(active_clients), "count": len(active_clients)})


@app.route('/status', methods=['GET'])
def status():
    queue_stats = job_queue_manager.get_stats()
    
    with client_lock:
        client_count = len(active_clients)
    
    return jsonify({
        "status": "online",
        "discord_bot_connected": discord_bot.connected if discord_bot else False,
        "queue_size": queue_stats["size"],
        "processed_servers": processed_servers_manager.size(),
        "active_clients": client_count,
        **queue_stats
    })


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status": "online",
        "service": "Discord Bot",
        "discord_bot_connected": discord_bot.connected if discord_bot else False
    })


@socketio.on('connect')
def handle_connect(auth=None):
    client_id = auth.get('client_id', request.sid) if auth and isinstance(auth, dict) else request.sid
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = connect_time
    
    emit('connected', {'status': 'ok', 'client_id': client_id})
    logger.info(f"🔌 Client connected: {client_id[:8]}... (time: {connect_time})")


@socketio.on('get_job')
def handle_get_job(data):
    client_id = data.get('client_id', 'unknown')
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    
    if job:
        emit('job_response', {"has_job": True, **job.to_dict()})
    else:
        emit('job_response', {"has_job": False})


@socketio.on('register')
def handle_register(data):
    client_id = data.get('client_id', 'unknown')
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = connect_time
    
    emit('registered', {"status": "ok", "client_id": client_id})
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {"has_job": True, **job.to_dict()})
    else:
        emit('job', {"has_job": False})


@socketio.on('clear_job')
def handle_clear_job(data):
    client_id = data.get('client_id', 'unknown')
    server_id = data.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared via WS: {server_id[:8]}... by {client_id[:8]}...")
        
        job = job_queue_manager.get_job_for_client(client_id, set())
        if job:
            emit('job', {"has_job": True, **job.to_dict()})
        else:
            emit('job', {"has_job": False})


# ============================================================================
# WebSocket Broadcasting
# ============================================================================

ws_event_loop: Optional[asyncio.AbstractEventLoop] = None
ws_clients: Dict[str, str] = {}
jobs_clients: Dict[str, str] = {}
ws_clients_lock = Lock()
jobs_clients_lock = Lock()

def broadcast_job_to_ws_clients(job: Job):
    """Broadcast new job to WebSocket clients (ONLY clients who loaded BEFORE job was created)"""
    job_data = {"has_job": True, **job.to_dict()}
    
    with client_lock:
        eligible_clients = []
        for client_id, load_time in client_connection_times.items():
            if load_time > 0 and load_time < job.created_time:
                eligible_clients.append(client_id)
        
        if not eligible_clients:
            logger.info(f"⏭️ Skipping job broadcast: No eligible clients (job created: {job.created_time:.2f}, clients: {len(client_connection_times)})")
            return
    
    with ws_clients_lock:
        sent_count = 0
        for client_id in eligible_clients:
            if client_id in ws_clients:
                session_id = ws_clients[client_id]
                try:
                    socketio.emit('job', job_data, namespace='/ws', room=session_id)
                    sent_count += 1
                except Exception as e:
                    logger.warning(f"Failed to send job to client {client_id[:8]}...: {e}")
    
    jobs_sent_count = 0
    with jobs_clients_lock:
        for client_id in eligible_clients:
            if client_id in jobs_clients:
                session_id = jobs_clients[client_id]
                try:
                    socketio.emit('job', job_data, namespace='/jobs', room=session_id)
                    jobs_sent_count += 1
                except Exception as e:
                    logger.warning(f"Failed to send job to /jobs client {client_id[:8]}...: {e}")
    
    logger.info(f"📤 Broadcasted job to {sent_count} eligible WS clients (/ws) + {jobs_sent_count} (/jobs): {job.pet_name} | {job.format_duel()} (created: {job.created_time:.2f})")


def run_websocket_server():
    global ws_event_loop
    ws_event_loop = asyncio.new_event_loop()
    
    async def ws_server():
        logger.info(f"🔌 WebSocket support via Flask-SocketIO on port {Config.PORT}")
    
    def run_loop():
        asyncio.set_event_loop(ws_event_loop)
        ws_event_loop.run_until_complete(ws_server())
    
    thread = Thread(target=run_loop, daemon=True)
    thread.start()


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
    
    run_websocket_server()
    
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    logger.info(f"🌐 Flask API started on port {Config.PORT}")
    logger.info(f"🔌 WebSocket support via Flask-SocketIO on port {Config.PORT}")
    logger.info(f"   Raw WebSocket endpoint: wss://idek-production.up.railway.app/socket.io/?transport=websocket&EIO=4&namespace=/ws")
    logger.info(f"   SocketIO endpoint: wss://idek-production.up.railway.app/socket.io/?transport=websocket&EIO=4&namespace=/jobs")
    logger.info("⏳ Starting Discord Bot...\n")
    
    run_discord_bot()
