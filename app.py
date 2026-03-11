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
    duel_mode: Optional[str] = None  # NEW: "Yes", "No", or None if unknown
    
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
    
    def to_dict(self):
        data = asdict(self)
        data['pet_value_formatted'] = self.format_value()
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
    def parse_duel_mode(cls, fields: list) -> Optional[str]:
        """Parse duel mode from embed fields. Returns 'Yes', 'No', or None."""
        for field in fields:
            field_name = field.get("name", "").lower()
            if "duel" in field_name:
                raw = cls.clean_text(field.get("value", ""))
                # Check inactive BEFORE active to avoid substring match bug
                if "inactive" in raw.lower() or "❌" in raw:
                    return "No"
                elif "✅" in raw or raw.lower() == "active":
                    return "Yes"
                else:
                    return "No"
        return None

    @classmethod
    def parse_embed(cls, embed_dict: dict) -> Dict:
        """Parse embed from Gateway JSON format - returns ALL pets found (priority and non-priority) as separate entries"""
        server_id = None
        pet_thumbnail = None
        
        # Extract thumbnail
        if embed_dict.get("thumbnail") and embed_dict["thumbnail"].get("url"):
            pet_thumbnail = embed_dict["thumbnail"]["url"]
        
        # Extract fields
        fields = embed_dict.get("fields", [])
        
        # Find server ID
        for field in fields:
            field_name = field.get("name", "").lower()
            field_value = cls.clean_text(field.get("value", ""))
            
            if ("server" in field_name and "id" in field_name) or \
               ("job" in field_name and "id" in field_name):
                server_id = field_value
                break
        
        # Check description and title for server ID if not found
        if not server_id:
            for text in [embed_dict.get("description", ""), embed_dict.get("title", "")]:
                match = cls.SERVER_ID_PATTERN.search(text)
                if match:
                    server_id = match.group()
                    break
        
        # NEW: Parse duel mode
        duel_mode = cls.parse_duel_mode(fields)

        # Find ALL pets (both priority and non-priority) - send each as separate job
        all_pets_found: Dict[str, Dict] = {}  # pet_name -> {value, name}
        
        # Check "highest pet" field
        for field in fields:
            field_name = field.get("name", "").lower()
            if "highest" in field_name and "pet" in field_name:
                field_value = cls.clean_text(field.get("value", ""))
                name, value = cls.extract_pet_from_line(field_value)
                if name and value > 0:
                    # Use exact name as key to preserve all pets
                    if name not in all_pets_found:
                        all_pets_found[name] = {"value": value, "name": name}
                    else:
                        # Keep highest value if same pet appears multiple times
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
                    
                    # Use exact name as key - keep ALL pets separately
                    if name not in all_pets_found:
                        all_pets_found[name] = {"value": value, "name": name}
                    else:
                        # Keep highest value if same pet appears multiple times
                        all_pets_found[name]["value"] = max(
                            all_pets_found[name]["value"], value
                        )
                break
        
        # Return ALL pets found (both priority and non-priority) as separate entries
        all_pets_list: List[PetInfo] = []
        
        if all_pets_found:
            # Sort: priority pets first (by priority order), then non-priority pets
            priority_pets = []
            regular_pets = []
            
            for pet_name, pet_data in all_pets_found.items():
                pet_name_lower = pet_name.lower()
                # Check if it's in priority list
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
            
            # Sort priority pets by priority order (lowest index = highest priority)
            priority_pets.sort(key=lambda x: PRIORITY_PETS_INDEX.get(x[0].lower(), len(PRIORITY_PETS)))
            
            # Add priority pets first, then regular pets
            for _, pet_info in priority_pets:
                all_pets_list.append(pet_info)
            
            for pet_info in regular_pets:
                all_pets_list.append(pet_info)
        
        return {
            "server_id": server_id,
            "pet_info_list": all_pets_list,  # ALL pets, not just priority
            "duel_mode": duel_mode  # NEW
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
            # Use unique_tracking_id if provided, otherwise fall back to server_id
            tracking_id = unique_tracking_id or job.server_id
            
            # Check for existing job with same server_id AND pet_name (same pet from same server)
            for idx, existing_job in enumerate(self.queue):
                if existing_job.server_id == job.server_id and existing_job.pet_name == job.pet_name:
                    if job.pet_value > existing_job.pet_value:
                        # Remove old job and add new one
                        del self.queue[idx]
                        self.queue.append(job)
                        logger.info(
                            f"🔄 Updated job {job.server_id[:8]}... "
                            f"{existing_job.pet_name} → {job.pet_name} "
                            f"({job.format_value()})"
                        )
                        return True
                    return False
            
            # Add new job (allows multiple pets from same server with same server_id)
            self.queue.append(job)
            self.sent_servers.add(tracking_id)  # Track by unique_tracking_id if provided
            duel_tag = f" ⚔️ DUEL" if job.duel_mode == "Yes" else ""
            logger.info(
                f"🚀 NEW JOB: {job.server_id[:8]}... "
                f"Pet: {job.pet_name} ({job.format_value()}){duel_tag} "
                f"[Queue: {len(self.queue)}]"
            )
            return True
    
    def get_job_for_client(self, client_id: str, processed_servers: Set[str]) -> Optional[Job]:
        """Get next available job for client (only jobs created AFTER client loaded)"""
        with self.lock:
            # Get client's connection time (default to 0 if not tracked)
            client_connect_time = client_connection_times.get(client_id, 0)
            
            for job in self.queue:
                server_id = job.server_id
                pet_name = job.pet_name
                job_key = (server_id, pet_name)
                
                # Skip if already processed (check by server_id only for backward compatibility)
                if server_id in processed_servers:
                    continue
                
                # Skip if this specific pet+server combo was already sent to this client
                if job_key in self.job_sent_to_clients:
                    if client_id in self.job_sent_to_clients[job_key]:
                        continue
                
                # CRITICAL: Skip ALL jobs created BEFORE client loaded
                # Only send jobs created AFTER client loaded (job.created_time > client_connect_time)
                # If client loaded at time 100 and job created at time 200, send it (job is newer)
                # If client loaded at time 200 and job created at time 100, skip it (job is older)
                if client_connect_time > 0 and job.created_time <= client_connect_time:
                    continue  # Skip old job - job was created before or at the same time client loaded
                
                # Mark as sent to this client (track by server_id + pet_name)
                if job_key not in self.job_sent_to_clients:
                    self.job_sent_to_clients[job_key] = set()
                self.job_sent_to_clients[job_key].add(client_id)
                
                return job
            
            return None
    
    def remove_job(self, server_id: str, pet_name: Optional[str] = None):
        """Remove job from queue. If pet_name provided, remove only that specific pet. Otherwise remove all jobs with that server_id."""
        with self.lock:
            if pet_name:
                # Remove only the specific pet+server combo
                self.queue = deque(
                    [j for j in self.queue if not (j.server_id == server_id and j.pet_name == pet_name)],
                    maxlen=self.queue.maxlen
                )
            else:
                # Remove all jobs with this server_id (backward compatibility)
                self.queue = deque(
                    [j for j in self.queue if j.server_id != server_id],
                    maxlen=self.queue.maxlen
                )
    
    def size(self) -> int:
        """Get queue size"""
        with self.lock:
            return len(self.queue)
    
    def get_stats(self) -> Dict:
        """Get queue statistics"""
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
        """Add processed server"""
        with self.lock:
            if server_id not in self.servers:
                self.servers[server_id] = {
                    "clients": set(),
                    "timestamp": datetime.now()
                }
            self.servers[server_id]["clients"].add(client_id)
            
            # Cleanup if needed
            if len(self.servers) > self.max_size:
                self._cleanup()
    
    def contains(self, server_id: str) -> bool:
        """Check if server is processed"""
        with self.lock:
            # Periodic cleanup
            if time.time() - self.last_cleanup > 300:  # Every 5 minutes
                self._cleanup()
            
            return server_id in self.servers
    
    def _cleanup(self):
        """Remove old entries"""
        cutoff = datetime.now() - timedelta(seconds=self.ttl)
        self.servers = {
            sid: data for sid, data in self.servers.items()
            if data["timestamp"] > cutoff
        }
        self.last_cleanup = time.time()
        logger.info(f"🧹 Cleaned up processed servers. Remaining: {len(self.servers)}")
    
    def size(self) -> int:
        """Get count of processed servers"""
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
        
        # Create bot with minimal intents
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guild_messages = True
        
        self.bot = discord.Client(intents=intents)
        self.connected = False
        
        # Set up event handlers
        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)
    
    async def on_ready(self):
        """Called when bot is ready"""
        self.connected = True
        logger.info(f"✅ Discord bot ready! Logged in as {self.bot.user}")
        logger.info(f"📡 Listening to channel ID: {self.channel_id}")
    
    async def on_message(self, message: discord.Message):
        """Called when a message is received"""
        # Only process messages from the target channel
        if message.channel.id != self.channel_id:
            return
        
        # Skip if already processed
        if message.id in self.processed_message_ids:
            return
        
        # Only process messages with embeds
        if not message.embeds:
            return
        
        self.processed_message_ids.add(message.id)
        
        # Clean up old message IDs periodically
        if len(self.processed_message_ids) > Config.MAX_MESSAGE_IDS:
            # Keep only recent IDs (simple cleanup)
            self.processed_message_ids = set(list(self.processed_message_ids)[-Config.MAX_MESSAGE_IDS:])
        
        logger.info(f"📨 Message with {len(message.embeds)} embed(s) (ID: {message.id})")
        
        # Process embeds
        for idx, embed in enumerate(message.embeds):
            try:
                # Convert discord.Embed to dict format for PetParser
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
                duel_mode = result["duel_mode"]  # NEW
                
                if not server_id or not pet_info_list:
                    continue
                
                # Create a separate job for EACH pet found
                for pet_info in pet_info_list:
                    unique_tracking_id = f"{server_id}_{pet_info.name}"
                    
                    # Skip if this specific pet+server combo was already processed
                    if self.processed_servers.contains(unique_tracking_id):
                        continue
                    
                    # Create and add job for this pet
                    job = Job(
                        server_id=server_id,
                        pet_name=pet_info.name,
                        pet_value=pet_info.value,
                        pet_thumbnail=pet_info.thumbnail,
                        retries=Config.DEFAULT_RETRIES,
                        retry_delay=Config.DEFAULT_RETRY_DELAY,
                        created_time=time.time(),
                        is_priority=pet_info.is_priority,
                        duel_mode=duel_mode  # NEW
                    )
                    
                    if self.job_queue.add_job(job, unique_tracking_id=unique_tracking_id):
                        # Broadcast to WebSocket clients
                        broadcast_job_to_ws_clients(job)
                
            except Exception as e:
                logger.error(f"❌ Error processing embed {idx + 1}: {e}")
    
    async def start(self):
        """Start the bot"""
        try:
            await self.bot.start(self.token)
        except Exception as e:
            logger.error(f"❌ Discord bot error: {e}")
            self.connected = False
    
    async def close(self):
        """Close the bot"""
        await self.bot.close()
        self.connected = False


# ============================================================================
# Flask API & SocketIO
# ============================================================================

# Initialize managers
job_queue_manager = JobQueueManager()
processed_servers_manager = ProcessedServersManager()

# Flask app
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

# Client tracking
active_clients: Set[str] = set()
client_lock = Lock()
# Track when each client connected/loaded (to filter out old jobs)
client_connection_times: Dict[str, float] = {}

# Raw WebSocket endpoint for Roblox executors (using SocketIO's raw WebSocket support)
@socketio.on('connect', namespace='/ws')
def handle_raw_ws_connect(auth=None):
    """Handle raw WebSocket connection for Roblox executors"""
    client_id = request.args.get('client_id') or request.sid
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        # Only set connection time if not already set (preserve /client_loaded time)
        # This ensures we use the actual client load time, not WebSocket connect time
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with ws_clients_lock:
        ws_clients[client_id] = request.sid  # Store SocketIO session ID
    
    emit('connected', {'status': 'ok', 'client_id': client_id}, namespace='/ws')
    logger.info(f"🔌 Raw WebSocket client connected: {client_id[:8]}... (load time: {client_connection_times.get(client_id, connect_time):.2f})")
    
    # Send any available job immediately (only jobs created after client loaded)
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {
            "has_job": True,
            **job.to_dict()
        }, namespace='/ws')
    else:
        emit('job', {"has_job": False}, namespace='/ws')


@socketio.on('disconnect', namespace='/ws')
def handle_raw_ws_disconnect():
    """Handle raw WebSocket disconnection"""
    client_id = request.args.get('client_id') or request.sid
    
    with ws_clients_lock:
        ws_clients.pop(client_id, None)
    
    with client_lock:
        active_clients.discard(client_id)
        client_connection_times.pop(client_id, None)  # Clean up connection time
    
    logger.info(f"🔌 Raw WebSocket client disconnected: {client_id[:8]}...")


@socketio.on('register', namespace='/ws')
def handle_raw_ws_register(data):
    """Handle registration for raw WebSocket clients"""
    client_id = data.get('client_id', request.sid)
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        # Only set connection time if not already set (preserve /client_loaded time)
        # This ensures we use the actual client load time, not WebSocket connect time
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with ws_clients_lock:
        ws_clients[client_id] = request.sid
    
    emit('registered', {
        "status": "ok",
        "client_id": client_id
    }, namespace='/ws')
    
    logger.info(f"🔌 Raw WebSocket client registered: {client_id[:8]}... (load time: {client_connection_times.get(client_id, connect_time):.2f})")
    
    # Send any available job immediately (only jobs created after client loaded)
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {
            "has_job": True,
            **job.to_dict()
        }, namespace='/ws')
    else:
        emit('job', {"has_job": False}, namespace='/ws')


@socketio.on('clear_job', namespace='/ws')
def handle_raw_ws_clear_job(data):
    """Handle job clearing for raw WebSocket clients"""
    client_id = data.get('client_id', request.sid)
    server_id = data.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared via raw WS: {server_id[:8]}... by {client_id[:8]}...")
        
        # Send next job if available
        job = job_queue_manager.get_job_for_client(client_id, set())
        if job:
            emit('job', {
                "has_job": True,
                **job.to_dict()
            }, namespace='/ws')
        else:
            emit('job', {"has_job": False}, namespace='/ws')


@socketio.on('ping', namespace='/ws')
def handle_raw_ws_ping():
    """Handle ping from raw WebSocket clients"""
    emit('pong', {}, namespace='/ws')


# SocketIO namespace for job distribution (SocketIO protocol)
@socketio.on('connect', namespace='/jobs')
def handle_jobs_connect(auth):
    """Handle connection to /jobs namespace"""
    client_id = auth.get('client_id', request.sid) if auth else request.sid
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        # Only set connection time if not already set (preserve /client_loaded time)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with jobs_clients_lock:
        jobs_clients[client_id] = request.sid  # Store session ID for /jobs namespace
    
    emit('connected', {'status': 'ok', 'client_id': client_id}, namespace='/jobs')
    logger.info(f"🔌 Client connected to /jobs namespace: {client_id[:8]}... (load time: {client_connection_times.get(client_id, connect_time):.2f})")
    
    # Send any available job immediately (only jobs created after client loaded)
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {
            "has_job": True,
            **job.to_dict()
        }, namespace='/jobs')
    else:
        emit('job', {"has_job": False}, namespace='/jobs')


@socketio.on('register', namespace='/jobs')
def handle_jobs_register(data):
    """Handle registration in /jobs namespace"""
    client_id = data.get('client_id', request.sid)
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        # Only set connection time if not already set (preserve /client_loaded time)
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    with jobs_clients_lock:
        jobs_clients[client_id] = request.sid  # Store session ID for /jobs namespace
    
    emit('registered', {
        "status": "ok",
        "client_id": client_id
    }, namespace='/jobs')
    
    # Send any available job immediately (only jobs created after client loaded)
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {
            "has_job": True,
            **job.to_dict()
        }, namespace='/jobs')
    else:
        emit('job', {"has_job": False}, namespace='/jobs')


@socketio.on('clear_job', namespace='/jobs')
def handle_jobs_clear_job(data):
    """Handle job clearing in /jobs namespace"""
    client_id = data.get('client_id', request.sid)
    server_id = data.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared via WS: {server_id[:8]}... by {client_id[:8]}...")
        
        # Send next job if available
        job = job_queue_manager.get_job_for_client(client_id, set())
        if job:
            emit('job', {
                "has_job": True,
                **job.to_dict()
            }, namespace='/jobs')
        else:
            emit('job', {"has_job": False}, namespace='/jobs')


@app.route('/ws', methods=['GET'])
def ws_upgrade():
    """WebSocket upgrade endpoint - Railway will handle the upgrade"""
    # This route exists to help Railway identify WebSocket endpoints
    # Actual WebSocket handling is done via Flask-SocketIO
    return jsonify({
        "status": "websocket_endpoint",
        "message": "Use SocketIO endpoint: /socket.io/?transport=websocket&EIO=4&namespace=/ws"
    }), 426  # 426 Upgrade Required


# Peer registry: job_id -> set of client_ids in that server
peer_registry: Dict[str, Set[str]] = {}
peer_registry_lock = Lock()


@app.route('/register_peer', methods=['GET', 'POST'])
def register_peer():
    """Register a client as being in a specific Roblox server"""
    client_id = request.args.get('client_id', 'unknown')
    job_id = request.args.get('job_id', '')

    if not job_id:
        return jsonify({"status": "error", "message": "job_id required"}), 400

    with peer_registry_lock:
        if job_id not in peer_registry:
            peer_registry[job_id] = set()
        peer_registry[job_id].add(client_id)

    return jsonify({"status": "ok", "job_id": job_id, "client_id": client_id})


@app.route('/get_peers', methods=['GET'])
def get_peers():
    """Get all client IDs in the same Roblox server (job_id), excluding the requester"""
    client_id = request.args.get('client_id', 'unknown')
    job_id = request.args.get('job_id', '')

    if not job_id:
        return jsonify({"status": "error", "message": "job_id required"}), 400

    with peer_registry_lock:
        all_in_server = peer_registry.get(job_id, set())
        peers = [cid for cid in all_in_server if cid != client_id]

    return jsonify({"peers": peers, "job_id": job_id})



def get_job():
    """Get next job for client (only jobs created after client loaded)"""
    client_id = request.args.get('client_id', 'unknown')
    since = float(request.args.get('since', 0))
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        if since > 0:
            # Always trust the client's reported load time - survives server restarts
            client_connection_times[client_id] = since
        elif client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    job = job_queue_manager.get_job_for_client(
        client_id,
        set()  # processed_servers handled internally
    )
    
    if job:
        logger.info(f"📤 Sending job to {client_id[:8]}...: {job.pet_name}")
        return jsonify({
            "has_job": True,
            **job.to_dict()
        })
    
    return jsonify({"has_job": False})


@app.route('/clear_job', methods=['GET', 'POST'])
def clear_job():
    """Mark job as completed"""
    client_id = request.args.get('client_id', 'unknown')
    server_id = request.args.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared: {server_id[:8]}... by {client_id[:8]}...")
    
    return jsonify({
        "status": "cleared",
        "queue_size": job_queue_manager.size()
    })


@app.route('/client_loaded', methods=['POST'])
def client_loaded():
    """Mark client as loaded - prevents receiving old/stale jobs"""
    client_id = request.args.get('client_id', 'unknown')
    load_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        # ALWAYS update load time (even if already set) - this is when client actually loaded
        # This ensures we skip ALL jobs created before this time
        client_connection_times[client_id] = load_time
    
    logger.info(f"✅ Client loaded: {client_id[:8]}... (load time: {load_time:.2f}) - Will skip ALL jobs created before this time")
    return jsonify({"status": "ok", "load_time": load_time})


@app.route('/get_client_ids', methods=['GET'])
def get_client_ids():
    """Get all active client IDs"""
    with client_lock:
        return jsonify({
            "client_ids": list(active_clients),
            "count": len(active_clients)
        })


@app.route('/status', methods=['GET'])
def status():
    """Get system status"""
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
    """Health check"""
    return jsonify({
        "status": "online",
        "service": "Discord Bot",
        "discord_bot_connected": discord_bot.connected if discord_bot else False
    })


@socketio.on('connect')
def handle_connect(auth=None):
    """Handle SocketIO connection"""
    client_id = auth.get('client_id', request.sid) if auth and isinstance(auth, dict) else request.sid
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = connect_time
    
    emit('connected', {'status': 'ok', 'client_id': client_id})
    logger.info(f"🔌 Client connected: {client_id[:8]}... (time: {connect_time})")


@socketio.on('get_job')
def handle_get_job(data):
    """Handle job request via SocketIO (only jobs created after client connected)"""
    client_id = data.get('client_id', 'unknown')
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        # Set connection time if not already set
        if client_id not in client_connection_times:
            client_connection_times[client_id] = connect_time
    
    job = job_queue_manager.get_job_for_client(client_id, set())
    
    if job:
        emit('job_response', {
            "has_job": True,
            **job.to_dict()
        })
    else:
        emit('job_response', {"has_job": False})


@socketio.on('register')
def handle_register(data):
    """Handle WebSocket client registration"""
    client_id = data.get('client_id', 'unknown')
    connect_time = time.time()
    
    with client_lock:
        active_clients.add(client_id)
        client_connection_times[client_id] = connect_time
    
    emit('registered', {
        "status": "ok",
        "client_id": client_id
    })
    
    # Send any available job immediately (only jobs created after connection)
    job = job_queue_manager.get_job_for_client(client_id, set())
    if job:
        emit('job', {
            "has_job": True,
            **job.to_dict()
        })
    else:
        emit('job', {"has_job": False})


@socketio.on('clear_job')
def handle_clear_job(data):
    """Handle job clearing via SocketIO"""
    client_id = data.get('client_id', 'unknown')
    server_id = data.get('server_id')
    
    if server_id:
        job_queue_manager.remove_job(server_id)
        processed_servers_manager.add(server_id, client_id)
        logger.info(f"✅ Job cleared via WS: {server_id[:8]}... by {client_id[:8]}...")
        
        # Send next job if available
        job = job_queue_manager.get_job_for_client(client_id, set())
        if job:
            emit('job', {
                "has_job": True,
                **job.to_dict()
            })
        else:
            emit('job', {"has_job": False})


# ============================================================================
# WebSocket Server for Job Distribution
# ============================================================================

# Removed handle_websocket_client - using Flask-SocketIO instead


# WebSocket event loop for broadcasting
ws_event_loop: Optional[asyncio.AbstractEventLoop] = None

# WebSocket clients for raw WebSocket connections (using SocketIO session IDs)
ws_clients: Dict[str, str] = {}  # client_id -> session_id (for /ws namespace)
jobs_clients: Dict[str, str] = {}  # client_id -> session_id (for /jobs namespace)
ws_clients_lock = Lock()
jobs_clients_lock = Lock()

def broadcast_job_to_ws_clients(job: Job):
    """Broadcast new job to WebSocket clients (ONLY clients who loaded BEFORE job was created)"""
    job_data = {
        "has_job": True,
        **job.to_dict()
    }
    
    # Filter: Only send to clients who loaded BEFORE this job was created
    with client_lock:
        eligible_clients = []
        for client_id, load_time in client_connection_times.items():
            if load_time > 0 and load_time < job.created_time:
                eligible_clients.append(client_id)
        
        if not eligible_clients:
            logger.info(f"⏭️ Skipping job broadcast: No eligible clients (job created: {job.created_time:.2f}, clients: {len(client_connection_times)})")
            return
    
    # Send to eligible clients only (not broadcast=True, send individually)
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
    
    # Send to eligible clients in /jobs namespace
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
    
    logger.info(f"📤 Broadcasted job to {sent_count} eligible WS clients (/ws) + {jobs_sent_count} (/jobs): {job.pet_name} (created: {job.created_time:.2f})")


def run_websocket_server():
    """Run raw WebSocket server integrated with Flask (same port)"""
    global ws_event_loop
    
    # Create event loop for WebSocket server
    ws_event_loop = asyncio.new_event_loop()
    
    async def ws_server():
        logger.info(f"🔌 WebSocket support via Flask-SocketIO on port {Config.PORT}")
    
    # Run in background thread
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
    """Run Flask server with SocketIO"""
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
    """Run Discord bot in asyncio loop"""
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
    
    # Initialize WebSocket event loop (for future use if needed)
    run_websocket_server()
    
    # Start Flask (includes SocketIO WebSocket support)
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    logger.info(f"🌐 Flask API started on port {Config.PORT}")
    logger.info(f"🔌 WebSocket support via Flask-SocketIO on port {Config.PORT}")
    logger.info(f"   Raw WebSocket endpoint: wss://idek-production.up.railway.app/socket.io/?transport=websocket&EIO=4&namespace=/ws")
    logger.info(f"   SocketIO endpoint: wss://idek-production.up.railway.app/socket.io/?transport=websocket&EIO=4&namespace=/jobs")
    logger.info("⏳ Starting Discord Bot...\n")
    
    # Run Discord bot (blocking)
    run_discord_bot()
