import random
import logging
import subprocess
import sys
import os
import re
import asyncio
import sqlite3
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

# ── docker SDK is optional – only used for host resource validation ──────────
try:
    import docker as docker_sdk
    _docker_client = docker_sdk.from_env()
except Exception:
    _docker_client = None

# ── Load environment variables ───────────────────────────────────────────────
load_dotenv()

TOKEN               = os.getenv("TOKEN", "")
ADMIN_ID            = int(os.getenv("ADMIN_ID", 0))
BOT_STATUS_NAME     = os.getenv("BOT_STATUS_NAME", "UnixNodes")
WATERMARK           = os.getenv("WATERMARK", "Powered by UnixNodes VPS Bot")
DEFAULT_RAM         = os.getenv("DEFAULT_RAM", "2g")
DEFAULT_CPU         = os.getenv("DEFAULT_CPU", "1")
DEFAULT_DISK        = os.getenv("DEFAULT_DISK", "10G")
VPS_HOSTNAME        = os.getenv("VPS_HOSTNAME", "unix-free")
SERVER_LIMIT        = int(os.getenv("SERVER_LIMIT", 1))
TOTAL_SERVER_LIMIT  = int(os.getenv("TOTAL_SERVER_LIMIT", 50))
DATABASE_FILE       = os.getenv("DATABASE_FILE", "vps_bot.db")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("vps_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── Bot setup ────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)


# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def is_admin(member: discord.Member | discord.User) -> bool:
    return member.id == ADMIN_ID


def parse_gb(resource_str: str) -> float:
    """Convert a Docker-style resource string (e.g. '2g', '512m') to GB."""
    match = re.match(r"(\d+(?:\.\d+)?)([mMgG])?", resource_str.strip())
    if not match:
        return 0.0
    num  = float(match.group(1))
    unit = (match.group(2) or "g").lower()
    return num if unit == "g" else num / 1024.0


def get_uptime(container_id: str) -> str:
    try:
        raw = subprocess.check_output(
            ["docker", "inspect", "-f", "{{.State.StartedAt}}", container_id],
            stderr=subprocess.STDOUT,
        ).decode().strip()
        if not raw or raw == "<no value>":
            return "Not running"
        start = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - start
        d     = delta.days
        h, r  = divmod(delta.seconds, 3600)
        m, _  = divmod(r, 60)
        return f"{d}d {h}h {m}m"
    except Exception as exc:
        logger.error("Uptime error for %s: %s", container_id, exc)
        return "Unknown"


def get_stats(container_id: str) -> dict:
    try:
        raw = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format",
             "{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}", container_id],
            stderr=subprocess.STDOUT,
        ).decode().strip()
        parts = raw.split("\t")
        if len(parts) == 3:
            return {"cpu": parts[0], "mem": parts[1], "net": parts[2]}
    except Exception as exc:
        logger.error("Stats error for %s: %s", container_id, exc)
    return {"cpu": "N/A", "mem": "N/A", "net": "N/A"}


def get_logs(container_id: str, lines: int = 50) -> str:
    try:
        raw = subprocess.check_output(
            ["docker", "logs", "--tail", str(lines), container_id],
            stderr=subprocess.STDOUT,
        ).decode()
        return raw[-2000:] or "(empty)"
    except Exception as exc:
        logger.error("Logs error for %s: %s", container_id, exc)
        return "Failed to fetch logs."


# ════════════════════════════════════════════════════════════════════════════
# Database
# ════════════════════════════════════════════════════════════════════════════

def init_db() -> None:
    conn = sqlite3.connect(DATABASE_FILE)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS vps (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER NOT NULL,
            container_id   TEXT UNIQUE NOT NULL,
            container_name TEXT NOT NULL,
            os_type        TEXT NOT NULL,
            hostname       TEXT NOT NULL,
            status         TEXT DEFAULT 'stopped',
            ssh_command    TEXT,
            ram            TEXT DEFAULT '{DEFAULT_RAM}',
            cpu            TEXT DEFAULT '{DEFAULT_CPU}',
            disk           TEXT DEFAULT '{DEFAULT_DISK}',
            suspended      INTEGER DEFAULT 0,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    """)
    # Migration: add suspended column if missing (existing DBs)
    cur.execute("PRAGMA table_info(vps)")
    cols = [c[1] for c in cur.fetchall()]
    if "suspended" not in cols:
        cur.execute("ALTER TABLE vps ADD COLUMN suspended INTEGER DEFAULT 0")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY
        )
    """)
    conn.commit()
    conn.close()


init_db()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn


# ── User helpers ─────────────────────────────────────────────────────────────

def add_user(user_id: int, username: str) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)",
            (user_id, username),
        )


def add_ban(user_id: int) -> None:
    with get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO bans (user_id) VALUES (?)", (user_id,))


def remove_ban(user_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))


def is_banned(user_id: int) -> bool:
    with get_db() as conn:
        return conn.execute(
            "SELECT 1 FROM bans WHERE user_id = ?", (user_id,)
        ).fetchone() is not None


# ── VPS helpers ───────────────────────────────────────────────────────────────

def add_vps(
    user_id: int,
    container_id: str,
    container_name: str,
    os_type: str,
    hostname: str,
    ssh_command: str,
    ram: str = DEFAULT_RAM,
    cpu: str = DEFAULT_CPU,
    disk: str = DEFAULT_DISK,
) -> None:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO vps
                (user_id, container_id, container_name, os_type, hostname,
                 status, ssh_command, ram, cpu, disk, suspended)
            VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, 0)
            """,
            (user_id, container_id, container_name, os_type, hostname,
             ssh_command, ram, cpu, disk),
        )


def get_user_vps(user_id: int) -> list:
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM vps WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()


def count_user_vps(user_id: int) -> int:
    return len(get_user_vps(user_id))


def get_vps_by_identifier(user_id: int, identifier: str | None):
    vps_list = get_user_vps(user_id)
    if not identifier:
        return vps_list[0] if vps_list else None
    lo = identifier.lower()
    for v in vps_list:
        if lo in v["container_id"].lower() or lo in v["container_name"].lower():
            return v
    return None


def update_vps_status(container_id: str, status: str) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE vps SET status = ? WHERE container_id = ?",
            (status, container_id),
        )


def update_vps_ssh(container_id: str, ssh_command: str) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE vps SET ssh_command = ? WHERE container_id = ?",
            (ssh_command, container_id),
        )


def update_vps_suspended(container_id: str, suspended: int) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE vps SET suspended = ? WHERE container_id = ?",
            (suspended, container_id),
        )


def delete_vps(container_id: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM vps WHERE container_id = ?", (container_id,))


def get_total_running() -> int:
    with get_db() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM vps WHERE status = 'running'"
        ).fetchone()[0]


# ════════════════════════════════════════════════════════════════════════════
# Async Docker wrappers
# ════════════════════════════════════════════════════════════════════════════

async def _run_docker(*args, timeout: float = 60.0) -> tuple[int, str, str]:
    """Run a docker command; returns (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return -1, "", "timeout"
    return proc.returncode, stdout.decode().strip(), stderr.decode().strip()


async def async_docker_run(
    image: str,
    hostname: str,
    ram: str,
    cpu: str,
    _disk: str,          # disk arg kept for API compat – not enforced at runtime
    container_name: str,
) -> str | None:
    rc, out, err = await _run_docker(
        "docker", "run", "-d",
        "--privileged", "--cap-add=ALL",
        "--restart", "unless-stopped",
        f"--memory={ram}",
        f"--cpus={cpu}",
        f"--hostname={hostname}",
        f"--name={container_name}",
        image,
        "tail", "-f", "/dev/null",
        timeout=60.0,
    )
    if rc != 0:
        logger.error("docker run failed: %s", err)
        return None
    return out or None


async def async_docker_start(container_id: str) -> bool:
    rc, _, err = await _run_docker("docker", "start", container_id, timeout=30.0)
    if rc != 0:
        logger.error("docker start failed for %s: %s", container_id, err)
    return rc == 0


async def async_docker_stop(container_id: str) -> bool:
    rc, _, err = await _run_docker("docker", "stop", container_id, timeout=30.0)
    if rc != 0:
        logger.warning("docker stop failed for %s: %s", container_id, err)
        # fallback: kill
        await _run_docker("docker", "kill", container_id, timeout=10.0)
    return rc == 0


async def async_docker_restart(container_id: str) -> bool:
    rc, _, err = await _run_docker("docker", "restart", container_id, timeout=30.0)
    if rc != 0:
        logger.error("docker restart failed for %s: %s", container_id, err)
    return rc == 0


async def async_docker_rm(container_id: str) -> bool:
    rc, _, err = await _run_docker("docker", "rm", "-f", container_id, timeout=30.0)
    if rc != 0:
        logger.error("docker rm failed for %s: %s", container_id, err)
    return rc == 0


async def async_install_tmate(container_id: str) -> None:
    cmd = (
        "apt-get update -qq && "
        "apt-get install -y -qq tmate curl wget sudo openssh-client"
    )
    rc, _, err = await _run_docker(
        "docker", "exec", container_id, "bash", "-c", cmd, timeout=120.0
    )
    if rc != 0:
        logger.warning("tmate install warning for %s: %s", container_id, err)
    else:
        logger.info("tmate installed in %s", container_id)


async def _capture_ssh_line(process: asyncio.subprocess.Process) -> str | None:
    """Read stdout of a running tmate -F process until we see the ssh session line."""
    while True:
        try:
            raw = await asyncio.wait_for(process.stdout.readline(), timeout=30.0)
        except asyncio.TimeoutError:
            break
        if not raw:
            break
        line = raw.decode("utf-8", errors="replace").strip()
        if "ssh session:" in line.lower():
            return line.split("ssh session:", 1)[-1].strip()
    return None


async def docker_exec_tmate(container_id: str) -> asyncio.subprocess.Process | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "exec", container_id, "tmate", "-F",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        return proc
    except Exception as exc:
        logger.error("tmate exec failed for %s: %s", container_id, exc)
        return None


# ════════════════════════════════════════════════════════════════════════════
# Shared embed builder
# ════════════════════════════════════════════════════════════════════════════

def _footer_url() -> str | None:
    return bot.user.avatar.url if (bot.user and bot.user.avatar) else None


def embed_ok(title: str = "", description: str = "") -> discord.Embed:
    e = discord.Embed(
        title=title, description=description,
        color=discord.Color.green(), timestamp=datetime.now(timezone.utc),
    )
    e.set_footer(text=WATERMARK, icon_url=_footer_url())
    return e


def embed_err(description: str) -> discord.Embed:
    return discord.Embed(description=description, color=discord.Color.red())


def embed_info(title: str = "", description: str = "") -> discord.Embed:
    e = discord.Embed(
        title=title, description=description,
        color=discord.Color.blue(), timestamp=datetime.now(timezone.utc),
    )
    e.set_author(
        name=bot.user.name if bot.user else "VPS Bot",
        icon_url=_footer_url(),
    )
    e.set_footer(text=WATERMARK, icon_url=_footer_url())
    return e


# ════════════════════════════════════════════════════════════════════════════
# Core VPS operations
# ════════════════════════════════════════════════════════════════════════════

async def regen_ssh_command(
    interaction: discord.Interaction,
    vps_identifier: str | None,
    *,
    send_response: bool = True,
    target_user: discord.User | discord.Member | None = None,
) -> bool:
    if target_user is None:
        target_user = interaction.user

    vps = get_vps_by_identifier(target_user.id, vps_identifier)
    if not vps:
        if send_response:
            await interaction.response.send_message(
                embed=embed_err("No active VPS found."), ephemeral=True
            )
        return False

    if vps["status"] != "running":
        if send_response:
            await interaction.response.send_message(
                embed=embed_err("VPS must be running to generate an SSH session."),
                ephemeral=True,
            )
        return False

    if send_response:
        await interaction.response.defer(ephemeral=True)

    container_id  = vps["container_id"]
    exec_proc     = await docker_exec_tmate(container_id)
    if not exec_proc:
        msg = embed_err("Failed to execute tmate.")
        if send_response:
            await interaction.followup.send(embed=msg, ephemeral=True)
        return False

    ssh_line = await _capture_ssh_line(exec_proc)
    try:
        exec_proc.kill()
    except Exception:
        pass

    if not ssh_line:
        msg = embed_err("Failed to capture SSH session line.")
        if send_response:
            await interaction.followup.send(embed=msg, ephemeral=True)
        return False

    update_vps_ssh(container_id, ssh_line)
    dm_embed = embed_ok("New SSH Session Generated", f"```{ssh_line}```")

    try:
        await target_user.send(embed=dm_embed)
    except discord.Forbidden:
        logger.warning("Cannot DM user %s", target_user.id)
        if send_response:
            await interaction.followup.send(
                embed=embed_err(
                    "SSH session regenerated but your DMs are closed. "
                    "Please allow DMs from server members and try again."
                ),
                ephemeral=True,
            )
        return True  # still succeeded

    if send_response:
        await interaction.followup.send(
            embed=embed_ok(description="New SSH session sent to your DMs ✅"),
            ephemeral=True,
        )
    return True


async def manage_vps(
    interaction: discord.Interaction,
    vps_identifier: str | None,
    action: str,
    *,
    target_user: discord.User | discord.Member | None = None,
) -> None:
    if target_user is None:
        target_user = interaction.user

    await interaction.response.defer(ephemeral=True)

    vps = get_vps_by_identifier(target_user.id, vps_identifier)
    if not vps:
        await interaction.followup.send(embed=embed_err("No VPS found."), ephemeral=True)
        return

    # Block suspended start unless admin
    if action == "start" and vps["suspended"] and target_user.id == interaction.user.id:
        await interaction.followup.send(
            embed=embed_err("This VPS is suspended. Contact an admin."),
            ephemeral=True,
        )
        return

    container_id = vps["container_id"]
    success      = False

    if action == "start":
        success = await async_docker_start(container_id)
        if success:
            update_vps_status(container_id, "running")
    elif action == "stop":
        success = await async_docker_stop(container_id)
        if success:
            update_vps_status(container_id, "stopped")
    elif action == "restart":
        success = await async_docker_restart(container_id)
        if success:
            update_vps_status(container_id, "running")

    if not success:
        await interaction.followup.send(
            embed=embed_err(f"Failed to {action} the VPS."), ephemeral=True
        )
        return

    os_name = "Ubuntu 22.04" if vps["os_type"] == "ubuntu" else "Debian 12"
    e = embed_ok(f"VPS {action.title()}ed", f"OS: {os_name}")

    if action in ("start", "restart"):
        regen_ok = await regen_ssh_command(
            interaction, vps_identifier, send_response=False, target_user=target_user
        )
        e.description += (
            "\nNew SSH session sent to your DMs ✅"
            if regen_ok
            else "\n⚠️ Could not regenerate SSH session."
        )

    await interaction.followup.send(embed=e, ephemeral=True)


async def reinstall_vps(
    interaction: discord.Interaction,
    vps_identifier: str,
    os_type: str,
    *,
    target_user: discord.User | discord.Member | None = None,
) -> None:
    if target_user is None:
        target_user = interaction.user

    await interaction.response.defer(ephemeral=True)

    vps = get_vps_by_identifier(target_user.id, vps_identifier)
    if not vps:
        await interaction.followup.send(embed=embed_err("No VPS found."), ephemeral=True)
        return

    container_id = vps["container_id"]
    user_id  = vps["user_id"]
    hostname = vps["hostname"]
    ram, cpu, disk = vps["ram"], vps["cpu"], vps["disk"]

    await async_docker_stop(container_id)
    await asyncio.sleep(2)
    await async_docker_rm(container_id)
    delete_vps(container_id)

    suffix         = random.randint(1000, 9999)
    new_name       = f"{os_type}-vps-{user_id}-{suffix}"
    image          = "ubuntu:22.04" if os_type == "ubuntu" else "debian:bookworm"
    new_id         = await async_docker_run(image, hostname, ram, cpu, disk, new_name)

    if not new_id:
        await interaction.followup.send(
            embed=embed_err("Reinstall failed: Docker creation error."), ephemeral=True
        )
        return

    await async_install_tmate(new_id)
    await asyncio.sleep(10)

    exec_proc = await docker_exec_tmate(new_id)
    ssh_line  = await _capture_ssh_line(exec_proc) if exec_proc else None
    if exec_proc:
        try:
            exec_proc.kill()
        except Exception:
            pass

    if not ssh_line:
        await interaction.followup.send(
            embed=embed_err("Reinstall failed: Unable to generate SSH session."),
            ephemeral=True,
        )
        await async_docker_rm(new_id)
        return

    add_vps(user_id, new_id, new_name, os_type, hostname, ssh_line, ram, cpu, disk)
    os_name = "Ubuntu 22.04" if os_type == "ubuntu" else "Debian 12"
    dm_embed = embed_ok("VPS Reinstalled", f"OS: {os_name}\n```{ssh_line}```")

    try:
        await target_user.send(embed=dm_embed)
    except discord.Forbidden:
        logger.warning("Cannot DM user %s after reinstall", target_user.id)

    await interaction.followup.send(
        embed=embed_ok(description="VPS reinstalled! Check your DMs for SSH details."),
        ephemeral=True,
    )


async def create_vps(
    interaction: discord.Interaction,
    os_type: str,
    ram: str  = DEFAULT_RAM,
    cpu: str  = DEFAULT_CPU,
    disk: str = DEFAULT_DISK,
    *,
    target_user: discord.User | discord.Member | None = None,
) -> None:
    if target_user is None:
        target_user = interaction.user

    user_id  = target_user.id
    username = str(target_user)
    add_user(user_id, username)

    if is_banned(user_id):
        await interaction.response.send_message(
            embed=embed_err("You are banned from creating VPS instances."),
            ephemeral=True,
        )
        return

    if count_user_vps(user_id) >= SERVER_LIMIT:
        await interaction.response.send_message(
            embed=embed_err(f"You've reached the {SERVER_LIMIT}-VPS limit."),
            ephemeral=True,
        )
        return

    if get_total_running() >= TOTAL_SERVER_LIMIT:
        await interaction.response.send_message(
            embed=embed_err(
                f"Global server limit reached ({TOTAL_SERVER_LIMIT} running). "
                "Try again later."
            ),
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    await interaction.followup.send("⏳ Creating your VPS instance…", ephemeral=True)

    hostname       = f"{VPS_HOSTNAME}-{user_id}"
    suffix         = random.randint(1000, 9999)
    container_name = f"{os_type}-vps-{user_id}-{suffix}"
    image          = "ubuntu:22.04" if os_type == "ubuntu" else "debian:bookworm"

    container_id = await async_docker_run(image, hostname, ram, cpu, disk, container_name)
    if not container_id:
        await interaction.followup.send(
            embed=embed_err("Failed to create Docker container."), ephemeral=True
        )
        return

    await asyncio.sleep(5)
    await async_install_tmate(container_id)
    await asyncio.sleep(10)

    exec_proc = await docker_exec_tmate(container_id)
    ssh_line  = await _capture_ssh_line(exec_proc) if exec_proc else None
    if exec_proc:
        try:
            exec_proc.kill()
        except Exception:
            pass

    if not ssh_line:
        await interaction.followup.send(
            embed=embed_err("Creation failed: unable to generate SSH session."),
            ephemeral=True,
        )
        await async_docker_stop(container_id)
        await asyncio.sleep(2)
        await async_docker_rm(container_id)
        return

    add_vps(user_id, container_id, container_name, os_type, hostname, ssh_line, ram, cpu, disk)

    os_name  = "Ubuntu 22.04" if os_type == "ubuntu" else "Debian 12"
    dm_embed = embed_ok(
        "VPS Instance Created",
        f"OS: {os_name}\nRAM: {ram} | CPU: {cpu} | Disk: {disk}\n```{ssh_line}```",
    )
    try:
        await target_user.send(embed=dm_embed)
    except discord.Forbidden:
        logger.warning("Cannot DM user %s after creation", target_user.id)

    await interaction.followup.send(
        embed=embed_ok(description="Your VPS is ready! Check your DMs for SSH details ✅"),
        ephemeral=True,
    )


# ════════════════════════════════════════════════════════════════════════════
# Admin helpers
# ════════════════════════════════════════════════════════════════════════════

async def admin_manage_vps(
    interaction: discord.Interaction,
    target_user_id: int,
    vps_identifier: str,
    action: str,
) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(
            embed=embed_err("Admin only."), ephemeral=True
        )
        return

    try:
        target_user = await bot.fetch_user(target_user_id)
    except Exception:
        await interaction.response.send_message(
            embed=embed_err("User not found."), ephemeral=True
        )
        return

    vps = get_vps_by_identifier(target_user_id, vps_identifier)
    if not vps:
        await interaction.response.send_message(
            embed=embed_err("VPS not found for this user."), ephemeral=True
        )
        return

    container_id = vps["container_id"]
    success      = False
    msg          = ""

    if action == "delete":
        await async_docker_stop(container_id)
        await asyncio.sleep(2)
        await async_docker_rm(container_id)
        delete_vps(container_id)
        success = True
        msg     = f"Deleted VPS for {target_user}"

    elif action == "start":
        success = await async_docker_start(container_id)
        if success:
            update_vps_status(container_id, "running")
        msg = f"Started VPS for {target_user}"

    elif action == "stop":
        success = await async_docker_stop(container_id)
        if success:
            update_vps_status(container_id, "stopped")
        msg = f"Stopped VPS for {target_user}"

    elif action == "restart":
        success = await async_docker_restart(container_id)
        if success:
            update_vps_status(container_id, "running")
        msg = f"Restarted VPS for {target_user}"

    elif action == "suspend":
        success = await async_docker_stop(container_id)
        if success:
            update_vps_status(container_id, "stopped")
            update_vps_suspended(container_id, 1)
        msg = f"Suspended VPS for {target_user}"

    elif action == "unsuspend":
        update_vps_suspended(container_id, 0)
        success = True
        msg     = f"Unsuspended VPS for {target_user} – they can now start it."

    if success:
        await interaction.response.send_message(
            embed=embed_ok("Admin Action Completed", msg)
        )
    else:
        await interaction.response.send_message(
            embed=embed_err(f"Action '{action}' failed.")
        )


async def admin_kill_all(interaction: discord.Interaction) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(
            embed=embed_err("Admin only."), ephemeral=True
        )
        return

    await interaction.response.defer()

    with get_db() as conn:
        rows = conn.execute(
            "SELECT container_id FROM vps WHERE status = 'running'"
        ).fetchall()

    stopped = 0
    for row in rows:
        cid = row["container_id"]
        if await async_docker_stop(cid):
            update_vps_status(cid, "stopped")
            stopped += 1
            logger.info("Stopped %s", cid)

    await interaction.followup.send(
        embed=embed_ok(
            "Admin: Kill All",
            f"Successfully stopped {stopped} running VPS instance(s).",
        )
    )


# ════════════════════════════════════════════════════════════════════════════
# Slash commands
# ════════════════════════════════════════════════════════════════════════════

# ── /deploy ───────────────────────────────────────────────────────────────
@bot.tree.command(name="deploy", description="Deploy a new VPS instance")
@app_commands.describe(os_type="Operating system")
@app_commands.choices(os_type=[
    app_commands.Choice(name="Ubuntu 22.04", value="ubuntu"),
    app_commands.Choice(name="Debian 12",    value="debian"),
])
async def deploy(interaction: discord.Interaction, os_type: str) -> None:
    await create_vps(interaction, os_type)


# ── /list ─────────────────────────────────────────────────────────────────
@bot.tree.command(name="list", description="List all your VPS instances")
async def list_vps(interaction: discord.Interaction) -> None:
    vps_list = get_user_vps(interaction.user.id)
    if not vps_list:
        await interaction.response.send_message(
            embed=embed_err("You have no VPS instances."), ephemeral=True
        )
        return

    e = embed_info("Your VPS Instances")
    for vps in vps_list[:25]:
        status_emoji  = "🟢" if vps["status"] == "running" else "🔴"
        uptime        = get_uptime(vps["container_id"])
        suspended_txt = " (Suspended)" if vps["suspended"] else ""
        e.add_field(
            name  = f"{status_emoji} {vps['container_name']} ({vps['os_type']}){suspended_txt}",
            value = (
                f"ID: ```{vps['container_id']}```"
                f"Status: {vps['status']} | Uptime: {uptime}\n"
                f"Resources: {vps['ram']} RAM | {vps['cpu']} CPU | {vps['disk']} Disk"
            ),
            inline=False,
        )
    await interaction.response.send_message(embed=e, ephemeral=True)


# ── /vps-info ─────────────────────────────────────────────────────────────
@bot.tree.command(name="vps-info", description="View full details of your VPS")
@app_commands.describe(vps_identifier="VPS ID or name (defaults to first)")
async def vps_info(
    interaction: discord.Interaction, vps_identifier: str | None = None
) -> None:
    vps = get_vps_by_identifier(interaction.user.id, vps_identifier)
    if not vps:
        await interaction.response.send_message(
            embed=embed_err("No VPS found."), ephemeral=True
        )
        return

    container_id = vps["container_id"]
    stats        = get_stats(container_id)
    uptime       = get_uptime(container_id)
    os_name      = "Ubuntu 22.04" if vps["os_type"] == "ubuntu" else "Debian 12"

    e = embed_info(f"VPS Details: {vps['container_name']}")
    e.add_field(name="OS",        value=os_name,                          inline=True)
    e.add_field(name="Hostname",  value=vps["hostname"],                  inline=True)
    e.add_field(name="Status",    value=vps["status"],                    inline=True)
    e.add_field(name="Suspended", value="Yes" if vps["suspended"] else "No", inline=True)
    e.add_field(name="Container ID", value=f"```{container_id}```",       inline=False)
    e.add_field(name="Allocated", value=f"{vps['ram']} RAM | {vps['cpu']} CPU | {vps['disk']} Disk", inline=False)
    e.add_field(name="Usage",     value=f"CPU: {stats['cpu']} | Mem: {stats['mem']}", inline=False)
    e.add_field(name="Uptime",    value=uptime,                           inline=True)
    e.add_field(name="Net I/O",   value=stats["net"],                     inline=True)
    e.add_field(name="Created",   value=vps["created_at"],                inline=True)
    if vps["ssh_command"]:
        ssh_display = vps["ssh_command"][:100] + ("…" if len(vps["ssh_command"]) > 100 else "")
        e.add_field(name="SSH Command", value=f"```{ssh_display}```",     inline=False)

    await interaction.response.send_message(embed=e, ephemeral=True)


# ── /start / /stop / /restart ─────────────────────────────────────────────
@bot.tree.command(name="start", description="Start your VPS")
@app_commands.describe(vps_identifier="VPS ID or name")
async def start_vps(interaction: discord.Interaction, vps_identifier: str) -> None:
    await manage_vps(interaction, vps_identifier, "start")


@bot.tree.command(name="stop", description="Stop your VPS")
@app_commands.describe(vps_identifier="VPS ID or name")
async def stop_vps(interaction: discord.Interaction, vps_identifier: str) -> None:
    await manage_vps(interaction, vps_identifier, "stop")


@bot.tree.command(name="restart", description="Restart your VPS")
@app_commands.describe(vps_identifier="VPS ID or name")
async def restart_vps(interaction: discord.Interaction, vps_identifier: str) -> None:
    await manage_vps(interaction, vps_identifier, "restart")


# ── /regen-ssh ────────────────────────────────────────────────────────────
@bot.tree.command(name="regen-ssh", description="Regenerate SSH session for your VPS")
@app_commands.describe(vps_identifier="VPS ID or name (defaults to first)")
async def regen_ssh(
    interaction: discord.Interaction, vps_identifier: str | None = None
) -> None:
    await regen_ssh_command(interaction, vps_identifier)


# ── /reinstall ────────────────────────────────────────────────────────────
@bot.tree.command(name="reinstall", description="Reinstall your VPS with a new OS")
@app_commands.describe(vps_identifier="VPS ID or name", os_type="New OS")
@app_commands.choices(os_type=[
    app_commands.Choice(name="Ubuntu 22.04", value="ubuntu"),
    app_commands.Choice(name="Debian 12",    value="debian"),
])
async def reinstall(
    interaction: discord.Interaction, vps_identifier: str, os_type: str = "ubuntu"
) -> None:
    await reinstall_vps(interaction, vps_identifier, os_type)


# ── /remove ───────────────────────────────────────────────────────────────
@bot.tree.command(name="remove", description="Remove a VPS instance")
@app_commands.describe(vps_identifier="VPS ID or name")
async def remove_vps(interaction: discord.Interaction, vps_identifier: str) -> None:
    await interaction.response.defer(ephemeral=True)
    vps = get_vps_by_identifier(interaction.user.id, vps_identifier)
    if not vps:
        await interaction.followup.send(embed=embed_err("VPS not found."), ephemeral=True)
        return
    container_id = vps["container_id"]
    await async_docker_stop(container_id)
    await asyncio.sleep(2)
    await async_docker_rm(container_id)
    delete_vps(container_id)
    await interaction.followup.send(embed=embed_ok("VPS Removed"), ephemeral=True)


# ── /logs ─────────────────────────────────────────────────────────────────
@bot.tree.command(name="logs", description="View recent logs for your VPS")
@app_commands.describe(vps_identifier="VPS ID or name", lines="Lines to show (default 50)")
async def user_logs(
    interaction: discord.Interaction, vps_identifier: str, lines: int = 50
) -> None:
    vps = get_vps_by_identifier(interaction.user.id, vps_identifier)
    if not vps:
        await interaction.response.send_message(
            embed=embed_err("VPS not found."), ephemeral=True
        )
        return
    log_text = get_logs(vps["container_id"], lines)
    e = embed_info(f"Logs: {vps['container_name']}")
    e.add_field(name="Recent Logs", value=f"```{log_text[:1000]}```", inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


# ── /ping ─────────────────────────────────────────────────────────────────
@bot.tree.command(name="ping", description="Check bot latency")
async def ping(interaction: discord.Interaction) -> None:
    lat = round(bot.latency * 1000)
    await interaction.response.send_message(
        embed=embed_ok("🏓 Pong!", f"Latency: {lat}ms"), ephemeral=True
    )


# ── /about ────────────────────────────────────────────────────────────────
@bot.tree.command(name="about", description="Show bot & developer information")
async def about(interaction: discord.Interaction) -> None:
    e = discord.Embed(
        title="🤖 VPS Manager Bot • About",
        description=(
            "**A powerful, fast, and user-friendly Discord bot for managing VPS servers "
            "and Docker containers.**\n\n"
            "Designed with **speed**, **stability**, **security**, and **simplicity** in mind 🚀🔒"
        ),
        color=discord.Color.from_rgb(88, 101, 242),
        timestamp=datetime.now(timezone.utc),
    )
    e.add_field(
        name="📌 Bot Information",
        value=(
            "➜ **Name:** VPS Manager Bot\n"
            "➜ **Version:** v1.1\n"
            "➜ **Framework:** Python • discord.py\n"
            "➜ **Uptime Status:** 🟢 Online & Stable"
        ),
        inline=False,
    )
    e.add_field(
        name="👨‍💻 Developer • Hopingboyz",
        value=(
            "📺 [YouTube](https://www.youtube.com/@Hopingboyz)  "
            "💻 [GitHub](https://github.com/Hopingboyz)  "
            "📸 [Instagram](https://instagram.com/hopingboyz)"
        ),
        inline=False,
    )
    e.set_footer(text="Built with ❤️ by Hopingboyz")
    await interaction.response.send_message(embed=e, ephemeral=True)


# ── /help ─────────────────────────────────────────────────────────────────
@bot.tree.command(name="help", description="Command list")
async def help_cmd(interaction: discord.Interaction) -> None:
    e = embed_info("VPS Bot Help")
    user_cmds = [
        ("/deploy <os>",                  "Deploy a new VPS"),
        ("/list",                         "List your VPS instances"),
        ("/vps-info [id]",                "View VPS details"),
        ("/start <id>",                   "Start a VPS"),
        ("/stop <id>",                    "Stop a VPS"),
        ("/restart <id>",                 "Restart a VPS"),
        ("/regen-ssh [id]",               "Regenerate SSH session"),
        ("/reinstall <id> [os]",          "Reinstall VPS with new OS"),
        ("/remove <id>",                  "Remove a VPS"),
        ("/logs <id> [lines]",            "View container logs"),
        ("/about",                        "Bot information"),
        ("/ping",                         "Check latency"),
    ]
    for name, desc in user_cmds:
        e.add_field(name=name, value=desc, inline=False)

    if ADMIN_ID > 0:
        e.add_field(name="\u200b", value="**Admin Commands**", inline=False)
        admin_cmds = [
            ("/admin-create <user> <os> [ram] [cpu] [disk]", "Create VPS for a user"),
            ("/admin-manage <user> <id> <action>",           "Manage a user's VPS"),
            ("/admin-list",                                  "List all VPS instances"),
            ("/admin-list-users",                            "List users with VPS counts"),
            ("/admin-stats",                                 "Bot statistics"),
            ("/admin-vps-info <user> <id>",                  "Full VPS details"),
            ("/admin-logs <user> <id> [lines]",              "View VPS logs"),
            ("/admin-delete-user <user>",                    "Delete all VPS for a user"),
            ("/admin-ban <user>",                            "Ban a user"),
            ("/admin-unban <user>",                          "Unban a user"),
            ("/admin-kill-all",                              "Stop all running VPS"),
        ]
        for name, desc in admin_cmds:
            e.add_field(name=name, value=desc, inline=False)

    await interaction.response.send_message(embed=e, ephemeral=True)


# ════════════════════════════════════════════════════════════════════════════
# Admin slash commands
# ════════════════════════════════════════════════════════════════════════════

@bot.tree.command(name="admin-create", description="Admin: Create a VPS for a user")
@app_commands.describe(
    target_user="Target user",
    os_type="OS type",
    ram="RAM (e.g. 2g)",
    cpu="CPU cores",
    disk="Disk (e.g. 10G)",
)
@app_commands.choices(os_type=[
    app_commands.Choice(name="Ubuntu 22.04", value="ubuntu"),
    app_commands.Choice(name="Debian 12",    value="debian"),
])
@app_commands.guild_only()
async def admin_create(
    interaction: discord.Interaction,
    target_user: discord.User,
    os_type: str,
    ram:  str | None = None,
    cpu:  str | None = None,
    disk: str | None = None,
) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return
    if get_total_running() >= TOTAL_SERVER_LIMIT:
        await interaction.response.send_message(
            embed=embed_err(f"Global limit reached: {TOTAL_SERVER_LIMIT} running."),
            ephemeral=True,
        )
        return
    await create_vps(
        interaction, os_type,
        ram  or DEFAULT_RAM,
        cpu  or DEFAULT_CPU,
        disk or DEFAULT_DISK,
        target_user=target_user,
    )


@bot.tree.command(name="admin-manage", description="Admin: Manage a user's VPS")
@app_commands.describe(
    target_user="Target user",
    vps_identifier="VPS ID or name",
    action="Action to perform",
)
@app_commands.choices(action=[
    app_commands.Choice(name="start",     value="start"),
    app_commands.Choice(name="stop",      value="stop"),
    app_commands.Choice(name="restart",   value="restart"),
    app_commands.Choice(name="delete",    value="delete"),
    app_commands.Choice(name="suspend",   value="suspend"),
    app_commands.Choice(name="unsuspend", value="unsuspend"),
])
@app_commands.guild_only()
async def admin_manage(
    interaction: discord.Interaction,
    target_user: discord.User,
    vps_identifier: str,
    action: str,
) -> None:
    await admin_manage_vps(interaction, target_user.id, vps_identifier, action)


@bot.tree.command(name="admin-kill-all", description="Admin: Stop all running VPS instances")
@app_commands.guild_only()
async def admin_kill_all_cmd(interaction: discord.Interaction) -> None:
    await admin_kill_all(interaction)


@bot.tree.command(name="admin-list", description="Admin: List all VPS instances")
@app_commands.guild_only()
async def admin_list(interaction: discord.Interaction) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return

    with get_db() as conn:
        rows = conn.execute("""
            SELECT u.username, v.container_id, v.container_name, v.os_type,
                   v.hostname, v.status, v.ram, v.cpu, v.disk, v.suspended
            FROM vps v JOIN users u ON v.user_id = u.user_id
            ORDER BY v.created_at DESC
        """).fetchall()

    if not rows:
        await interaction.response.send_message(
            embed=embed_err("No VPS instances found.")
        )
        return

    e = embed_info("All VPS Instances")
    for row in rows[:25]:
        status_emoji  = "🟢" if row["status"] == "running" else "🔴"
        suspended_txt = " (Suspended)" if row["suspended"] else ""
        e.add_field(
            name  = f"{status_emoji} {row['username']} – {row['container_name']} ({row['os_type']}){suspended_txt}",
            value = (
                f"ID: ```{row['container_id']}```"
                f"Status: {row['status']}\n"
                f"Resources: {row['ram']} RAM | {row['cpu']} CPU | {row['disk']} Disk"
            ),
            inline=False,
        )
    if len(rows) > 25:
        e.set_footer(
            text=f"{WATERMARK} | Showing first 25 of {len(rows)}",
            icon_url=_footer_url(),
        )
    await interaction.response.send_message(embed=e)


@bot.tree.command(name="admin-list-users", description="Admin: List users with VPS counts")
@app_commands.guild_only()
async def admin_list_users(interaction: discord.Interaction) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return

    with get_db() as conn:
        rows = conn.execute("""
            SELECT u.username,
                   COUNT(v.id) AS total_vps,
                   SUM(CASE WHEN v.status = 'running' THEN 1 ELSE 0 END) AS running_vps
            FROM users u LEFT JOIN vps v ON u.user_id = v.user_id
            GROUP BY u.user_id, u.username
            ORDER BY total_vps DESC
        """).fetchall()

    if not rows:
        await interaction.response.send_message(embed=embed_err("No users found."))
        return

    e = embed_info("Users Overview")
    for row in rows[:25]:
        e.add_field(
            name  = row["username"],
            value = f"Total VPS: {row['total_vps']} | Running: {row['running_vps'] or 0}",
            inline=False,
        )
    await interaction.response.send_message(embed=e)


@bot.tree.command(name="admin-stats", description="Admin: View bot statistics")
@app_commands.guild_only()
async def admin_stats(interaction: discord.Interaction) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return

    with get_db() as conn:
        num_users   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        num_vps     = conn.execute("SELECT COUNT(*) FROM vps").fetchone()[0]
        num_running = conn.execute("SELECT COUNT(*) FROM vps WHERE status='running'").fetchone()[0]
        num_banned  = conn.execute("SELECT COUNT(*) FROM bans").fetchone()[0]
        running     = conn.execute("SELECT ram, cpu, disk FROM vps WHERE status='running'").fetchall()

    total_cpu  = sum(float(r["cpu"])    for r in running)
    total_ram  = sum(parse_gb(r["ram"]) for r in running)
    total_disk = sum(parse_gb(r["disk"]) for r in running)

    e = embed_info("Bot Statistics")
    e.add_field(name="Total Users",     value=num_users,                 inline=True)
    e.add_field(name="Banned Users",    value=num_banned,                inline=True)
    e.add_field(name="Total VPS",       value=num_vps,                   inline=True)
    e.add_field(name="Running VPS",     value=num_running,               inline=True)
    e.add_field(name="CPU Allocated",   value=f"{total_cpu} cores",      inline=True)
    e.add_field(name="RAM Allocated",   value=f"{total_ram:.1f} GB",     inline=True)
    e.add_field(name="Disk Allocated",  value=f"{total_disk:.1f} GB",    inline=True)
    await interaction.response.send_message(embed=e)


@bot.tree.command(name="admin-delete-user", description="Admin: Delete all VPS for a user")
@app_commands.describe(target_user="Target user")
@app_commands.guild_only()
async def admin_delete_user(
    interaction: discord.Interaction, target_user: discord.User
) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return

    await interaction.response.defer()
    vps_list = get_user_vps(target_user.id)
    deleted  = 0
    for vps in vps_list:
        cid = vps["container_id"]
        await async_docker_stop(cid)
        await asyncio.sleep(1)
        await async_docker_rm(cid)
        delete_vps(cid)
        deleted += 1
        logger.info("Admin deleted VPS %s for user %s", cid, target_user.id)

    await interaction.followup.send(
        embed=embed_ok(description=f"Deleted {deleted} VPS instance(s) for {target_user}.")
    )


@bot.tree.command(name="admin-ban", description="Admin: Ban a user from creating VPS")
@app_commands.describe(target_user="Target user")
@app_commands.guild_only()
async def admin_ban(interaction: discord.Interaction, target_user: discord.User) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return
    add_ban(target_user.id)
    await interaction.response.send_message(
        embed=embed_ok(description=f"Banned {target_user} from creating VPS instances.")
    )


@bot.tree.command(name="admin-unban", description="Admin: Unban a user")
@app_commands.describe(target_user="Target user")
@app_commands.guild_only()
async def admin_unban(interaction: discord.Interaction, target_user: discord.User) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return
    remove_ban(target_user.id)
    await interaction.response.send_message(
        embed=embed_ok(description=f"Unbanned {target_user}.")
    )


@bot.tree.command(name="admin-vps-info", description="Admin: Full VPS details for a user")
@app_commands.describe(target_user="Target user", vps_identifier="VPS ID or name")
@app_commands.guild_only()
async def admin_vps_info(
    interaction: discord.Interaction,
    target_user: discord.User,
    vps_identifier: str,
) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return

    vps = get_vps_by_identifier(target_user.id, vps_identifier)
    if not vps:
        await interaction.response.send_message(embed=embed_err("VPS not found."))
        return

    container_id = vps["container_id"]
    stats        = get_stats(container_id)
    uptime       = get_uptime(container_id)
    os_name      = "Ubuntu 22.04" if vps["os_type"] == "ubuntu" else "Debian 12"

    e = embed_info(f"{target_user.name} – VPS: {vps['container_name']}")
    e.add_field(name="OS",        value=os_name,                             inline=True)
    e.add_field(name="Hostname",  value=vps["hostname"],                     inline=True)
    e.add_field(name="Status",    value=vps["status"],                       inline=True)
    e.add_field(name="Suspended", value="Yes" if vps["suspended"] else "No", inline=True)
    e.add_field(name="Container ID", value=f"```{container_id}```",          inline=False)
    e.add_field(name="Allocated", value=f"{vps['ram']} RAM | {vps['cpu']} CPU | {vps['disk']} Disk", inline=False)
    e.add_field(name="Usage",     value=f"CPU: {stats['cpu']} | Mem: {stats['mem']}", inline=False)
    e.add_field(name="Uptime",    value=uptime,                              inline=True)
    e.add_field(name="Net I/O",   value=stats["net"],                        inline=True)
    e.add_field(name="Created",   value=vps["created_at"],                   inline=True)
    if vps["ssh_command"]:
        ssh_display = vps["ssh_command"][:100] + ("…" if len(vps["ssh_command"]) > 100 else "")
        e.add_field(name="SSH Command", value=f"```{ssh_display}```",        inline=False)

    await interaction.response.send_message(embed=e)


@bot.tree.command(name="admin-logs", description="Admin: View logs for a user's VPS")
@app_commands.describe(
    target_user="Target user",
    vps_identifier="VPS ID or name",
    lines="Lines (default 50)",
)
@app_commands.guild_only()
async def admin_logs(
    interaction: discord.Interaction,
    target_user: discord.User,
    vps_identifier: str,
    lines: int = 50,
) -> None:
    if not is_admin(interaction.user):
        await interaction.response.send_message(embed=embed_err("Admin only."), ephemeral=True)
        return

    vps = get_vps_by_identifier(target_user.id, vps_identifier)
    if not vps:
        await interaction.response.send_message(embed=embed_err("VPS not found."))
        return

    log_text = get_logs(vps["container_id"], lines)
    e = embed_info(f"Logs: {target_user.name} – {vps['container_name']}")
    e.add_field(name="Recent Logs", value=f"```{log_text[:1000]}```", inline=False)
    await interaction.response.send_message(embed=e)


# ════════════════════════════════════════════════════════════════════════════
# Background tasks
# ════════════════════════════════════════════════════════════════════════════

@tasks.loop(minutes=5)
async def sync_statuses() -> None:
    """Keep DB status in sync with actual Docker container state."""
    with get_db() as conn:
        rows = conn.execute("SELECT container_id, status FROM vps").fetchall()

    for row in rows:
        cid  = row["container_id"]
        stat = row["status"]
        try:
            actual = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Status}}", cid],
                stderr=subprocess.STDOUT,
            ).decode().strip()
            if actual != stat:
                update_vps_status(cid, actual)
                logger.info("Synced %s → %s", cid, actual)
        except subprocess.CalledProcessError:
            if stat != "stopped":
                update_vps_status(cid, "stopped")
                logger.info("Container %s gone → marked stopped", cid)
        except Exception as exc:
            logger.error("Status sync error for %s: %s", cid, exc)


@tasks.loop(seconds=10)
async def change_status() -> None:
    try:
        count = get_total_running()
        await bot.change_presence(
            activity=discord.Game(name=f"{BOT_STATUS_NAME} | {count} Active")
        )
    except Exception as exc:
        logger.error("Status update failed: %s", exc)


# ════════════════════════════════════════════════════════════════════════════
# Bot events
# ════════════════════════════════════════════════════════════════════════════

@bot.event
async def on_ready() -> None:
    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)
    change_status.start()
    sync_statuses.start()
    try:
        synced = await bot.tree.sync()
        logger.info("Synced %d slash command(s)", len(synced))
    except Exception as exc:
        logger.error("Command sync failed: %s", exc)


# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not TOKEN:
        logger.error("TOKEN is not set in .env – aborting.")
        sys.exit(1)
    bot.run(TOKEN)
