import discord
from discord.ext import commands
from discord import app_commands
import os, sys, asyncio, random, aiohttp, re
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════
#  ENVIRONMENT
# ══════════════════════════════════════════════════════════
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN environment variable is not set.")

# ══════════════════════════════════════════════════════════
#  CONFIG  — edit these to match your server
# ══════════════════════════════════════════════════════════
STAFF_ROLE_NAME    = "Support"
WELCOME_CHANNEL_ID = 1463606144064032952
BOOST_CHANNEL_ID   = 1469698626283503626
BOOST_ROLE_ID      = 1471512804535046237

PROTECTED_GUILD_ID = 1463580079819849834  # Insta-ban anyone who adds a bot

CRIMSON_GIF    = "https://cdn.discordapp.com/attachments/1470798856085307423/1471984801266532362/IMG_7053.gif"
TICKET_BANNER  = "https://cdn.discordapp.com/attachments/1470798856085307423/1479075133586149386/standard_6.gif"

# Embed colours
C_CRIMSON  = 0xDC143C
C_SUCCESS  = 0x2ECC71
C_WARNING  = 0xF39C12
C_ERROR    = 0xE74C3C
C_INFO     = 0x5865F2
C_BOOST    = 0xFF73FA

WELCOME_ENABLED = True
LEAVE_ENABLED   = True

WELCOME_MESSAGES = [
    "🎉 Welcome to the server, {mention}! We're glad to have you here!",
    "👋 Hey {mention}! Welcome aboard! Make yourself at home!",
    "🌟 {mention} just joined the party! Welcome!",
    "🎊 Everyone welcome {mention} to the server!",
    "✨ {mention} has entered the chat! Welcome!",
]
LEAVE_MESSAGES = [
    "👋 **{name}** has left the server. Goodbye!",
    "😢 **{name}** just left. We'll miss you!",
    "🚪 **{name}** has left the building.",
    "💔 **{name}** decided to leave. Safe travels!",
    "👻 **{name}** has vanished from the server.",
]

MEME_SUBREDDITS = {
    "dankmemes":       "dankmemes",
    "funny":           "funny",
    "me_irl":          "me_irl",
    "surrealmemes":    "surrealmemes",
    "ProgrammerHumor": "ProgrammerHumor",
    "random":          "memes",
}

EMOJI_ROLE_MAP = {
    "35384gatohappymeme": 1472171839324164226,
    "alertorange":        1472172053581795359,
    "21124crownorange":   1472172151145762960,
    "Legit":              1472172311326232577,
}

# ══════════════════════════════════════════════════════════
#  BOT INIT
# ══════════════════════════════════════════════════════════
intents = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=intents)

# ══════════════════════════════════════════════════════════
#  IN-MEMORY STORAGE  (resets on restart)
# ══════════════════════════════════════════════════════════
afk_users       = {}   # {user_id: {"reason": str, "original_nick": str|None}}
ticket_counter  = 0
ticket_transcript_channels = {}  # {guild_id: channel_id}
ticket_creators = {}             # {channel_id: user_id}
warnings_db     = {}   # {user_id: [{"reason","moderator","moderator_name","timestamp","guild_id"}]}
server_stats    = {}   # {guild_id: {"joins","leaves","messages","mod_actions"}}
vouch_db        = {}   # {guild_id: {user_id: [{"by","by_name","timestamp","proof_url"}]}}
vouch_channels  = {}   # {guild_id: channel_id}
boost_channel_id         = BOOST_CHANNEL_ID
reaction_role_message_id = None
bot_start_time  = None



# ── Antinuke ──
antinuke_enabled      = {}   # {guild_id: bool}
antinuke_wl           = {}   # {guild_id: [user_id]}
antinuke_log_channels = {}   # {guild_id: channel_id}
antinuke_actions      = {}   # {guild_id: {user_id: [datetime]}}
AN_THRESHOLD = 3
AN_TIMEFRAME = 10

# ── Auto-Moderation ──
automod_cfg  = {}   # {guild_id: config dict}
automod_spam = {}   # {guild_id: {user_id: [datetime]}}  — spam tracking
automod_dup  = {}   # {guild_id: {user_id: [content]}}   — duplicate tracking
automod_warn_count = {}  # {guild_id: {user_id: int}}    — strike tracking

DEFAULT_BAD_WORDS = [
    "nigger","nigga","faggot","retard","kys","kill yourself",
    "cunt","slut","whore","rape","nonce","pedo","pedophile",
]

# Scam/phishing domains to always block regardless of link filter
SCAM_DOMAINS = [
    "free-nitro","nitro-gift","discord-gift","steamgift","steam-gift",
    "grabify","iplogger","blasze","ps3cfw","bit.ly/free","freegift",
    "discordnitro","giveway","giveaways.com","claim-nitro",
]

# NSFW domains — always blocked regardless of any filter setting
NSFW_DOMAINS = [
    "pornhub","xvideos","xhamster","redtube","youporn","tube8","xnxx",
    "xxx","porn","nsfw","onlyfans","fansly","manyvids","4chan","rule34",
    "gelbooru","danbooru","hentai","nhentai","e-hentai","8muses",
    "spankbang","beeg","chaturbate","cam4","bongacams","stripchat",
    "livejasmin","myfreecams","nsfwreddit","brazzers","bangbros",
    "realitykings","mofos","pornhub","slutty","sexvid","sexhd","fux",
]

INVITE_RE = re.compile(
    r"(discord\.gg/|discord\.com/invite/|discordapp\.com/invite/|dsc\.gg/|invite\.gg/)\S+", re.I
)
# Catches http, https, www links — also catches sneaky dot-spaced links like "google . com"
LINK_RE = re.compile(
    r"https?://[^\s]+|www\.[^\s]+|\b\w+\s*\.\s*(com|net|org|gg|io|co|tv|me|xyz|ru|tk|ml|cf|ga)\b", re.I
)
# Catches invite bypasses like "discord .gg/xxx" or "disc ord.gg"
INVITE_BYPASS_RE = re.compile(
    r"dis\s*c?\s*o?\s*r?\s*d\s*[\.\s]\s*g\s*g\s*/\s*\S+|"
    r"dis\s*c\s*o\s*r\s*d\s*[\.\s]\s*c\s*o\s*m\s*/\s*invite", re.I
)
ZALGO_RE   = re.compile(r"[\u0300-\u036f\u0489]")
EMOJI_RE   = re.compile(r"<a?:\w+:\d+>|[\U0001F300-\U0001FAFF]")
CAPS_MIN_LEN = 8
CAPS_PCT     = 0.70
SPAM_COUNT   = 5
SPAM_WINDOW  = 5    # seconds
MENTION_MAX  = 5
DUP_COUNT    = 3    # same message X times = spam
DUP_WINDOW   = 30   # seconds
MAX_MSG_LEN  = 1200 # characters
STRIKE_TIMEOUT = {  # strikes → timeout duration in seconds
    3: 60,
    5: 300,
    7: 3600,
}

def get_automod(guild_id: int) -> dict:
    if guild_id not in automod_cfg:
        automod_cfg[guild_id] = {
            "enabled":          False,
            "log_channel":      None,
            "filter_profanity": True,
            "filter_invites":   True,
            "filter_links":     True,
            "filter_caps":      True,
            "filter_spam":      True,
            "filter_mentions":  True,
            "filter_zalgo":     True,
            "filter_emoji":     False,
            "filter_duplicates":True,    # NEW
            "filter_length":    False,   # NEW — block walls of text
            "filter_scam":      True,    # NEW — always block scam links
            "warn_on_delete":   True,
            "auto_timeout":     True,    # NEW — timeout on repeat offenses
            "custom_words":     [],
            "whitelist_roles":  [],
            "whitelist_channels":[],
        }
    return automod_cfg[guild_id]

def automod_immune(member: discord.Member, cfg: dict) -> bool:
    if member.guild_permissions.administrator: return True
    if is_staff(member): return True
    if any(r.id in cfg["whitelist_roles"] for r in member.roles): return True
    return False

async def automod_strike(member: discord.Member, guild: discord.Guild, cfg: dict):
    """Track strikes and auto-timeout repeat offenders."""
    if not cfg.get("auto_timeout"): return
    gid = guild.id
    uid = member.id
    automod_warn_count.setdefault(gid, {})
    automod_warn_count[gid][uid] = automod_warn_count[gid].get(uid, 0) + 1
    strikes = automod_warn_count[gid][uid]
    for threshold, duration in sorted(STRIKE_TIMEOUT.items()):
        if strikes == threshold:
            try:
                until = discord.utils.utcnow() + timedelta(seconds=duration)
                await member.timeout(until, reason=f"AutoMod: {strikes} strikes")
                log_id = cfg.get("log_channel")
                if log_id and (ch := guild.get_channel(log_id)):
                    mins = duration // 60 if duration >= 60 else None
                    dur_str = f"{duration // 3600}h" if duration >= 3600 else f"{duration // 60}m" if mins else f"{duration}s"
                    e = _base("⏱️  AutoMod — Auto Timeout", color=C_ERROR)
                    e.set_thumbnail(url=member.display_avatar.url)
                    e.add_field(name="👤 User",     value=f"{member.mention}\n`{member.id}`", inline=True)
                    e.add_field(name="⚡ Strikes",  value=str(strikes),                       inline=True)
                    e.add_field(name="⏱️ Duration", value=dur_str,                            inline=True)
                    ft(e, "Crimson Gen • AutoMod")
                    await ch.send(embed=e)
            except Exception: pass
            break

async def automod_action(msg: discord.Message, reason: str, cfg: dict):
    """Delete message, warn user, log, and track strikes."""
    try: await msg.delete()
    except Exception: pass

    member = msg.author
    guild  = msg.guild

    if cfg.get("warn_on_delete"):
        try:
            await msg.channel.send(
                embed=warn("Message Removed", f"{member.mention} — **{reason}**\nPlease follow the server rules."),
                delete_after=6
            )
        except Exception: pass

    # Log
    log_id = cfg.get("log_channel")
    if log_id and (ch := guild.get_channel(log_id)):
        e = _base("🤖  AutoMod — Message Removed", color=C_WARNING)
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="👤 User",    value=f"{member.mention}\n`{member.id}`",           inline=True)
        e.add_field(name="📍 Channel", value=msg.channel.mention,                           inline=True)
        e.add_field(name="⚡ Reason",  value=reason,                                        inline=True)
        e.add_field(name="💬 Content", value=f"```{msg.content[:900] or '[no text]'}```",  inline=False)
        ft(e, "Crimson Gen • AutoMod")
        await ch.send(embed=e)

    # Strike system
    await automod_strike(member, guild, cfg)

# ══════════════════════════════════════════════════════════
#  EMBED HELPERS
# ══════════════════════════════════════════════════════════
def _base(title="", desc="", color=C_CRIMSON) -> discord.Embed:
    return discord.Embed(title=title, description=desc, color=color, timestamp=datetime.utcnow())

def ok(title: str, desc: str = "")    -> discord.Embed: return _base(f"✅  {title}", desc, C_SUCCESS)
def err(title: str, desc: str = "")   -> discord.Embed: return _base(f"❌  {title}", desc, C_ERROR)
def warn(title: str, desc: str = "")  -> discord.Embed: return _base(f"⚠️  {title}", desc, C_WARNING)
def info(title: str, desc: str = "")  -> discord.Embed: return _base(f"ℹ️  {title}", desc, C_INFO)

def ft(embed: discord.Embed, text="Crimson Gen", icon=None) -> discord.Embed:
    embed.set_footer(text=text, icon_url=icon)
    return embed

# ══════════════════════════════════════════════════════════
#  MISC HELPERS
# ══════════════════════════════════════════════════════════
def get_stats(guild_id: int) -> dict:
    if guild_id not in server_stats:
        server_stats[guild_id] = {"joins": 0, "leaves": 0, "messages": 0, "mod_actions": 0}
    return server_stats[guild_id]

BOT_OWNER_ID = 983407797972656129

def is_staff(member: discord.Member) -> bool:
    if member.id == BOT_OWNER_ID: return True
    role = discord.utils.get(member.guild.roles, name=STAFF_ROLE_NAME)
    return (role and role in member.roles) or member.guild_permissions.administrator

def get_vouches(guild_id, user_id): return vouch_db.get(guild_id, {}).get(user_id, [])

def parse_duration(s: str):
    s = s.lower().strip()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    labels = {"s": "second", "m": "minute", "h": "hour", "d": "day"}
    if s and s[-1] in units:
        try:
            val = int(s[:-1])
            label = f"{val} {labels[s[-1]]}{'s' if val != 1 else ''}"
            return val * units[s[-1]], label
        except ValueError: pass
    raise ValueError("Invalid duration format")

# ══════════════════════════════════════════════════════════
#  ANTINUKE HELPERS
# ══════════════════════════════════════════════════════════
def an_on(guild_id):         return antinuke_enabled.get(guild_id, False)
def an_wl(guild_id, uid):    return uid in antinuke_wl.get(guild_id, [])

def an_track(guild_id: int, user_id: int) -> bool:
    now = datetime.utcnow()
    antinuke_actions.setdefault(guild_id, {}).setdefault(user_id, [])
    antinuke_actions[guild_id][user_id] = [
        t for t in antinuke_actions[guild_id][user_id]
        if (now - t).total_seconds() < AN_TIMEFRAME
    ]
    antinuke_actions[guild_id][user_id].append(now)
    return len(antinuke_actions[guild_id][user_id]) >= AN_THRESHOLD

async def an_punish(guild: discord.Guild, offender: discord.Member, action_type: str, detail: str = ""):
    # Strip roles (keep integration-managed & default)
    try:
        safe = [r for r in offender.roles if r.managed or r.is_default()]
        await offender.edit(roles=safe, reason=f"[ANTINUKE] {action_type}")
    except Exception: pass

    # DM before ban
    try:
        e = _base("🛡️  You were banned by Antinuke",
                  f"**Server:** {guild.name}\n**Reason:** {action_type}\n**Detail:** {detail or 'N/A'}", C_ERROR)
        await offender.send(embed=e)
    except Exception: pass

    # Ban
    try:
        await guild.ban(offender, reason=f"[ANTINUKE] {action_type}", delete_message_days=0)
    except Exception: pass

    # Log
    log_id = antinuke_log_channels.get(guild.id)
    if log_id and (ch := guild.get_channel(log_id)):
        e = _base("🛡️  Antinuke — Threat Neutralised", color=C_ERROR)
        e.set_thumbnail(url=offender.display_avatar.url)
        e.add_field(name="👤 Offender",    value=f"{offender.mention}\n`{offender.id}`", inline=True)
        e.add_field(name="⚡ Type",        value=f"`{action_type}`",                     inline=True)
        e.add_field(name="📋 Detail",      value=detail or "N/A",                        inline=False)
        e.add_field(name="⚙️ Punishment",  value="Roles stripped → Banned",              inline=True)
        ft(e, "Crimson Gen • Antinuke")
        await ch.send(embed=e)

# ══════════════════════════════════════════════════════════
#  ON READY
# ══════════════════════════════════════════════════════════
@bot.event
async def on_ready():
    global bot_start_time
    bot_start_time = datetime.utcnow()
    bot.add_view(TicketPanelView())
    bot.add_view(TicketActionsView())
    bot.add_view(ClosedTicketView())
    bot.add_view(StaffTicketInfoView())


    try:
        synced = await bot.tree.sync()
        # Also sync to each guild for instant updates
        for guild in bot.guilds:
            try:
                await bot.tree.sync(guild=guild)
            except Exception:
                pass
        print(f"✅  Synced {len(synced)} commands as {bot.user}")
    except Exception as e:
        print(f"❌  Sync failed: {e}")
    await bot.change_presence(
        activity=discord.Streaming(name="Crimson Gen", url="https://twitch.tv/crimsongen"),
        status=discord.Status.online
    )

# ══════════════════════════════════════════════════════════
#  ON MESSAGE
# ══════════════════════════════════════════════════════════
@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot: return
    if msg.guild: get_stats(msg.guild.id)["messages"] += 1
    m = msg.author

    # ── AutoMod ──────────────────────────────────────────────
    if msg.guild:
        cfg = get_automod(msg.guild.id)
        if cfg["enabled"] and not automod_immune(m, cfg) and msg.channel.id not in cfg["whitelist_channels"]:
            content = msg.content or ""
            lower   = content.lower()
            blocked = False

            # ── NSFW links (always blocked, no toggle needed) ──
            if not blocked and any(s in lower for s in NSFW_DOMAINS):
                await automod_action(msg, "NSFW link not allowed here", cfg); blocked = True

            # ── Scam / phishing links (always checked first) ──
            if not blocked and cfg.get("filter_scam"):
                if any(s in lower for s in SCAM_DOMAINS):
                    await automod_action(msg, "Scam / phishing link detected", cfg); blocked = True

            # ── Invite bypass detection ────────────────────────
            if not blocked and cfg["filter_invites"] and INVITE_BYPASS_RE.search(content):
                await automod_action(msg, "Unauthorised Discord invite (bypass attempt)", cfg); blocked = True

            # ── Discord invites ────────────────────────────────
            if not blocked and cfg["filter_invites"] and INVITE_RE.search(content):
                await automod_action(msg, "Unauthorised Discord invite", cfg); blocked = True

            # ── Links ─────────────────────────────────────────
            if not blocked and cfg["filter_links"] and LINK_RE.search(content):
                await automod_action(msg, "Links are not allowed here", cfg); blocked = True

            # ── Profanity / custom words ───────────────────────
            if not blocked and cfg["filter_profanity"]:
                bad = DEFAULT_BAD_WORDS + cfg.get("custom_words", [])
                if any(w in lower for w in bad):
                    await automod_action(msg, "Prohibited language", cfg); blocked = True

            # ── Excessive caps ─────────────────────────────────
            if not blocked and cfg["filter_caps"] and len(content) >= CAPS_MIN_LEN:
                letters = [c for c in content if c.isalpha()]
                if letters and sum(1 for c in letters if c.isupper()) / len(letters) >= CAPS_PCT:
                    await automod_action(msg, "Excessive use of CAPS", cfg); blocked = True

            # ── Mass mentions ──────────────────────────────────
            if not blocked and cfg["filter_mentions"] and len(msg.mentions) > MENTION_MAX:
                await automod_action(msg, f"Mass mentions ({len(msg.mentions)} users)", cfg); blocked = True

            # ── Zalgo text ─────────────────────────────────────
            if not blocked and cfg["filter_zalgo"] and len(ZALGO_RE.findall(content)) > 5:
                await automod_action(msg, "Zalgo / corrupted text", cfg); blocked = True

            # ── Emoji spam ─────────────────────────────────────
            if not blocked and cfg["filter_emoji"] and len(EMOJI_RE.findall(content)) > 8:
                await automod_action(msg, "Excessive emoji spam", cfg); blocked = True

            # ── Message too long ───────────────────────────────
            if not blocked and cfg.get("filter_length") and len(content) > MAX_MSG_LEN:
                await automod_action(msg, f"Message too long ({len(content)} chars, max {MAX_MSG_LEN})", cfg); blocked = True

            # ── Duplicate messages ─────────────────────────────
            if not blocked and cfg.get("filter_duplicates"):
                now = datetime.utcnow()
                gid = msg.guild.id
                uid = m.id
                automod_dup.setdefault(gid, {}).setdefault(uid, [])
                # Keep only recent entries within window
                automod_dup[gid][uid] = [
                    (t, c) for t, c in automod_dup[gid][uid]
                    if (now - t).total_seconds() < DUP_WINDOW
                ]
                automod_dup[gid][uid].append((now, lower.strip()))
                same = sum(1 for _, c in automod_dup[gid][uid] if c == lower.strip())
                if same >= DUP_COUNT:
                    automod_dup[gid][uid] = []
                    await automod_action(msg, f"Duplicate messages ({DUP_COUNT}x in {DUP_WINDOW}s)", cfg); blocked = True

            # ── Spam (rate limit) ──────────────────────────────
            if not blocked and cfg["filter_spam"]:
                now = datetime.utcnow()
                automod_spam.setdefault(msg.guild.id, {}).setdefault(m.id, [])
                automod_spam[msg.guild.id][m.id] = [
                    t for t in automod_spam[msg.guild.id][m.id]
                    if (now - t).total_seconds() < SPAM_WINDOW
                ]
                automod_spam[msg.guild.id][m.id].append(now)
                if len(automod_spam[msg.guild.id][m.id]) >= SPAM_COUNT:
                    automod_spam[msg.guild.id][m.id] = []
                    await automod_action(msg, f"Spamming ({SPAM_COUNT} messages in {SPAM_WINDOW}s)", cfg)
                    blocked = True

            if blocked: return
    # ─────────────────────────────────────────────────────────

    # AFK removal
    if m.id in afk_users:
        data = afk_users.pop(m.id)
        try:
            if m.display_name.startswith("[AFK]"):
                await m.edit(nick=data.get("original_nick"), reason="No longer AFK")
        except Exception: pass
        await msg.channel.send(embed=ok("Welcome Back!", f"{m.mention}, your AFK status has been removed."), delete_after=6)
    # Ping AFK user
    for user in msg.mentions:
        if user.id in afk_users:
            await msg.channel.send(
                embed=info("User is AFK", f"{user.mention} is AFK\n**Reason:** {afk_users[user.id]['reason']}"),
                delete_after=10
            )


    # ─────────────────────────────────────────────────────────

    # ── Anti-Everyone (antieveryone.py) ──────────────────────
    if msg.guild and msg.mention_everyone and an_on(msg.guild.id):
        if not _an_immune(msg.guild, msg.author.id) and _an_can_act(msg.guild.id, "mention_everyone"):
            # Timeout 1 hour
            retries = 3
            while retries > 0:
                try:
                    await msg.author.edit(
                        timed_out_until=discord.utils.utcnow() + timedelta(seconds=3600),
                        reason="Mentioned Everyone/Here | Unwhitelisted User"
                    )
                    break
                except discord.Forbidden: break
                except discord.HTTPException as e:
                    if e.status == 429:
                        await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
                    else: break
                except Exception: break
            # Delete @everyone messages in channel
            retries = 3
            while retries > 0:
                try:
                    async for m in msg.channel.history(limit=100):
                        if m.mention_everyone:
                            await m.delete()
                            await asyncio.sleep(3)
                    break
                except discord.Forbidden: break
                except discord.HTTPException as e:
                    if e.status == 429:
                        await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
                    else: break
                except Exception: break
            await _an_log(msg.guild, "Anti-Everyone Mention", msg.author, f"#{msg.channel.name}", "User timed out 1hr · Messages deleted")

    await bot.process_commands(msg)

# ══════════════════════════════════════════════════════════
#  WELCOME / LEAVE
# ══════════════════════════════════════════════════════════
@bot.event
async def on_member_join(member: discord.Member):
    get_stats(member.guild.id)["joins"] += 1

    # ── Bot protection — always on for protected guild ──────
    if member.bot:
        guild = member.guild
        always_protected = guild.id == PROTECTED_GUILD_ID
        if always_protected or an_on(guild.id):
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.bot_add):
                adder   = entry.user
                adder_m = guild.get_member(adder.id)
                is_immune = (
                    adder.bot or
                    adder.id == BOT_OWNER_ID or
                    adder.id == guild.owner_id or
                    an_wl(guild.id, adder.id) or
                    (adder_m and adder_m.guild_permissions.administrator)
                )
                # If adder is immune — allow the bot, do nothing
                if is_immune:
                    break
                # Ban the bot and the adder
                try: await guild.ban(member, reason="Unauthorised bot — not allowed in this server", delete_message_days=0)
                except Exception: pass
                if adder_m:
                    try: await adder_m.ban(reason="Added an unauthorised bot to the server", delete_message_days=0)
                    except Exception: pass
                # Log
                log_id = antinuke_log_channels.get(guild.id)
                if log_id and (ch := guild.get_channel(log_id)):
                    e = _base("🤖  Unauthorised Bot Blocked", color=C_ERROR)
                    e.set_thumbnail(url=member.display_avatar.url)
                    e.add_field(name="🤖  Bot",      value=f"{member.mention}\n`{member.id}`", inline=True)
                    e.add_field(name="👤  Added By", value=f"{adder.mention}\n`{adder.id}`",   inline=True)
                    e.add_field(name="⚙️  Action",   value="Bot banned · Adder banned",         inline=False)
                    ft(e, "Crimson Gen • Protection")
                    await ch.send(embed=e)
                break
            return

    if not WELCOME_ENABLED: return
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch: return
    e = _base("🎉  Welcome to the Server!", color=C_SUCCESS)
    e.description = (
        f"Hey {member.mention}, welcome to **{member.guild.name}**!\n\n"
        f"📅 **Account Created:** <t:{int(member.created_at.timestamp())}:R>\n"
        f"👥 **You are member #{member.guild.member_count}**\n\n"
        f"📚 **Get Started**\n"
        f"› Read the rules  ›  Grab your roles  ›  Say hello!"
    )
    e.set_thumbnail(url=member.display_avatar.url)
    e.set_image(url=CRIMSON_GIF)
    ft(e, f"{member.guild.name} • Welcome", member.guild.icon.url if member.guild.icon else None)
    await ch.send(content=member.mention, embed=e)

@bot.event
async def on_member_remove(member: discord.Member):
    get_stats(member.guild.id)["leaves"] += 1
    guild = member.guild

    # ── Anti-Kick (antikick.py) ───────────────────────────
    if an_on(guild.id) and _an_can_act(guild.id, "kick", max_requests=6):
        kick_entry = await _an_fetch(guild, discord.AuditLogAction.kick, member.id)
        if kick_entry:
            executor = kick_entry.user
            if not _an_immune(guild, executor.id):
                retries = 3
                while retries > 0:
                    try:
                        await guild.ban(executor, reason="Member Kick | Unwhitelisted User", delete_message_days=0)
                        break
                    except discord.Forbidden: break
                    except discord.HTTPException as e:
                        if e.status == 429:
                            await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
                        else: break
                    except Exception: break
                await _an_log(guild, "Anti-Kick", executor, f"Kicked: {member}", "Executor banned")
                await asyncio.sleep(2)

    # ── Anti-Prune (antiprune.py) ─────────────────────────
    if an_on(guild.id):
        prune_entry = await _an_fetch(guild, discord.AuditLogAction.member_prune)
        if prune_entry:
            executor = prune_entry.user
            if not _an_immune(guild, executor.id):
                await _an_ban(guild, executor, "Member Prune | Unwhitelisted User")
                await _an_log(guild, "Anti-Prune", executor, f"Pruned: {member}", "Executor banned")

    # ── Leave message ─────────────────────────────────────
    if not LEAVE_ENABLED: return
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch: return
    e = _base("👋  Member Left", color=C_ERROR)
    e.description = (
        f"**{member.name}** has left the server.\n\n"
        f"📥 **Joined:** <t:{int(member.joined_at.timestamp())}:R>\n"
        f"👥 **Members now:** {guild.member_count}"
    )
    e.set_thumbnail(url=member.display_avatar.url)
    ft(e, f"{guild.name} • Goodbye", guild.icon.url if guild.icon else None)
    await ch.send(embed=e)

# ══════════════════════════════════════════════════════════
#  AFK
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="afk", description="Set your AFK status")
@app_commands.describe(reason="Why you're going AFK")
async def afk(interaction: discord.Interaction, reason: str = "AFK"):
    await interaction.response.defer(ephemeral=True)
    m = interaction.user
    afk_users[m.id] = {"reason": reason, "original_nick": m.nick}
    try:
        if not m.display_name.startswith("[AFK]"):
            await m.edit(nick=f"[AFK] {m.display_name}"[:32], reason=f"AFK: {reason}")
    except Exception: pass
    e = ok("AFK Set", f"You are now AFK.\n**Reason:** {reason}")
    ft(e, "Crimson Gen")
    await interaction.followup.send(embed=e, ephemeral=True)

# ══════════════════════════════════════════════════════════
#  MEME
# ══════════════════════════════════════════════════════════
async def fetch_meme(subreddit: str) -> dict:
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://meme-api.com/gimme/{subreddit}",
                         headers={"User-Agent": "CrimsonBot/3.0"},
                         timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: raise Exception(f"API returned {r.status}")
            d = await r.json()
            if d.get("nsfw") or not d.get("url"): raise Exception("NSFW/no image")
            return d

def build_meme_embed(d: dict, requester: discord.Member) -> discord.Embed:
    e = discord.Embed(title=d.get("title","")[:256], url=d.get("postLink","https://reddit.com"), color=0xFF4500)
    e.set_image(url=d.get("url",""))
    e.set_author(
        name=f"r/{d.get('subreddit','?')}",
        url=f"https://reddit.com/r/{d.get('subreddit','')}",
        icon_url="https://www.redditstatic.com/desktop2x/img/favicon/android-icon-192x192.png"
    )
    e.add_field(name="⬆️ Upvotes",  value=f"`{d.get('ups',0):,}`",            inline=True)
    e.add_field(name="💬 Comments", value=f"`{d.get('num_comments',0):,}`",    inline=True)
    e.add_field(name="👤 Author",   value=f"u/{d.get('author','?')}",          inline=True)
    ft(e, f"Requested by {requester.name}", requester.display_avatar.url)
    return e

class MemeView(discord.ui.View):
    def __init__(self, category: str = "random"):
        super().__init__(timeout=120)
        self.category = category
        self.add_item(discord.ui.Button(label="Open on Reddit", emoji="🔗",
                                        style=discord.ButtonStyle.link, url="https://reddit.com/r/memes"))

    @discord.ui.button(label="Another Meme", emoji="🔄", style=discord.ButtonStyle.primary, custom_id="meme_refresh")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        try:
            d = await fetch_meme(MEME_SUBREDDITS.get(self.category, "memes"))
            await interaction.edit_original_response(embed=build_meme_embed(d, interaction.user), view=self)
        except Exception:
            await interaction.followup.send(embed=err("Failed", "Couldn't grab a meme. Try again!"), ephemeral=True)

@bot.tree.command(name="meme", description="Get a random meme")
@app_commands.describe(category="Meme category to browse")
@app_commands.choices(category=[
    app_commands.Choice(name="🔥 Dank Memes",       value="dankmemes"),
    app_commands.Choice(name="😂 Funny",             value="funny"),
    app_commands.Choice(name="🤙 Me IRL",            value="me_irl"),
    app_commands.Choice(name="🧠 Surreal",           value="surrealmemes"),
    app_commands.Choice(name="💻 Programmer Humor",  value="ProgrammerHumor"),
    app_commands.Choice(name="🎲 Random",            value="random"),
])
async def meme(interaction: discord.Interaction, category: str = "random"):
    await interaction.response.defer()
    try:
        d = await fetch_meme(MEME_SUBREDDITS.get(category, "memes"))
        await interaction.followup.send(embed=build_meme_embed(d, interaction.user), view=MemeView(category))
    except Exception:
        await interaction.followup.send(embed=err("Failed", "Couldn't fetch a meme right now. Try again later!"), ephemeral=True)

# ══════════════════════════════════════════════════════════
#  TICKET SYSTEM
# ══════════════════════════════════════════════════════════
import io as _io

async def generate_transcript(channel: discord.TextChannel) -> discord.File:
    messages = []
    async for msg in channel.history(limit=2000, oldest_first=True):
        messages.append(msg)
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows = ""
    for msg in messages:
        ts      = msg.created_at.strftime("%H:%M")
        name    = discord.utils.escape_markdown(msg.author.display_name)
        bot_tag = " <span class='bot'>BOT</span>" if msg.author.bot else ""
        content = (msg.content or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\n","<br>")
        attaches = "".join(
            f'<br><img src="{a.url}" style="max-width:400px;max-height:300px;border-radius:8px;margin-top:6px;">'
            if any(a.filename.lower().endswith(x) for x in [".png",".jpg",".jpeg",".gif",".webp"])
            else f'<br><a href="{a.url}" target="_blank">📎 {a.filename}</a>'
            for a in msg.attachments
        )
        rows += f"""<div class="msg"><img class="avatar" src="{msg.author.display_avatar.url}" onerror="this.style.display='none'"><div class="msg-body"><span class="author">{name}{bot_tag}</span><span class="ts">{ts}</span><div class="content">{content}{attaches}</div></div></div>"""
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Transcript — {channel.name}</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{background:#1e1f22;color:#dcddde;font-family:'Segoe UI',sans-serif;font-size:14px}}.header{{background:#2b2d31;padding:20px 30px;border-bottom:2px solid #dc143c}}.header h1{{color:#dc143c;font-size:18px}}.header p{{color:#96989d;font-size:12px;margin-top:4px}}.messages{{padding:20px 30px}}.msg{{display:flex;gap:14px;padding:6px 0}}.msg:hover{{background:#2b2d31;border-radius:8px;padding:6px 10px}}.avatar{{width:38px;height:38px;border-radius:50%;flex-shrink:0;margin-top:2px}}.msg-body{{flex:1}}.author{{color:#fff;font-weight:600}}.bot{{background:#5865f2;color:#fff;font-size:10px;padding:1px 5px;border-radius:4px;margin-left:5px}}.ts{{color:#72767d;font-size:11px;margin-left:8px}}.content{{color:#dcddde;margin-top:2px;line-height:1.5;word-break:break-word}}.footer{{text-align:center;padding:20px;color:#72767d;font-size:11px;border-top:1px solid #2b2d31;margin-top:20px}}</style>
</head><body><div class="header"><h1>📋 Transcript — #{channel.name}</h1><p>Generated: {now_str} · {len(messages)} messages · Crimson Gen</p></div><div class="messages">{rows}</div><div class="footer">Crimson Gen Ticket System · {now_str}</div></body></html>"""
    return discord.File(_io.BytesIO(html.encode("utf-8")), filename=f"transcript-{channel.name}.html")
class TicketCategorySelect(discord.ui.Select):
    def __init__(self):
        super().__init__(
            placeholder="📂  Choose a category...",
            custom_id="ticket_cat_select",
            options=[
                discord.SelectOption(label="General Support", description="Questions and general help",     emoji="❓"),
                discord.SelectOption(label="Report User",     description="Report someone breaking rules",  emoji="🚨"),
                discord.SelectOption(label="Partnership",     description="Partnership inquiries",           emoji="🤝"),
                discord.SelectOption(label="Bug Report",      description="Report a bug or issue",           emoji="🐛"),
                discord.SelectOption(label="Other",           description="Anything else",                   emoji="📝"),
            ]
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        global ticket_counter
        ticket_counter += 1
        num        = str(ticket_counter).zfill(4)
        cat        = self.values[0]
        guild      = interaction.guild
        staff_role = discord.utils.get(guild.roles, name=STAFF_ROLE_NAME)
        cat_folder = discord.utils.get(guild.categories, name="📩 Tickets") or \
                     await guild.create_category("📩 Tickets")

        ch = await guild.create_text_channel(
            f"ticket-{num}",
            category=cat_folder,
            topic=f"Ticket #{num} | {cat} | {interaction.user.id}"
        )
        await ch.set_permissions(guild.default_role, read_messages=False)
        await ch.set_permissions(interaction.user,   read_messages=True, send_messages=True)
        if staff_role:
            await ch.set_permissions(staff_role, read_messages=True, send_messages=True)

        cat_emojis = {"General Support":"❓","Report User":"🚨","Partnership":"🤝","Bug Report":"🐛","Other":"📝"}
        cat_emoji  = cat_emojis.get(cat, "🎟️")
        ts         = int(datetime.utcnow().timestamp())

        # ── Main ticket embed ──────────────────────────────────
        e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
        e.set_author(
            name=f"{guild.name}  ·  Ticket #{num}",
            icon_url=guild.icon.url if guild.icon else None
        )
        e.description = (
            f"### {cat_emoji}  {cat}\n"
            f"> Hey {interaction.user.mention}, thanks for opening a ticket!\n"
            f"> A staff member will be with you shortly.\n"
            f"> Please describe your issue below.\n\u200b"
        )
        e.set_thumbnail(url=interaction.user.display_avatar.url)
        e.add_field(name="👤  User",      value=f"{interaction.user.mention}\n`{interaction.user.id}`", inline=True)
        e.add_field(name="📂  Category",  value=f"{cat_emoji}  {cat}",                                   inline=True)
        e.add_field(name="🟢  Status",    value="Open",                                                    inline=True)
        e.add_field(name="📅  Opened",    value=f"<t:{ts}:R>",                                            inline=True)
        e.add_field(name="🎟️  Ticket",    value=f"`#{num}`",                                              inline=True)
        e.add_field(name="\u200b",       value="\u200b",                                                inline=True)
        e.set_image(url=TICKET_BANNER)
        e.set_footer(text=f"Crimson Gen  ·  Ticket #{num}", icon_url=guild.me.display_avatar.url)
        await ch.send(content=interaction.user.mention, embed=e, view=TicketActionsView())

        # ── Staff info embed ───────────────────────────────────
        if staff_role:
            si = discord.Embed(color=0x2b2d31, timestamp=datetime.utcnow())
            si.set_author(
                name="New Ticket  ·  Staff Panel",
                icon_url=guild.me.display_avatar.url
            )
            si.set_thumbnail(url=interaction.user.display_avatar.url)
            si.add_field(name="👤  Creator",   value=f"{interaction.user.mention}\n`{interaction.user.id}`", inline=True)
            si.add_field(name="📂  Category",  value=f"{cat_emoji}  {cat}",                                   inline=True)
            si.add_field(name="🎟️  Ticket #",  value=f"`#{num}`",                                             inline=True)
            si.add_field(name="📅  Opened",    value=f"<t:{ts}:F>",                                           inline=True)
            si.add_field(name="🟢  Status",    value="Open",                                                   inline=True)
            si.add_field(name="\u200b",       value="\u200b",                                               inline=True)
            si.set_footer(text=f"Crimson Gen  ·  Ticket #{num}", icon_url=guild.me.display_avatar.url)
            await ch.send(content=f"👁️ {staff_role.mention}", embed=si, view=StaffTicketInfoView())

        ticket_creators[ch.id] = interaction.user.id
        await interaction.followup.send(
            embed=discord.Embed(color=C_SUCCESS, description=f"✅  Your ticket is ready: {ch.mention}"),
            ephemeral=True
        )


class TicketPanelView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None); self.add_item(TicketCategorySelect())


class StaffTicketInfoView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Add User",    emoji="➕", style=discord.ButtonStyle.green, custom_id="staff_add_user")
    async def add_user(self, i: discord.Interaction, b):
        staff_role = discord.utils.get(i.guild.roles, name=STAFF_ROLE_NAME)
        if not (staff_role and staff_role in i.user.roles) and not i.user.guild_permissions.administrator:
            return await i.response.send_message("❌ Staff only.", ephemeral=True)
        await i.response.send_modal(ManageUsersModal())

    @discord.ui.button(label="Remove User", emoji="➖", style=discord.ButtonStyle.red,   custom_id="staff_remove_user")
    async def remove_user(self, i: discord.Interaction, b):
        staff_role = discord.utils.get(i.guild.roles, name=STAFF_ROLE_NAME)
        if not (staff_role and staff_role in i.user.roles) and not i.user.guild_permissions.administrator:
            return await i.response.send_message("❌ Staff only.", ephemeral=True)
        await i.response.send_modal(RemoveUserModal())


class TicketActionsView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    def _is_staff(self, i): 
        sr = discord.utils.get(i.guild.roles, name=STAFF_ROLE_NAME)
        return (sr and sr in i.user.roles) or i.user.guild_permissions.administrator

    @discord.ui.button(label="Rename",  emoji="✏️",  style=discord.ButtonStyle.primary, custom_id="t_rename",  row=0)
    async def rename(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        await i.response.send_modal(RenameTicketModal())

    @discord.ui.button(label="Close",   emoji="🔒", style=discord.ButtonStyle.red,     custom_id="t_close",   row=0)
    async def close_ticket(self, i: discord.Interaction, b):
        is_creator = i.channel.overwrites_for(i.user).read_messages is True
        if not self._is_staff(i) and not is_creator:
            return await i.response.send_message("❌ Only staff or the ticket creator can close.", ephemeral=True)
        await i.response.send_modal(CloseReasonModal())

    @discord.ui.button(label="Claim",   emoji="⭐", style=discord.ButtonStyle.green,   custom_id="t_claim",   row=0)
    async def claim(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        e = discord.Embed(color=C_SUCCESS, description=f"⭐  {i.user.mention} has claimed this ticket.", timestamp=datetime.utcnow())
        e.set_footer(text="Crimson Gen  ·  Tickets", icon_url=i.guild.me.display_avatar.url)
        await i.response.send_message(embed=e)

    @discord.ui.button(label="Add User", emoji="➕", style=discord.ButtonStyle.gray,   custom_id="t_adduser", row=1)
    async def add_user(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        await i.response.send_modal(ManageUsersModal())

    @discord.ui.button(label="Delete",  emoji="🗑️", style=discord.ButtonStyle.red,     custom_id="t_delete",  row=1)
    async def delete(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        e = discord.Embed(color=C_ERROR, description="🗑️  Deleting ticket in **5 seconds**...", timestamp=datetime.utcnow())
        e.set_footer(text="Crimson Gen  ·  Tickets")
        await i.response.send_message(embed=e)
        await asyncio.sleep(5)
        await i.channel.delete()


class ClosedTicketView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    def _is_staff(self, i):
        sr = discord.utils.get(i.guild.roles, name=STAFF_ROLE_NAME)
        return (sr and sr in i.user.roles) or i.user.guild_permissions.administrator

    @discord.ui.button(label="Reopen",     emoji="🔓", style=discord.ButtonStyle.green,   custom_id="ticket_reopen_btn")
    async def reopen_ticket(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        await i.response.defer()
        for m in i.channel.members:
            await i.channel.set_permissions(m, send_messages=True, read_messages=True)
        e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
        e.description = f"🔓  Ticket reopened by {i.user.mention}."
        e.set_footer(text="Crimson Gen  ·  Tickets", icon_url=i.guild.me.display_avatar.url)
        await i.channel.send(embed=e, view=TicketActionsView())

    @discord.ui.button(label="Transcript", emoji="📋", style=discord.ButtonStyle.primary, custom_id="ticket_transcript_btn")
    async def save_transcript(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        await i.response.defer(ephemeral=True)
        try:
            tf = await generate_transcript(i.channel)
            e  = discord.Embed(color=C_INFO, description=f"📋  Transcript for `{i.channel.name}` generated.", timestamp=datetime.utcnow())
            e.set_footer(text="Crimson Gen  ·  Tickets", icon_url=i.guild.me.display_avatar.url)
            await i.followup.send(embed=e, file=tf, ephemeral=True)
            ch_id = ticket_transcript_channels.get(i.guild.id)
            if ch_id and (lch := i.guild.get_channel(ch_id)):
                tf2  = await generate_transcript(i.channel)
                le   = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
                le.set_author(name="Ticket Transcript", icon_url=i.guild.me.display_avatar.url)
                le.add_field(name="📋  Channel",   value=f"`{i.channel.name}`",   inline=True)
                le.add_field(name="💾  Saved by",  value=i.user.mention,           inline=True)
                le.add_field(name="📅  Time",      value=f"<t:{int(datetime.utcnow().timestamp())}:F>", inline=False)
                le.set_footer(text="Crimson Gen  ·  Tickets", icon_url=i.guild.me.display_avatar.url)
                await lch.send(embed=le, file=tf2)
        except Exception as ex:
            await i.followup.send(f"❌ Failed: `{ex}`", ephemeral=True)

    @discord.ui.button(label="Delete",     emoji="🗑️", style=discord.ButtonStyle.red,     custom_id="ticket_delete_btn")
    async def delete_ticket(self, i: discord.Interaction, b):
        if not self._is_staff(i): return await i.response.send_message("❌ Staff only.", ephemeral=True)
        e = discord.Embed(color=C_ERROR, description="🗑️  Deleting in 3 seconds...", timestamp=datetime.utcnow())
        e.set_footer(text="Crimson Gen  ·  Tickets")
        await i.response.send_message(embed=e)
        await asyncio.sleep(3)
        await i.channel.delete()


class CloseReasonModal(discord.ui.Modal, title="Close Ticket"):
    reason = discord.ui.TextInput(
        label="Reason for closing",
        style=discord.TextStyle.paragraph,
        placeholder="Why are you closing this ticket?",
        required=True,
        max_length=500
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        await interaction.channel.set_permissions(interaction.guild.default_role, send_messages=False, read_messages=False)
        for m in interaction.channel.members:
            if staff_role and staff_role not in m.roles and not m.guild_permissions.administrator:
                await interaction.channel.set_permissions(m, send_messages=False)

        ts = int(datetime.utcnow().timestamp())

        # Generate transcript before sending close embed
        transcript_file = None
        try:
            transcript_file = await generate_transcript(interaction.channel)
        except Exception:
            pass

        # ── Close embed ────────────────────────────────────────
        e = discord.Embed(color=0x2b2d31, timestamp=datetime.utcnow())
        e.set_author(
            name=f"Ticket Closed  ·  {interaction.channel.name}",
            icon_url=interaction.user.display_avatar.url
        )
        e.description = f"### 🔒  This ticket has been closed\n\u200b"
        e.add_field(name="👤  Closed By", value=interaction.user.mention,           inline=True)
        e.add_field(name="📅  Closed At", value=f"<t:{ts}:F>",                      inline=True)
        e.add_field(name="📝  Reason",    value=f"```{self.reason.value}```",        inline=False)
        e.set_footer(text="Crimson Gen  ·  Tickets", icon_url=interaction.guild.me.display_avatar.url)

        content = staff_role.mention if staff_role else None
        if transcript_file:
            await interaction.channel.send(content=content, embed=e, file=transcript_file, view=ClosedTicketView())
        else:
            await interaction.channel.send(content=content, embed=e, view=ClosedTicketView())

        # ── DM transcript to creator ───────────────────────────
        creator_id = ticket_creators.get(interaction.channel.id)
        if creator_id:
            try:
                creator = interaction.guild.get_member(creator_id)
                if creator:
                    tf_dm = await generate_transcript(interaction.channel)
                    dm_e  = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
                    dm_e.set_author(name=f"{interaction.guild.name}  ·  Ticket Closed", icon_url=interaction.guild.icon.url if interaction.guild.icon else None)
                    dm_e.description = (
                        f"### 📋  Your ticket has been closed\n"
                        f"> **Ticket:** `{interaction.channel.name}`\n"
                        f"> **Closed by:** {interaction.user.display_name}\n"
                        f"> **Reason:** {self.reason.value}\n\n"
                        f"Your transcript is attached — open it in any browser."
                    )
                    dm_e.set_footer(text="Crimson Gen  ·  Tickets")
                    await creator.send(embed=dm_e, file=tf_dm)
            except Exception:
                pass

        # ── Auto-log to transcript channel ────────────────────
        ch_id = ticket_transcript_channels.get(interaction.guild.id)
        if ch_id and (lch := interaction.guild.get_channel(ch_id)):
            try:
                tf_log = await generate_transcript(interaction.channel)
                le = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
                le.set_author(name="Ticket Transcript", icon_url=interaction.guild.me.display_avatar.url)
                le.add_field(name="📋  Channel",    value=f"`{interaction.channel.name}`",   inline=True)
                le.add_field(name="👤  Closed by",  value=interaction.user.mention,           inline=True)
                le.add_field(name="📝  Reason",     value=self.reason.value,                  inline=False)
                le.add_field(name="📅  Time",       value=f"<t:{ts}:F>",                      inline=False)
                le.set_footer(text="Crimson Gen  ·  Tickets", icon_url=interaction.guild.me.display_avatar.url)
                await lch.send(embed=le, file=tf_log)
            except Exception:
                pass


class RenameTicketModal(discord.ui.Modal, title="Rename Ticket"):
    name = discord.ui.TextInput(label="New name", placeholder="e.g. payment-issue", max_length=50)
    async def on_submit(self, i: discord.Interaction):
        await i.response.defer(ephemeral=True)
        clean = "".join(c for c in self.name.value.lower().replace(" ", "-") if c.isalnum() or c == "-")
        try:
            await i.channel.edit(name=f"ticket-{clean}")
            await i.followup.send(embed=discord.Embed(color=C_SUCCESS, description=f"✅  Renamed to `ticket-{clean}`"), ephemeral=True)
        except Exception as ex:
            await i.followup.send(f"❌ Failed: `{ex}`", ephemeral=True)


class ManageUsersModal(discord.ui.Modal, title="Add / Remove User"):
    user_input = discord.ui.TextInput(label="User ID or name (- prefix to remove)", max_length=100)
    async def on_submit(self, i: discord.Interaction):
        await i.response.defer(ephemeral=True)
        raw    = self.user_input.value.strip()
        remove = raw.startswith("-")
        raw    = raw.lstrip("-").strip().replace("@","").replace("<","").replace(">","").replace("!","")
        user   = None
        if raw.isdigit():
            try: user = await i.guild.fetch_member(int(raw))
            except: pass
        if not user: user = discord.utils.get(i.guild.members, name=raw)
        if not user: user = discord.utils.get(i.guild.members, display_name=raw)
        if not user:
            return await i.followup.send(f"❌ User `{raw}` not found.", ephemeral=True)
        if remove:
            await i.channel.set_permissions(user, overwrite=None)
            await i.followup.send(embed=discord.Embed(color=C_SUCCESS, description=f"✅  Removed {user.mention}"), ephemeral=True)
        else:
            await i.channel.set_permissions(user, read_messages=True, send_messages=True)
            await i.followup.send(embed=discord.Embed(color=C_SUCCESS, description=f"✅  Added {user.mention}"), ephemeral=True)
            await i.channel.send(embed=discord.Embed(color=C_INFO, description=f"👥  {user.mention} was added by {i.user.mention}."))


class RemoveUserModal(discord.ui.Modal, title="Remove User"):
    user_input = discord.ui.TextInput(label="User ID or name", max_length=100)
    async def on_submit(self, i: discord.Interaction):
        await i.response.defer(ephemeral=True)
        raw  = self.user_input.value.strip().replace("@","").replace("<","").replace(">","").replace("!","")
        user = None
        if raw.isdigit():
            try: user = await i.guild.fetch_member(int(raw))
            except: pass
        if not user: user = discord.utils.get(i.guild.members, name=raw)
        if not user: user = discord.utils.get(i.guild.members, display_name=raw)
        if not user: return await i.followup.send(f"❌ User not found.", ephemeral=True)
        await i.channel.set_permissions(user, overwrite=None)
        await i.followup.send(embed=discord.Embed(color=C_SUCCESS, description=f"✅  Removed {user.mention}"), ephemeral=True)


# ── Ticket Commands ──────────────────────────────────────
@bot.tree.command(name="panel", description="Send the support ticket panel")
@app_commands.checks.has_permissions(administrator=True)
async def panel(interaction: discord.Interaction):
    e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
    e.set_author(
        name=f"{interaction.guild.name}  ·  Support",
        icon_url=interaction.guild.icon.url if interaction.guild.icon else None
    )
    e.description = (
        "### 🎟️  Support Tickets\n"
        "> Need help? Select a category below and a **private ticket** will be created just for you.\n"
        "> Our staff team will assist you as soon as possible.\n\u200b"
    )
    e.add_field(name="⚡  Response",   value="Fast as possible",     inline=True)
    e.add_field(name="🔒  Private",    value="Just you & staff",      inline=True)
    e.add_field(name="📂  Categories", value="5 to choose from",      inline=True)
    e.set_image(url=TICKET_BANNER)
    e.set_footer(text=f"Crimson Gen  ·  {interaction.guild.name}", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.channel.send(embed=e, view=TicketPanelView())
    await interaction.response.send_message(
        embed=discord.Embed(color=C_SUCCESS, description="✅  Panel sent."),
        ephemeral=True
    )

@bot.tree.command(name="close", description="Close the current ticket")
async def close(interaction: discord.Interaction):
    if not interaction.channel.name.startswith("ticket-"):
        return await interaction.response.send_message("❌ Run this inside a ticket channel.", ephemeral=True)
    await interaction.response.send_modal(CloseReasonModal())

@bot.tree.command(name="deleteticket", description="Force-delete a ticket channel")
@app_commands.checks.has_permissions(manage_channels=True)
async def deleteticket(interaction: discord.Interaction):
    if not interaction.channel.name.startswith("ticket-"):
        return await interaction.response.send_message("❌ Run this inside a ticket channel.", ephemeral=True)
    e = discord.Embed(color=C_ERROR, description="🗑️  Force deleting in **3 seconds**...", timestamp=datetime.utcnow())
    await interaction.response.send_message(embed=e)
    await asyncio.sleep(3)
    try:
        await interaction.channel.delete(reason=f"Force deleted by {interaction.user}")
    except Exception as ex:
        pass

@bot.tree.command(name="ticket_transcriptchannel", description="Set the channel where ticket transcripts are saved")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Channel to save transcripts to (leave empty to clear)")
async def ticket_transcriptchannel(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if channel:
        ticket_transcript_channels[interaction.guild.id] = channel.id
        e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
        e.description = f"✅  Transcripts will be auto-saved to {channel.mention} when tickets close."
    else:
        ticket_transcript_channels.pop(interaction.guild.id, None)
        e = discord.Embed(color=C_WARNING, timestamp=datetime.utcnow())
        e.description = "✅  Transcript channel cleared. Staff can still save manually via the 📋 button."
    e.set_footer(text="Crimson Gen  ·  Tickets", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="ticket_transcript", description="Manually generate a transcript for this ticket")
@app_commands.checks.has_permissions(manage_channels=True)
async def ticket_transcript_cmd(interaction: discord.Interaction):
    if not interaction.channel.name.startswith("ticket-"):
        return await interaction.response.send_message("❌ Run this inside a ticket channel.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        tf = await generate_transcript(interaction.channel)
        e  = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
        e.set_author(name="Ticket Transcript", icon_url=interaction.guild.me.display_avatar.url)
        e.description = f"📋  Transcript for `{interaction.channel.name}` generated."
        e.set_footer(text="Crimson Gen  ·  Tickets", icon_url=interaction.guild.me.display_avatar.url)
        await interaction.followup.send(embed=e, file=tf, ephemeral=True)
        ch_id = ticket_transcript_channels.get(interaction.guild.id)
        if ch_id and (lch := interaction.guild.get_channel(ch_id)):
            tf2 = await generate_transcript(interaction.channel)
            le  = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
            le.set_author(name="Ticket Transcript", icon_url=interaction.guild.me.display_avatar.url)
            le.add_field(name="📋  Channel",    value=f"`{interaction.channel.name}`",   inline=True)
            le.add_field(name="💾  By",         value=interaction.user.mention,           inline=True)
            le.add_field(name="📅  Time",       value=f"<t:{int(datetime.utcnow().timestamp())}:F>", inline=False)
            le.set_footer(text="Crimson Gen  ·  Tickets", icon_url=interaction.guild.me.display_avatar.url)
            await lch.send(embed=le, file=tf2)
    except Exception as ex:
        await interaction.followup.send(f"❌ Failed: `{ex}`", ephemeral=True)

# ══════════════════════════════════════════════════════════
#  MODERATION
# ══════════════════════════════════════════════════════════
def mod_stat(guild_id): get_stats(guild_id)["mod_actions"] += 1

def hier_check(interaction: discord.Interaction, target: discord.Member) -> bool:
    return target.top_role < interaction.user.top_role or interaction.user.id == interaction.guild.owner_id

@bot.tree.command(name="ban", description="Ban a member from the server")
@app_commands.checks.has_permissions(ban_members=True)
@app_commands.describe(user="Member to ban", reason="Reason for the ban")
async def ban(interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
    await interaction.response.defer(ephemeral=True)
    if not hier_check(interaction, user):
        return await interaction.followup.send(embed=err("Hierarchy Error", "You can't ban someone with an equal or higher role."), ephemeral=True)
    mod_stat(interaction.guild.id)
    try: await user.send(embed=warn(f"You were banned from {interaction.guild.name}", f"**Reason:** {reason}"))
    except Exception: pass
    await user.ban(reason=reason)
    e = _base("🔨  Member Banned", color=C_ERROR)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="👤 User",   value=f"{user.mention}\n`{user.id}`", inline=True)
    e.add_field(name="📝 Reason", value=reason,                         inline=True)
    ft(e, f"Banned by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)

@bot.tree.command(name="kick", description="Kick a member from the server")
@app_commands.checks.has_permissions(kick_members=True)
@app_commands.describe(user="Member to kick", reason="Reason for the kick")
async def kick(interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
    await interaction.response.defer(ephemeral=True)
    if not hier_check(interaction, user):
        return await interaction.followup.send(embed=err("Hierarchy Error", "You can't kick someone with an equal or higher role."), ephemeral=True)
    mod_stat(interaction.guild.id)
    try: await user.send(embed=warn(f"You were kicked from {interaction.guild.name}", f"**Reason:** {reason}"))
    except Exception: pass
    await user.kick(reason=reason)
    e = _base("👢  Member Kicked", color=C_WARNING)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="👤 User",   value=f"{user.mention}\n`{user.id}`", inline=True)
    e.add_field(name="📝 Reason", value=reason,                         inline=True)
    ft(e, f"Kicked by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)

@bot.tree.command(name="timeout", description="Timeout a member")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.describe(user="Member to timeout", duration="e.g. 10m  2h  7d  (max 28d)", reason="Reason")
async def timeout_cmd(interaction: discord.Interaction, user: discord.Member, duration: str, reason: str = "No reason provided"):
    await interaction.response.defer(ephemeral=True)
    if user.id == interaction.user.id: return await interaction.followup.send(embed=err("Error", "You can't timeout yourself."), ephemeral=True)
    if user.bot:                        return await interaction.followup.send(embed=err("Error", "You can't timeout bots."), ephemeral=True)
    if not hier_check(interaction, user): return await interaction.followup.send(embed=err("Hierarchy Error", "You can't timeout someone with an equal or higher role."), ephemeral=True)
    try: secs, label = parse_duration(duration)
    except ValueError: return await interaction.followup.send(embed=err("Invalid Duration", "Use formats like `10m`, `2h`, `7d`. Max: `28d`."), ephemeral=True)
    if secs > 2419200: return await interaction.followup.send(embed=err("Too Long", "Maximum timeout is 28 days."), ephemeral=True)
    until = discord.utils.utcnow() + timedelta(seconds=secs)
    mod_stat(interaction.guild.id)
    await user.timeout(until, reason=reason)
    e = _base("⏰  Member Timed Out", color=C_WARNING)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="👤 User",     value=f"{user.mention}\n`{user.id}`",   inline=True)
    e.add_field(name="⏱ Duration", value=label,                             inline=True)
    e.add_field(name="🔓 Ends",     value=f"<t:{int(until.timestamp())}:R>", inline=True)
    e.add_field(name="📝 Reason",   value=reason,                           inline=False)
    ft(e, f"Timed out by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)
    try: await user.send(embed=warn(f"You were timed out in {interaction.guild.name}", f"**Duration:** {label}\n**Reason:** {reason}"))
    except Exception: pass

@bot.tree.command(name="untimeout", description="Remove a member's timeout")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.describe(user="Member to untimeout")
async def untimeout(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    await user.timeout(None)
    e = ok("Timeout Removed", f"{user.mention}'s timeout has been lifted.")
    ft(e, f"Removed by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)

@bot.tree.command(name="purge", description="Bulk delete messages (1–1000)")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.describe(
    amount="Number of messages to delete (1–1000)",
    user="Only delete messages from this user (optional)",
    contains="Only delete messages containing this text (optional)",
    bots_only="Only delete messages from bots"
)
async def purge(
    interaction: discord.Interaction,
    amount: int,
    user: discord.Member = None,
    contains: str = None,
    bots_only: bool = False
):
    await interaction.response.defer(ephemeral=True)

    if not 1 <= amount <= 1000:
        return await interaction.followup.send(
            embed=err("Invalid Amount", "Choose between **1** and **1000**."), ephemeral=True
        )

    def check(m: discord.Message) -> bool:
        if user     and m.author != user:                          return False
        if bots_only and not m.author.bot:                        return False
        if contains and contains.lower() not in m.content.lower(): return False
        return True

    try:
        # Fetch up to amount*2 messages to account for filtered ones, cap at 1000
        fetch_limit = min(amount * 2 if (user or contains or bots_only) else amount, 1000)
        now         = datetime.utcnow()
        messages    = []

        async for msg in interaction.channel.history(limit=fetch_limit, oldest_first=False):
            if check(msg):
                messages.append(msg)
            if len(messages) >= amount:
                break

        if not messages:
            return await interaction.followup.send(
                embed=warn("Nothing to Delete", "No matching messages found."), ephemeral=True
            )

        # Split into bulk-deletable (< 14 days) and old
        bulk = [m for m in messages if (now - m.created_at.replace(tzinfo=None)).days < 14]
        old  = [m for m in messages if (now - m.created_at.replace(tzinfo=None)).days >= 14]

        total_deleted = 0

        # Bulk delete in batches of 100 simultaneously — maximum speed
        for i in range(0, len(bulk), 100):
            batch = bulk[i:i+100]
            try:
                if len(batch) == 1:
                    await batch[0].delete()
                else:
                    await interaction.channel.delete_messages(batch)
                total_deleted += len(batch)
            except Exception:
                pass

        # Old messages deleted concurrently in groups of 5
        for i in range(0, len(old), 5):
            batch = old[i:i+5]
            results = await asyncio.gather(*[m.delete() for m in batch], return_exceptions=True)
            total_deleted += sum(1 for r in results if not isinstance(r, Exception))

    except discord.Forbidden:
        return await interaction.followup.send(
            embed=err("Missing Permissions", "I don't have permission to delete messages here."), ephemeral=True
        )
    except Exception as ex:
        return await interaction.followup.send(
            embed=err("Error", f"Something went wrong: `{ex}`"), ephemeral=True
        )

    filters = []
    if user:      filters.append(f"from {user.mention}")
    if bots_only: filters.append("bots only")
    if contains:  filters.append(f"containing `{contains}`")
    filter_str = "  ·  ".join(filters) if filters else "all messages"

    e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = (
        f"## 🗑️  Purge Complete\n"
        f"> Deleted **{total_deleted}** message(s)\n"
        f"> Filter: {filter_str}"
    )
    ft(e, f"Purged by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e, ephemeral=True)

@bot.tree.command(name="slowmode", description="Set slowmode on this channel")
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.describe(seconds="Seconds (0 to disable, max 21600)")
async def slowmode(interaction: discord.Interaction, seconds: int):
    await interaction.response.defer(ephemeral=True)
    if not 0 <= seconds <= 21600: return await interaction.followup.send(embed=err("Out of Range", "Must be 0–21600."), ephemeral=True)
    await interaction.channel.edit(slowmode_delay=seconds)
    msg = "Slowmode **disabled**." if seconds == 0 else f"Slowmode set to **{seconds}s**."
    e = ok("Slowmode Updated", msg)
    ft(e, f"Updated by {interaction.user.name}")
    await interaction.followup.send(embed=e)

@bot.tree.command(name="lock", description="Lock the current channel")
@app_commands.checks.has_permissions(manage_channels=True)
async def lock(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await interaction.channel.set_permissions(interaction.guild.default_role, send_messages=False)
    e = warn("🔒  Channel Locked", f"{interaction.channel.mention} has been locked by {interaction.user.mention}.")
    ft(e, f"Locked by {interaction.user.name}")
    await interaction.channel.send(embed=e)
    await interaction.followup.send(embed=ok("Locked"), ephemeral=True)

@bot.tree.command(name="unlock", description="Unlock the current channel")
@app_commands.checks.has_permissions(manage_channels=True)
async def unlock(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await interaction.channel.set_permissions(interaction.guild.default_role, send_messages=None)
    e = ok("🔓  Channel Unlocked", f"{interaction.channel.mention} has been unlocked by {interaction.user.mention}.")
    ft(e, f"Unlocked by {interaction.user.name}")
    await interaction.channel.send(embed=e)
    await interaction.followup.send(embed=ok("Unlocked"), ephemeral=True)

# ══════════════════════════════════════════════════════════
#  WARNING SYSTEM
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="warn", description="Issue a warning to a member")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.describe(user="Member to warn", reason="Reason for the warning")
async def warn_cmd(interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
    await interaction.response.defer(ephemeral=True)
    if user.id == interaction.user.id: return await interaction.followup.send(embed=err("Error", "You can't warn yourself."), ephemeral=True)
    if user.bot:                        return await interaction.followup.send(embed=err("Error", "You can't warn bots."), ephemeral=True)
    if not hier_check(interaction, user): return await interaction.followup.send(embed=err("Hierarchy Error", "You can't warn someone with an equal or higher role."), ephemeral=True)
    mod_stat(interaction.guild.id)
    warnings_db.setdefault(user.id, []).append({
        "reason": reason, "moderator": interaction.user.id,
        "moderator_name": interaction.user.name,
        "timestamp": datetime.utcnow(), "guild_id": interaction.guild.id
    })
    count = len(warnings_db[user.id])
    e = _base("⚠️  Member Warned", color=C_WARNING)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="👤 User",           value=f"{user.mention}\n`{user.id}`", inline=True)
    e.add_field(name="⚠️ Total Warnings", value=f"**{count}**",                 inline=True)
    e.add_field(name="📝 Reason",         value=reason,                         inline=False)
    ft(e, f"Warned by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)
    try:
        dm = warn(f"You were warned in {interaction.guild.name}",
                  f"**Reason:** {reason}\n**Total Warnings:** {count}\n\nPlease follow the server rules.")
        await user.send(embed=dm)
    except Exception: pass

@bot.tree.command(name="warnings", description="View all warnings for a user")
@app_commands.checks.has_permissions(moderate_members=True)
@app_commands.describe(user="User to check")
async def warnings_cmd(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    records = warnings_db.get(user.id, [])
    if not records: return await interaction.followup.send(embed=ok("No Warnings", f"{user.mention} has a clean record."), ephemeral=True)
    e = _base(f"⚠️  Warnings — {user.name}", f"**{len(records)}** total warning(s)", C_WARNING)
    e.set_thumbnail(url=user.display_avatar.url)
    for i, w in enumerate(records[-10:], start=max(1, len(records)-9)):
        e.add_field(
            name=f"Warning #{i}",
            value=f"**Reason:** {w['reason']}\n**By:** {w.get('moderator_name','?')}\n**When:** <t:{int(w['timestamp'].timestamp())}:R>",
            inline=False
        )
    if len(records) > 10: ft(e, f"Showing last 10 of {len(records)} warnings")
    await interaction.followup.send(embed=e, ephemeral=True)

@bot.tree.command(name="clearwarnings", description="Clear all warnings for a user")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User to clear warnings for")
async def clearwarnings(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    count = len(warnings_db.get(user.id, []))
    if not count: return await interaction.followup.send(embed=info("No Warnings", f"{user.mention} already has no warnings."), ephemeral=True)
    warnings_db[user.id] = []
    e = ok("Warnings Cleared", f"Cleared **{count}** warning(s) for {user.mention}.")
    ft(e, f"Cleared by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)

@bot.tree.command(name="delwarn", description="Remove a specific warning from a user")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User", warning_number="Which warning to delete (use /warnings to find the number)")
async def delwarn(interaction: discord.Interaction, user: discord.Member, warning_number: int):
    await interaction.response.defer(ephemeral=True)
    records = warnings_db.get(user.id, [])
    if not records: return await interaction.followup.send(embed=err("No Warnings", f"{user.mention} has no warnings."), ephemeral=True)
    if not 1 <= warning_number <= len(records): return await interaction.followup.send(embed=err("Invalid", f"Choose between 1 and {len(records)}."), ephemeral=True)
    deleted = records.pop(warning_number - 1)
    e = ok("Warning Deleted", f"Deleted warning **#{warning_number}** from {user.mention}.")
    e.add_field(name="Deleted Reason", value=deleted["reason"], inline=False)
    e.add_field(name="Remaining",      value=str(len(records)),  inline=True)
    ft(e, f"Deleted by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)

# ══════════════════════════════════════════════════════════
#  UTILITY
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="ping", description="Check the bot's latency")
async def ping(interaction: discord.Interaction):
    ms  = round(bot.latency * 1000)
    dot = "🟢" if ms < 100 else "🟡" if ms < 200 else "🔴"
    col = C_SUCCESS if ms < 100 else C_WARNING if ms < 200 else C_ERROR
    quality = "Excellent" if ms < 100 else "Good" if ms < 150 else "Moderate" if ms < 200 else "Poor"
    e = _base("", color=col)
    e.description = f"## 🏓 Pong!\n{dot} **{ms}ms** — {quality}"
    e.add_field(name="WebSocket",  value=f"`{ms}ms`",           inline=True)
    e.add_field(name="Status",     value=dot + " Online",        inline=True)
    ft(e, "Crimson Gen", bot.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="botinfo", description="View bot information and statistics")
async def botinfo(interaction: discord.Interaction):
    await interaction.response.defer()
    delta = datetime.utcnow() - bot_start_time if bot_start_time else None
    if delta:
        d, r = divmod(int(delta.total_seconds()), 86400)
        h, r = divmod(r, 3600); m, s = divmod(r, 60)
        uptime = f"{d}d {h}h {m}m {s}s"
    else:
        uptime = "Unknown"
    try:
        import psutil
        mem = f"{psutil.Process(os.getpid()).memory_info().rss/1024/1024:.1f} MB"
    except Exception:
        mem = "N/A"
    total_warns = sum(len(v) for v in warnings_db.values())
    ms = round(bot.latency * 1000)
    e = _base("", color=C_CRIMSON)
    e.description = f"## {bot.user.name}\n> Multipurpose Discord bot built for **Crimson Gen**"
    e.set_thumbnail(url=bot.user.display_avatar.url)
    e.add_field(name="⏰  Uptime",      value=f"```{uptime}```",                                              inline=True)
    e.add_field(name="📡  Latency",     value=f"```{ms}ms```",                                                inline=True)
    e.add_field(name="💾  Memory",      value=f"```{mem}```",                                                 inline=True)
    e.add_field(name="🏠  Servers",     value=f"```{len(bot.guilds)}```",                                     inline=True)
    e.add_field(name="👥  Users",       value=f"```{sum(g.member_count for g in bot.guilds):,}```",           inline=True)
    e.add_field(name="📝  Commands",    value=f"```{len(bot.tree.get_commands())}```",                        inline=True)
    e.add_field(name="🎟️  Tickets",     value=f"```{ticket_counter}```",                                     inline=True)
    e.add_field(name="💤  AFK Users",   value=f"```{len(afk_users)}```",                                     inline=True)
    e.add_field(name="⚠️  Warnings",    value=f"```{total_warns}```",                                        inline=True)
    e.add_field(name="🐍  Python",      value=f"```{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}```", inline=True)
    e.add_field(name="📦  discord.py",  value=f"```{discord.__version__}```",                                inline=True)
    e.add_field(name="🆔  Bot ID",      value=f"```{bot.user.id}```",                                        inline=True)
    ft(e, f"Requested by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.followup.send(embed=e)

@bot.tree.command(name="serverinfo", description="View detailed server information")
async def serverinfo(interaction: discord.Interaction):
    g      = interaction.guild
    humans = sum(1 for m in g.members if not m.bot)
    bots   = sum(1 for m in g.members if m.bot)
    stats  = get_stats(g.id)
    age    = (datetime.utcnow() - g.created_at.replace(tzinfo=None)).days
    feat_map = {
        "COMMUNITY": "🌐 Community", "VERIFIED": "✅ Verified", "PARTNERED": "🤝 Partnered",
        "DISCOVERABLE": "🔍 Discoverable", "VANITY_URL": "🔗 Vanity URL",
        "ANIMATED_ICON": "🎞️ Animated Icon", "BANNER": "🖼️ Banner", "NEWS": "📰 News Channels"
    }
    features   = [feat_map[f] for f in g.features if f in feat_map] or ["None"]
    tier_cols  = {0: 0x99AAB5, 1: C_BOOST, 2: C_BOOST, 3: 0xFFD700}
    boost_bars = "▰" * g.premium_subscription_count + "▱" * max(0, 14 - g.premium_subscription_count)
    e = discord.Embed(color=tier_cols.get(g.premium_tier, C_CRIMSON), timestamp=datetime.utcnow())
    e.description = f"## 🏠  {g.name}\n{g.description or '*No description set.*'}"
    if g.icon:   e.set_thumbnail(url=g.icon.url)
    if g.banner: e.set_image(url=g.banner.url)
    e.add_field(name="👑  Owner",        value=g.owner.mention if g.owner else "?",                   inline=True)
    e.add_field(name="📅  Created",      value=f"<t:{int(g.created_at.timestamp())}:R>",              inline=True)
    e.add_field(name="🆔  Server ID",    value=f"`{g.id}`",                                           inline=True)
    e.add_field(name="👥  Members",      value=f"**{g.member_count:,}** total\n`{humans:,}` humans  `{bots}` bots", inline=True)
    e.add_field(name="💬  Channels",     value=f"**{len(g.text_channels)}** text\n**{len(g.voice_channels)}** voice", inline=True)
    e.add_field(name="🎭  Roles",        value=f"**{len(g.roles)}** roles",                           inline=True)
    e.add_field(name="💜  Boost Level",  value=f"Level **{g.premium_tier}**\n`{boost_bars}`",         inline=True)
    e.add_field(name="🚀  Boosts",       value=f"**{g.premium_subscription_count}** boosts",          inline=True)
    e.add_field(name="🗓️  Age",          value=f"**{age}** days old",                                 inline=True)
    e.add_field(name="✨  Features",     value="\n".join(features),                                    inline=False)
    e.add_field(name="📊  Activity",
                value=(f"Joins **{stats['joins']}**  ·  Leaves **{stats['leaves']}**  ·  "
                       f"Messages **{stats['messages']:,}**  ·  Mod Actions **{stats['mod_actions']}**"),
                inline=False)
    ft(e, "Crimson Gen", bot.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="userinfo", description="View detailed information about a user")
@app_commands.describe(user="User to inspect (defaults to yourself)")
async def userinfo(interaction: discord.Interaction, user: discord.Member = None):
    user  = user or interaction.user
    color = user.color if user.color != discord.Color.default() else discord.Color.from_rgb(220, 20, 60)
    warns = len(warnings_db.get(user.id, []))
    e = discord.Embed(color=color, timestamp=datetime.utcnow())
    e.description = f"## {user.mention}\n`{user.name}`"
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="🆔  User ID",      value=f"`{user.id}`",                                        inline=True)
    e.add_field(name="🤖  Bot",          value="Yes" if user.bot else "No",                            inline=True)
    e.add_field(name="⚠️  Warnings",     value=f"**{warns}**",                                        inline=True)
    e.add_field(name="📅  Account Created", value=f"<t:{int(user.created_at.timestamp())}:R>",        inline=True)
    e.add_field(name="📥  Joined Server",   value=f"<t:{int(user.joined_at.timestamp())}:R>",         inline=True)
    e.add_field(name="🎨  Top Role",     value=user.top_role.mention,                                  inline=True)
    badges = []
    flags  = user.public_flags
    if flags.staff:                  badges.append("👨‍💼 Discord Staff")
    if flags.partner:                badges.append("🤝 Partner")
    if flags.hypesquad:              badges.append("🏠 HypeSquad")
    if flags.bug_hunter:             badges.append("🐛 Bug Hunter")
    if flags.early_supporter:        badges.append("⭐ Early Supporter")
    if flags.verified_bot_developer: badges.append("🛠️ Bot Developer")
    if badges:
        e.add_field(name="🏅  Badges", value="  ".join(badges), inline=False)
    roles = [r.mention for r in reversed(user.roles[1:])]
    if roles:
        val = " ".join(roles[:20]) + (f"\n*+{len(roles)-20} more*" if len(roles) > 20 else "")
        e.add_field(name=f"🎭  Roles ({len(roles)})", value=val, inline=False)
    ft(e, f"Requested by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="avatar", description="View a user's avatar in full size")
@app_commands.describe(user="User whose avatar to view")
async def avatar(interaction: discord.Interaction, user: discord.Member = None):
    user = user or interaction.user
    e    = _base("", color=user.color if user.color != discord.Color.default() else C_CRIMSON)
    e.description = f"## 🖼️  {user.display_name}'s Avatar"
    e.set_image(url=user.display_avatar.url)
    e.add_field(
        name="📥  Download",
        value=(f"[`PNG`]({user.display_avatar.with_format('png').url})  ·  "
               f"[`JPEG`]({user.display_avatar.with_format('jpeg').url})  ·  "
               f"[`WEBP`]({user.display_avatar.with_format('webp').url})"),
        inline=False
    )
    ft(e, f"Requested by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="announce", description="Send a formatted announcement to a channel")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    channel="Channel to post the announcement in",
    title="Announcement title",
    message="Announcement body (supports markdown)",
    ping="Role to ping with the announcement (optional)"
)
async def announce(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    title: str,
    message: str,
    ping: discord.Role = None
):
    ts = int(datetime.utcnow().timestamp())

    e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
    e.set_author(
        name=interaction.guild.name,
        icon_url=interaction.guild.icon.url if interaction.guild.icon else bot.user.display_avatar.url
    )
    e.title       = f"📢  {title}"
    e.description = f"{message}"
    e.add_field(name="\u200b", value=f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", inline=False)
    e.add_field(name="🕐  Posted",   value=f"<t:{ts}:F>",          inline=True)
    e.add_field(name="✍️  By",       value=interaction.user.mention, inline=True)
    if ping:
        e.add_field(name="🔔  Ping", value=ping.mention,             inline=True)
    e.set_footer(
        text=f"{interaction.guild.name}",
        icon_url=interaction.guild.icon.url if interaction.guild.icon else bot.user.display_avatar.url
    )

    content = ping.mention if ping else None
    await channel.send(content=content, embed=e)
    await interaction.response.send_message(
        embed=discord.Embed(color=C_SUCCESS, description=f"✅  Posted in {channel.mention}"),
        ephemeral=True
    )

@bot.tree.command(name="invite", description="Get the bot invite link")
async def invite(interaction: discord.Interaction):
    e = _base("", color=C_CRIMSON)
    e.description = (
        f"## 🤖  Crimson Gen\n"
        f"[**Click here to invite me**](https://discord.com/oauth2/authorize?client_id=1295074164230717573&permissions=8&integration_type=0&scope=bot) "
        f"to your server!\n\u200b"
    )
    e.set_thumbnail(url=bot.user.display_avatar.url)
    e.add_field(
        name="🎟️  Ticketing",
        value="Advanced ticket system with categories, staff controls, and auto-close",
        inline=False
    )
    e.add_field(
        name="🛡️  Security",
        value="Antinuke protection + full AutoMod with scam detection and auto-timeout",
        inline=False
    )
    e.add_field(
        name="⚙️  Utilities",
        value="Warnings, AFK, Memes, Reaction Roles, Emoji Steal, Vouch System, and more",
        inline=False
    )
    ft(e, "Crimson Gen", bot.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="fixcommands", description="Re-sync all slash commands (admin)")
@app_commands.checks.has_permissions(administrator=True)
async def fixcommands(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    # Sync globally and to this guild — do NOT clear first as it removes commands
    synced       = await bot.tree.sync()
    guild_synced = await bot.tree.sync(guild=interaction.guild)
    e = ok("Commands Resynced", f"Synced **{len(synced)}** global + **{len(guild_synced)}** guild commands.")
    e.set_footer(text="Guild commands update instantly. Global may take up to 1 hour.")
    await interaction.followup.send(embed=e, ephemeral=True)

# ══════════════════════════════════════════════════════════
#  WELCOME SYSTEM COMMANDS
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="setwelcome", description="Set the welcome/leave channel")
@app_commands.checks.has_permissions(administrator=True)
async def setwelcome(interaction: discord.Interaction, channel: discord.TextChannel):
    global WELCOME_CHANNEL_ID; WELCOME_CHANNEL_ID = channel.id
    await interaction.response.send_message(embed=ok("Welcome Channel Set", f"Channel set to {channel.mention}."), ephemeral=True)

@bot.tree.command(name="togglewelcome", description="Toggle welcome messages on/off")
@app_commands.checks.has_permissions(administrator=True)
async def togglewelcome(interaction: discord.Interaction):
    global WELCOME_ENABLED; WELCOME_ENABLED = not WELCOME_ENABLED
    await interaction.response.send_message(embed=ok("Toggled", f"Welcome messages: **{'✅ Enabled' if WELCOME_ENABLED else '❌ Disabled'}**"), ephemeral=True)

@bot.tree.command(name="toggleleave", description="Toggle leave messages on/off")
@app_commands.checks.has_permissions(administrator=True)
async def toggleleave(interaction: discord.Interaction):
    global LEAVE_ENABLED; LEAVE_ENABLED = not LEAVE_ENABLED
    await interaction.response.send_message(embed=ok("Toggled", f"Leave messages: **{'✅ Enabled' if LEAVE_ENABLED else '❌ Disabled'}**"), ephemeral=True)

@bot.tree.command(name="welcomestatus", description="View welcome system status")
@app_commands.checks.has_permissions(administrator=True)
async def welcomestatus(interaction: discord.Interaction):
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    e = info("Welcome System Status")
    e.add_field(name="Welcome Messages", value="✅ Enabled" if WELCOME_ENABLED else "❌ Disabled", inline=True)
    e.add_field(name="Leave Messages",   value="✅ Enabled" if LEAVE_ENABLED else "❌ Disabled",   inline=True)
    e.add_field(name="Channel",          value=ch.mention if ch else "Not set",                    inline=False)
    ft(e, "Crimson Gen")
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="testwelcome", description="Preview the welcome message")
@app_commands.checks.has_permissions(administrator=True)
async def testwelcome(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch: return await interaction.followup.send(embed=err("No Channel", "Use `/setwelcome` first."), ephemeral=True)
    m = interaction.user
    e = _base("🎉  Welcome to the Server!", color=C_SUCCESS)
    e.description = (
        f"Hey {m.mention}, welcome to **{m.guild.name}**!\n\n"
        f"📅 **Account Created:** <t:{int(m.created_at.timestamp())}:R>\n"
        f"👥 **You are member #{m.guild.member_count}**\n\n"
        f"📚 **Get Started**\n› Read the rules  ›  Grab your roles  ›  Say hello!"
    )
    e.set_thumbnail(url=m.display_avatar.url)
    e.set_image(url=CRIMSON_GIF)
    ft(e, f"{m.guild.name} • Welcome (TEST)", m.guild.icon.url if m.guild.icon else None)
    await ch.send(content=random.choice(WELCOME_MESSAGES).format(mention=m.mention, name=m.name), embed=e)
    await interaction.followup.send(embed=ok("Sent!", f"Test welcome posted in {ch.mention}."), ephemeral=True)

@bot.tree.command(name="testleave", description="Preview the leave message")
@app_commands.checks.has_permissions(administrator=True)
async def testleave(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch: return await interaction.followup.send(embed=err("No Channel", "Use `/setwelcome` first."), ephemeral=True)
    m = interaction.user
    e = _base("👋  Member Left", color=C_ERROR)
    e.description = f"**{m.name}** has left the server.\n\n📥 **Joined:** <t:{int(m.joined_at.timestamp())}:R>\n👥 **Members:** {m.guild.member_count}"
    e.set_thumbnail(url=m.display_avatar.url)
    ft(e, f"{m.guild.name} • Goodbye (TEST)", m.guild.icon.url if m.guild.icon else None)
    await ch.send(content=random.choice(LEAVE_MESSAGES).format(name=m.name), embed=e)
    await interaction.followup.send(embed=ok("Sent!", f"Test leave posted in {ch.mention}."), ephemeral=True)

# ══════════════════════════════════════════════════════════
#  REACTION ROLES
# ══════════════════════════════════════════════════════════
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id or not reaction_role_message_id: return
    if payload.message_id != reaction_role_message_id: return
    guild  = bot.get_guild(payload.guild_id)
    member = guild.get_member(payload.user_id) if guild else None
    if not member: return
    role_id = EMOJI_ROLE_MAP.get(payload.emoji.name)
    if role_id:
        role = guild.get_role(role_id)
        if role and role not in member.roles:
            await member.add_roles(role, reason="Reaction role")

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if not reaction_role_message_id or payload.message_id != reaction_role_message_id: return
    guild  = bot.get_guild(payload.guild_id)
    member = guild.get_member(payload.user_id) if guild else None
    if not member: return
    role_id = EMOJI_ROLE_MAP.get(payload.emoji.name)
    if role_id:
        role = guild.get_role(role_id)
        if role and role in member.roles:
            await member.remove_roles(role, reason="Reaction role removed")

@bot.tree.command(name="reactionroles", description="Send the reaction roles panel")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(emoji1="First emoji", emoji2="Second emoji", emoji3="Third emoji", emoji4="Fourth emoji")
async def reactionroles(interaction: discord.Interaction, emoji1: str, emoji2: str, emoji3: str, emoji4: str):
    global reaction_role_message_id
    e = _base("🎭  Get Your Notification Roles", color=C_CRIMSON)
    e.description = (
        f"{emoji1}  ›  DROP PING\n"
        f"{emoji2}  ›  ANNOUNCEMENT PING\n"
        f"{emoji3}  ›  GIVEAWAY PING\n"
        f"{emoji4}  ›  GEN BOT ANNOUNCEMENT PING\n\n"
        "React to receive a role  •  Unreact to remove it"
    )
    ft(e, "Crimson Gen • Reaction Roles")
    msg = await interaction.channel.send(embed=e)
    for emoji_str in [emoji1, emoji2, emoji3, emoji4]:
        match = re.match(r'<(a?):(\w+):(\d+)>', emoji_str)
        if match:
            em = bot.get_emoji(int(match.group(3)))
            if em: await msg.add_reaction(em)
        else:
            try: await msg.add_reaction(emoji_str)
            except Exception: pass
    reaction_role_message_id = msg.id
    await interaction.response.send_message(embed=ok("Panel Sent!", f"Message ID: `{msg.id}`"), ephemeral=True)

# ══════════════════════════════════════════════════════════
#  EMOJI TOOLS
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="steal", description="Steal a custom emoji from another server")
@app_commands.checks.has_permissions(manage_emojis=True)
@app_commands.describe(emoji="The custom emoji to steal", name="Custom name (optional)")
async def steal(interaction: discord.Interaction, emoji: str, name: str = None):
    await interaction.response.defer(ephemeral=True)
    match = re.match(r'<(a?):(\w+):(\d+)>', emoji)
    if not match: return await interaction.followup.send(embed=err("Invalid Emoji", "Paste a custom emoji (not a standard emoji)."), ephemeral=True)
    animated  = match.group(1) == "a"
    ename     = name or match.group(2)
    eid       = match.group(3)
    ext       = "gif" if animated else "png"
    url       = f"https://cdn.discordapp.com/emojis/{eid}.{ext}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url) as r:
                if r.status != 200: return await interaction.followup.send(embed=err("Download Failed", f"HTTP {r.status}"), ephemeral=True)
                data = await r.read()
        new = await interaction.guild.create_custom_emoji(name=ename, image=data, reason=f"Stolen by {interaction.user}")
        e = ok("Emoji Stolen!", f"Added {new} to the server.")
        e.set_thumbnail(url=new.url)
        e.add_field(name="Name",     value=f"`:{new.name}:`",              inline=True)
        e.add_field(name="Animated", value="Yes" if animated else "No",    inline=True)
        e.add_field(name="ID",       value=f"`{new.id}`",                  inline=True)
        ft(e, f"Stolen by {interaction.user.name}", interaction.user.display_avatar.url)
        await interaction.followup.send(embed=e)
    except discord.Forbidden:
        await interaction.followup.send(embed=err("No Permission", "I don't have permission to manage emojis."), ephemeral=True)
    except discord.HTTPException as ex:
        msg = "Server has hit the emoji limit." if ex.code == 30008 else str(ex)
        await interaction.followup.send(embed=err("Failed", msg), ephemeral=True)

@bot.tree.command(name="addemoji", description="Add an emoji from a direct image URL")
@app_commands.checks.has_permissions(manage_emojis=True)
@app_commands.describe(name="Emoji name", url="Direct image URL (png/jpg/gif)")
async def addemoji(interaction: discord.Interaction, name: str, url: str):
    await interaction.response.defer(ephemeral=True)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url) as r:
                if r.status != 200: return await interaction.followup.send(embed=err("Download Failed", f"HTTP {r.status}"), ephemeral=True)
                data = await r.read()
        new = await interaction.guild.create_custom_emoji(name=name, image=data, reason=f"Added by {interaction.user}")
        e = ok("Emoji Added!", f"Added {new} to the server.")
        e.set_thumbnail(url=new.url)
        e.add_field(name="Name", value=f"`:{new.name}:`", inline=True)
        ft(e, f"Added by {interaction.user.name}", interaction.user.display_avatar.url)
        await interaction.followup.send(embed=e)
    except discord.Forbidden:
        await interaction.followup.send(embed=err("No Permission", "I don't have permission to manage emojis."), ephemeral=True)
    except discord.HTTPException as ex:
        await interaction.followup.send(embed=err("Failed", str(ex)), ephemeral=True)

# ══════════════════════════════════════════════════════════
#  VOUCH SYSTEM
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="vouch", description="Vouch for a user (screenshot proof required)")
@app_commands.describe(user="User to vouch for", attachment="Screenshot / proof image (required)")
async def vouch(interaction: discord.Interaction, user: discord.Member, attachment: discord.Attachment):
    if user.id == interaction.user.id:
        return await interaction.response.send_message(embed=err("Error", "You can't vouch for yourself."), ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    gid, uid = interaction.guild.id, user.id
    vouch_db.setdefault(gid, {}).setdefault(uid, []).append({
        "by": interaction.user.id, "by_name": interaction.user.display_name,
        "timestamp": datetime.utcnow(), "proof_url": attachment.proxy_url
    })
    total = len(vouch_db[gid][uid])
    e = _base("✅  Vouch Recorded!", color=C_SUCCESS)
    e.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="✅ Vouched For",   value=user.mention,             inline=True)
    e.add_field(name="👤 Vouched By",    value=interaction.user.mention, inline=True)
    e.add_field(name="📊 Total Vouches", value=f"**{total}**",           inline=True)
    e.set_image(url=attachment.proxy_url)
    ft(e, f"User ID: {user.id}")
    ch_id = vouch_channels.get(gid)
    ch = bot.get_channel(ch_id) if ch_id else None
    if ch:
        await ch.send(embed=e)
        await interaction.followup.send(embed=ok("Vouch Recorded!", f"Sent to {ch.mention}."), ephemeral=True)
    else:
        await interaction.channel.send(embed=e)
        await interaction.followup.send(embed=ok("Vouch Recorded!"), ephemeral=True)

@bot.tree.command(name="vouches", description="View all vouches for a user")
@app_commands.describe(user="User to check (defaults to yourself)")
async def vouches(interaction: discord.Interaction, user: discord.Member = None):
    user = user or interaction.user
    records = get_vouches(interaction.guild.id, user.id)
    e = _base(f"✅  Vouches — {user.display_name}", color=C_SUCCESS)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="📊 Total Vouches", value=f"**{len(records)}**", inline=False)
    if records:
        recent = "\n".join(f"› <@{r['by']}> — <t:{int(r['timestamp'].timestamp())}:R>" for r in reversed(records[-5:]))
        e.add_field(name="🕒 Recent Vouches", value=recent, inline=False)
    ft(e, f"User ID: {user.id}")
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="vouchproof", description="View the proof image for a specific vouch")
@app_commands.describe(user="User to check", vouch_number="Which vouch to view (default: latest)")
async def vouchproof(interaction: discord.Interaction, user: discord.Member, vouch_number: int = 0):
    records = get_vouches(interaction.guild.id, user.id)
    if not records: return await interaction.response.send_message(embed=err("No Vouches", f"{user.mention} has no vouches."), ephemeral=True)
    idx = len(records) - 1 if vouch_number == 0 else vouch_number - 1
    if not 0 <= idx < len(records): return await interaction.response.send_message(embed=err("Invalid", f"{user.mention} has **{len(records)}** vouch(es)."), ephemeral=True)
    r = records[idx]
    e = _base(f"📎  Vouch Proof — #{idx+1}", color=C_SUCCESS)
    e.set_thumbnail(url=user.display_avatar.url)
    e.add_field(name="✅ Vouched For", value=user.mention,                                                           inline=True)
    e.add_field(name="👤 Vouched By",  value=f"<@{r['by']}>",                                                       inline=True)
    e.add_field(name="🕒 When",        value=f"<t:{int(r['timestamp'].timestamp())}:R>" if r.get("timestamp") else "?", inline=True)
    e.add_field(name="📊 Vouch",       value=f"**#{idx+1}** of **{len(records)}**",                                 inline=False)
    if r.get("proof_url"): e.set_image(url=r["proof_url"])
    ft(e, f"User ID: {user.id}")
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="removevouch", description="Remove vouches from a user")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User to remove vouches from", amount="How many to remove (default: 1)")
async def removevouch(interaction: discord.Interaction, user: discord.Member, amount: int = 1):
    records = get_vouches(interaction.guild.id, user.id)
    if not records: return await interaction.response.send_message(embed=err("No Vouches", f"{user.mention} has no vouches."), ephemeral=True)
    removed = min(amount, len(records))
    vouch_db[interaction.guild.id][user.id] = records[:-removed]
    remaining = len(vouch_db[interaction.guild.id][user.id])
    e = ok("Vouches Removed", f"Removed **{removed}** vouch(es) from {user.mention}.")
    e.add_field(name="📊 Remaining", value=f"**{remaining}**")
    ft(e, f"Removed by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="setvouchchannel", description="Set the dedicated vouch channel")
@app_commands.checks.has_permissions(administrator=True)
async def setvouchchannel(interaction: discord.Interaction, channel: discord.TextChannel):
    vouch_channels[interaction.guild.id] = channel.id
    await interaction.response.send_message(embed=ok("Vouch Channel Set", f"Vouches will be posted in {channel.mention}."), ephemeral=True)

@bot.tree.command(name="getvouchchannel", description="View the current vouch channel")
async def getvouchchannel(interaction: discord.Interaction):
    ch_id = vouch_channels.get(interaction.guild.id)
    if not ch_id: return await interaction.response.send_message(embed=err("Not Set", "No vouch channel set. Use `/setvouchchannel`."), ephemeral=True)
    ch = bot.get_channel(ch_id)
    await interaction.response.send_message(embed=info("Vouch Channel", f"Currently set to {ch.mention if ch else f'<#{ch_id}>'}"), ephemeral=True)



# ══════════════════════════════════════════════════════════
#  ANTINUKE SYSTEM
# ══════════════════════════════════════════════════════════

# ── Shared helpers ────────────────────────────────────────
_an_event_limits: dict = {}
_an_cooldowns:    dict = {}

def _an_can_act(guild_id, event_name, max_requests=5, interval=10, cooldown_duration=300):
    from datetime import datetime as _dt
    now = _dt.now()
    _an_event_limits.setdefault(guild_id, {}).setdefault(event_name, []).append(now)
    timestamps = [t for t in _an_event_limits[guild_id][event_name] if (now - t).total_seconds() <= interval]
    _an_event_limits[guild_id][event_name] = timestamps
    if guild_id in _an_cooldowns and event_name in _an_cooldowns[guild_id]:
        if (now - _an_cooldowns[guild_id][event_name]).total_seconds() < cooldown_duration:
            return False
        del _an_cooldowns[guild_id][event_name]
    if len(timestamps) > max_requests:
        _an_cooldowns.setdefault(guild_id, {})[event_name] = now
        return False
    return True

async def _an_fetch(guild, action, target_id=None):
    try:
        async for entry in guild.audit_logs(action=action, limit=1):
            if target_id and entry.target.id != target_id:
                return None
            age = (datetime.utcnow() - entry.created_at.replace(tzinfo=None)).total_seconds()
            if age > 3600:
                return None
            return entry
    except Exception:
        pass
    return None

def _an_immune(guild, user_id):
    return user_id == guild.owner_id or user_id == BOT_OWNER_ID or an_wl(guild.id, user_id)

async def _an_ban(guild, executor, reason, retries=3):
    while retries > 0:
        try:
            await guild.ban(executor, reason=reason, delete_message_days=0)
            return
        except discord.Forbidden: return
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2)))
                retries -= 1
            else: return
        except Exception: return

async def _an_log(guild, title, executor, detail="", result=""):
    log_id = antinuke_log_channels.get(guild.id)
    if not log_id: return
    ch = guild.get_channel(log_id)
    if not ch: return
    e = _base(f"🛡️  Antinuke — {title}", color=C_ERROR)
    e.add_field(name="👤  Executor", value=f"{executor.mention}\n`{executor.id}`", inline=True)
    e.add_field(name="⚡  Action",   value=title,                                    inline=True)
    if detail: e.add_field(name="📋  Detail", value=detail, inline=False)
    if result: e.add_field(name="⚙️  Result", value=result, inline=False)
    ft(e, "Crimson Gen • Antinuke")
    try: await ch.send(embed=e)
    except Exception: pass

# ── Anti-Ban (antiban.py) ─────────────────────────────────
@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "member_ban"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.ban, user.id)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    retries = 3
    while retries > 0:
        try:
            await guild.ban(executor, reason="Member Ban | Unwhitelisted User", delete_message_days=0)
            await guild.unban(user, reason="Reverting ban by unwhitelisted user")
            break
        except discord.Forbidden: break
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: break
        except Exception: break
    await _an_log(guild, "Anti-Ban", executor, f"Banned: {user}", "Victim unbanned · Executor banned")

# ── Anti-Kick (antikick.py) ───────────────────────────────
# NOTE: on_member_remove is defined elsewhere for welcome/leave — antikick logic merged there

# ── Anti-Channel Delete (antichdl.py) ────────────────────
@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "channel_delete"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.channel_delete, channel.id)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    retries = 3
    while retries > 0:
        try:
            new_ch = await channel.clone(reason="Channel Delete | Unwhitelisted User")
            await new_ch.edit(position=channel.position)
            break
        except discord.Forbidden: break
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: break
        except Exception: break
    await _an_ban(guild, executor, "Channel Delete | Unwhitelisted User")
    await _an_log(guild, "Anti-Channel Delete", executor, f"#{channel.name}", "Channel recreated · Executor banned")
    await asyncio.sleep(3)

# ── Anti-Channel Create (antichcr.py) ────────────────────
@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "channel_create", max_requests=6): return
    entry = await _an_fetch(guild, discord.AuditLogAction.channel_create, channel.id)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    retries = 3
    while retries > 0:
        try:
            await channel.delete(reason="Channel Create | Unwhitelisted User")
            await guild.ban(executor, reason="Channel Create | Unwhitelisted User", delete_message_days=0)
            return
        except discord.Forbidden: return
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: return
        except Exception: return
    await _an_log(guild, "Anti-Channel Create", executor, f"#{channel.name}", "Channel deleted · Executor banned")

# ── Anti-Channel Update (antichup.py) ────────────────────
@bot.event
async def on_guild_channel_update(before, after):
    guild = before.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "channel_update"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.channel_update, after.id)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    retries = 3
    while retries > 0:
        try:
            kwargs = {"name": before.name, "reason": "Channel Update | Unwhitelisted User"}
            if hasattr(before, "topic"):      kwargs["topic"]      = before.topic
            if hasattr(before, "nsfw"):       kwargs["nsfw"]       = before.nsfw
            if hasattr(before, "bitrate"):    kwargs["bitrate"]    = before.bitrate
            if hasattr(before, "user_limit"): kwargs["user_limit"] = before.user_limit
            await after.edit(**kwargs)
            break
        except discord.Forbidden: break
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: break
        except Exception: break
    await _an_ban(guild, executor, "Channel Update | Unwhitelisted User")
    await _an_log(guild, "Anti-Channel Update", executor, f"#{before.name}", "Reverted · Executor banned")
    await asyncio.sleep(3)

# ── Anti-Role Delete (antirldl.py) ───────────────────────
@bot.event
async def on_guild_role_delete(role: discord.Role):
    guild = role.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "role_delete"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.role_delete)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    await _an_ban(guild, executor, "Role Delete | Unwhitelisted User")
    try:
        await guild.create_role(
            name=role.name, permissions=role.permissions,
            color=role.color, hoist=role.hoist, mentionable=role.mentionable,
            reason="Role Delete | Unwhitelisted User"
        )
    except Exception: pass
    await _an_log(guild, "Anti-Role Delete", executor, f"@{role.name}", "Role recreated · Executor banned")
    await asyncio.sleep(2)

# ── Anti-Role Create (antirlcr.py) ───────────────────────
@bot.event
async def on_guild_role_create(role: discord.Role):
    guild = role.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "role_create"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.role_create)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    await _an_ban(guild, executor, "Role Create | Unwhitelisted User")
    try: await role.delete(reason="Role Create | Unwhitelisted User")
    except Exception: pass
    await _an_log(guild, "Anti-Role Create", executor, f"@{role.name}", "Role deleted · Executor banned")
    await asyncio.sleep(3)

# ── Anti-Role Update (antirlup.py) ───────────────────────
@bot.event
async def on_guild_role_update(before: discord.Role, after: discord.Role):
    guild = before.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "role_update"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.role_update, before.id)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    await _an_ban(guild, executor, "Role Update | Unwhitelisted User")
    retries = 3
    while retries > 0:
        try:
            await after.edit(
                name=before.name, permissions=before.permissions,
                color=before.color, hoist=before.hoist, mentionable=before.mentionable,
                reason="Role Update | Unwhitelisted User"
            )
            break
        except discord.Forbidden: break
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: break
        except Exception: break
    await _an_log(guild, "Anti-Role Update", executor, f"@{before.name}", "Reverted · Executor banned")
    await asyncio.sleep(3)

# ── Anti-Member Update / Dangerous Role (anti_member_update.py) ──
@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    guild = before.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "member_update"): return
    entry = await _an_fetch(guild, discord.AuditLogAction.member_role_update, after.id)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    try:
        new_role = next(r for r in after.roles if r not in before.roles)
    except StopIteration: return
    if not any([
        new_role.permissions.ban_members, new_role.permissions.administrator,
        new_role.permissions.manage_guild, new_role.permissions.manage_channels,
        new_role.permissions.manage_roles, new_role.permissions.mention_everyone,
        new_role.permissions.manage_webhooks
    ]): return
    retries = 3
    while retries > 0:
        try:
            await after.remove_roles(new_role, reason="Member Role Update with Dangerous Permissions | Unwhitelisted User")
            await guild.ban(executor, reason="Member Role Update with Dangerous Permissions | Unwhitelisted User", delete_message_days=0)
            return
        except discord.Forbidden: return
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: return
        except Exception: return
    await _an_log(guild, "Anti-Dangerous Role Grant", executor, f"@{new_role.name} → {after.mention}", "Role removed · Executor banned")
    await asyncio.sleep(3)

# ── Anti-Guild Update (antiguild.py) ─────────────────────
@bot.event
async def on_guild_update(before: discord.Guild, after: discord.Guild):
    if not an_on(after.id): return
    if not _an_can_act(after.id, "guild_update"): return
    entry = await _an_fetch(after, discord.AuditLogAction.guild_update)
    if not entry: return
    executor = entry.user
    if _an_immune(after, executor.id): return
    # Ban executor
    retries = 3
    while retries > 0:
        try:
            await after.ban(executor, reason="Guild Update | Unwhitelisted User", delete_message_days=0)
            break
        except discord.Forbidden: break
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: break
        except Exception: break
    # Revert changes
    retries = 3
    while retries > 0:
        try:
            kwargs = {}
            if before.name != after.name: kwargs["name"] = before.name
            if before.icon != after.icon: kwargs["icon"] = before.icon
            if before.splash != after.splash: kwargs["splash"] = before.splash
            if before.banner != after.banner: kwargs["banner"] = before.banner
            if kwargs: await after.edit(**kwargs, reason="Guild Update | Unwhitelisted User")
            break
        except discord.Forbidden: break
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
            else: break
        except Exception: break
    await _an_log(after, "Anti-Guild Update", executor, "", "Reverted · Executor banned")
    await asyncio.sleep(3)

# ── Anti-Webhook Update (antiwebhook.py) ─────────────────
@bot.event
async def on_webhooks_update(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "webhook_update", max_requests=6): return
    entry = await _an_fetch(guild, discord.AuditLogAction.webhook_update)
    if entry:
        executor = entry.user
        if not _an_immune(guild, executor.id):
            retries = 3
            while retries > 0:
                try:
                    await guild.ban(executor, reason="Webhook Update | Unwhitelisted User", delete_message_days=0)
                    if entry.target:
                        try: await entry.target.delete(reason="Webhook updated by unwhitelisted user")
                        except Exception: pass
                    break
                except discord.Forbidden: break
                except discord.HTTPException as e:
                    if e.status == 429:
                        await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
                    else: break
                except Exception: break
            await _an_log(guild, "Anti-Webhook Update", executor, f"#{channel.name}", "Executor banned")
            await asyncio.sleep(3)
            return
    # Also check webhook create
    entry = await _an_fetch(guild, discord.AuditLogAction.webhook_create)
    if entry:
        executor = entry.user
        if not _an_immune(guild, executor.id):
            retries = 3
            while retries > 0:
                try:
                    await guild.ban(executor, reason="Webhook Create | Unwhitelisted User", delete_message_days=0)
                    if entry.target:
                        try: await entry.target.delete(reason="Webhook created by unwhitelisted user")
                        except Exception: pass
                    break
                except discord.Forbidden: break
                except discord.HTTPException as e:
                    if e.status == 429:
                        await asyncio.sleep(float(e.response.headers.get("Retry-After", 2))); retries -= 1
                    else: break
                except Exception: break
            await _an_log(guild, "Anti-Webhook Create", executor, f"#{channel.name}", "Webhook deleted · Executor banned")
            await asyncio.sleep(3)
            return
    # Also check webhook delete
    entry = await _an_fetch(guild, discord.AuditLogAction.webhook_delete)
    if entry:
        executor = entry.user
        if not _an_immune(guild, executor.id):
            await _an_ban(guild, executor, "Webhook Delete | Unwhitelisted User")
            await _an_log(guild, "Anti-Webhook Delete", executor, f"#{channel.name}", "Executor banned")
            await asyncio.sleep(3)

# ── Anti-Integration (antiIntegration.py) ────────────────
@bot.event
async def on_guild_integrations_update(guild: discord.Guild):
    if not an_on(guild.id): return
    if not _an_can_act(guild.id, "integration_create", max_requests=6): return
    entry = await _an_fetch(guild, discord.AuditLogAction.integration_create)
    if not entry: return
    executor = entry.user
    if _an_immune(guild, executor.id): return
    await _an_ban(guild, executor, "Integration Create | Unwhitelisted User")
    await _an_log(guild, "Anti-Integration", executor, "", "Executor banned")

# ── Anti-Prune (antiprune.py) ─────────────────────────────
# Handled in on_member_remove below alongside antikick

# ── Anti-Everyone (antieveryone.py) ──────────────────────
# Handled in on_message — timeout + delete @everyone messages

# ── Antinuke Commands ─────────────────────────────────────
@bot.tree.command(name="antinuke", description="Toggle antinuke server protection")
@app_commands.checks.has_permissions(administrator=True)
async def antinuke(interaction: discord.Interaction):
    gid    = interaction.guild.id
    now_on = not antinuke_enabled.get(gid, False)
    antinuke_enabled[gid] = now_on
    log_id = antinuke_log_channels.get(gid)
    e = _base("🛡️  Antinuke System",
              f"Protection is now **{'🟢 ENABLED' if now_on else '🔴 DISABLED'}**",
              C_SUCCESS if now_on else C_ERROR)
    e.add_field(
        name="🔒  Active Protections",
        value=(
            "› Anti-Ban + unban victim\n"
            "› Anti-Kick\n"
            "› Anti-Channel Delete + recreate\n"
            "› Anti-Channel Create\n"
            "› Anti-Channel Update + revert\n"
            "› Anti-Role Delete + recreate\n"
            "› Anti-Role Create\n"
            "› Anti-Role Update + revert\n"
            "› Anti-Dangerous Role Grant\n"
            "› Anti-Guild Update + revert\n"
            "› Anti-Webhook Create/Update/Delete\n"
            "› Anti-Integration\n"
            "› Anti-Prune\n"
            "› Anti-Everyone Mention\n"
            "› Anti-Bot Add"
        ),
        inline=True
    )
    e.add_field(
        name="⚙️  Configuration",
        value=(
            f"Punishment: Instant ban\n"
            f"Revert: Yes (where possible)\n"
            f"Log: {f'<#{log_id}>' if log_id else 'Not set'}"
        ),
        inline=True
    )
    ft(e, "Crimson Gen • Antinuke", interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="antinuke_status", description="View the full antinuke status dashboard")
@app_commands.checks.has_permissions(administrator=True)
async def antinuke_status(interaction: discord.Interaction):
    gid     = interaction.guild.id
    enabled = antinuke_enabled.get(gid, False)
    wl      = antinuke_wl.get(gid, [])
    log_id  = antinuke_log_channels.get(gid)
    e = _base("🛡️  Antinuke Status Dashboard", color=C_SUCCESS if enabled else C_ERROR)
    e.add_field(name="⚡ Status",      value="🟢 Enabled" if enabled else "🔴 Disabled", inline=True)
    e.add_field(name="📋 Log Channel", value=f"<#{log_id}>" if log_id else "Not set",    inline=True)
    e.add_field(name="⚠️ Punishment",  value="Instant ban + revert",                     inline=True)
    wl_str = "\n".join(f"› <@{uid}>" for uid in wl) or "No whitelisted users."
    e.add_field(name=f"✅  Whitelist ({len(wl)} users)", value=wl_str, inline=False)
    ft(e, "Crimson Gen • Antinuke", interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="antinuke_whitelist", description="Add or remove a user from the antinuke whitelist")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User to whitelist or unwhitelist", action="Add or remove")
@app_commands.choices(action=[
    app_commands.Choice(name="✅ Add to whitelist",      value="add"),
    app_commands.Choice(name="❌ Remove from whitelist", value="remove"),
])
async def antinuke_whitelist_cmd(interaction: discord.Interaction, user: discord.Member, action: str):
    gid = interaction.guild.id
    antinuke_wl.setdefault(gid, [])
    if action == "add":
        if user.id not in antinuke_wl[gid]: antinuke_wl[gid].append(user.id)
        e = ok("Whitelisted", f"{user.mention} added to antinuke whitelist.")
    else:
        if user.id in antinuke_wl[gid]: antinuke_wl[gid].remove(user.id)
        e = ok("Removed", f"{user.mention} removed from antinuke whitelist.")
    e.set_thumbnail(url=user.display_avatar.url)
    ft(e, "Crimson Gen • Antinuke", interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="antinuke_setlog", description="Set the channel for antinuke action logs")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Where antinuke logs should be sent")
async def antinuke_setlog(interaction: discord.Interaction, channel: discord.TextChannel):
    antinuke_log_channels[interaction.guild.id] = channel.id
    e = ok("Log Channel Set", f"Antinuke logs will now be sent to {channel.mention}.")
    ft(e, "Crimson Gen • Antinuke", interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

# ══════════════════════════════════════════════════════════
#  AUTO-MODERATION COMMANDS
# ══════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════
#  AUTOMOD COMMANDS
# ══════════════════════════════════════════════════════════

FILTER_LABELS = {
    "filter_profanity":  ("🤬", "Profanity",        "Blocks banned words and slurs"),
    "filter_invites":    ("📨", "Invite Links",      "Blocks Discord server invite links"),
    "filter_links":      ("🔗", "All Links",         "Blocks all http/www links"),
    "filter_scam":       ("🎣", "Scam Detection",    "Blocks known phishing & scam domains"),
    "filter_caps":       ("🔊", "Excessive Caps",    "Blocks messages that are >70% caps"),
    "filter_spam":       ("⚡", "Spam",              f"Blocks {SPAM_COUNT}+ msgs in {SPAM_WINDOW}s"),
    "filter_mentions":   ("📢", "Mass Mentions",     f"Blocks {MENTION_MAX}+ mentions in one message"),
    "filter_zalgo":      ("🔀", "Zalgo Text",        "Blocks corrupted/zalgo characters"),
    "filter_emoji":      ("😂", "Emoji Spam",        "Blocks 8+ emojis in one message"),
    "filter_duplicates": ("📋", "Duplicates",        f"Blocks same message sent {DUP_COUNT}x in {DUP_WINDOW}s"),
    "filter_length":     ("📏", "Long Messages",     f"Blocks messages over {MAX_MSG_LEN} characters"),
    "warn_on_delete":    ("⚠️", "Warn on Delete",   "Sends a warning when a message is removed"),
    "auto_timeout":      ("⏱️", "Auto Timeout",      "Times out repeat offenders automatically"),
}

def _build_automod_embed(guild: discord.Guild, cfg: dict) -> discord.Embed:
    """Build the full AutoMod dashboard embed."""
    on     = cfg["enabled"]
    status = "🟢  **ENABLED**" if on else "🔴  **DISABLED**"
    col    = C_SUCCESS if on else C_ERROR
    log_ch = f"<#{cfg['log_channel']}>" if cfg["log_channel"] else "`Not set`"

    e = discord.Embed(color=col, timestamp=datetime.utcnow())
    e.description = (
        f"## 🤖  AutoMod System\n"
        f"> Status: {status}\n"
        f"> Log Channel: {log_ch}\n"
        f"> Auto Timeout: {'🟢 On' if cfg.get('auto_timeout') else '🔴 Off'}"
        f"  ·  Warn on Delete: {'🟢 On' if cfg.get('warn_on_delete') else '🔴 Off'}"
    )
    if guild.icon:
        e.set_thumbnail(url=guild.icon.url)

    # ── Filter status split into two columns ──────────────────
    filters_left = [
        "filter_profanity", "filter_invites", "filter_links",
        "filter_scam",      "filter_caps",    "filter_spam",
    ]
    filters_right = [
        "filter_mentions",   "filter_zalgo", "filter_emoji",
        "filter_duplicates", "filter_length",
    ]

    def row(key):
        emoji, label, _ = FILTER_LABELS[key]
        dot = "🟢" if cfg.get(key, False) else "🔴"
        return f"{dot} {emoji} {label}"

    e.add_field(
        name="🔍  Filters",
        value="\n".join(row(k) for k in filters_left),
        inline=True
    )
    e.add_field(
        name="\u200b",
        value="\n".join(row(k) for k in filters_right),
        inline=True
    )
    e.add_field(name="\u200b", value="\u200b", inline=True)  # spacer

    # ── Whitelist info ────────────────────────────────────────
    wl_roles = [f"<@&{r}>" for r in cfg["whitelist_roles"]] or ["`None`"]
    wl_chs   = [f"<#{c}>" for c in cfg["whitelist_channels"]] or ["`None`"]
    e.add_field(name="🎭  Whitelisted Roles",    value=" ".join(wl_roles[:10]),  inline=True)
    e.add_field(name="💬  Whitelisted Channels", value=" ".join(wl_chs[:10]),    inline=True)
    e.add_field(name="🚫  Custom Words",
                value=f"`{len(cfg['custom_words'])}` word(s) banned",            inline=True)

    # Strike thresholds
    e.add_field(
        name="⏱️  Strike → Timeout Thresholds",
        value="3 strikes → 1 min  ·  5 strikes → 5 min  ·  7 strikes → 1 hour",
        inline=False
    )

    e.set_footer(text=f"Crimson Gen • AutoMod  ·  {guild.name}", icon_url=guild.me.display_avatar.url)
    return e





@bot.tree.command(name="automod_toggle", description="Enable or disable AutoMod entirely")
@app_commands.checks.has_permissions(administrator=True)
async def automod_toggle(interaction: discord.Interaction):
    cfg           = get_automod(interaction.guild.id)
    cfg["enabled"] = not cfg["enabled"]
    on             = cfg["enabled"]
    e = discord.Embed(color=C_SUCCESS if on else C_ERROR, timestamp=datetime.utcnow())
    e.description  = (
        f"## {'✅' if on else '❌'}  AutoMod {'Enabled' if on else 'Disabled'}\n"
        f"> AutoMod is now **{'🟢 ACTIVE' if on else '🔴 INACTIVE'}** for this server."
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_status", description="View the full AutoMod dashboard")
@app_commands.checks.has_permissions(manage_guild=True)
async def automod_status(interaction: discord.Interaction):
    cfg = get_automod(interaction.guild.id)
    e   = _build_automod_embed(interaction.guild, cfg)
    if cfg["custom_words"]:
        words = " · ".join(f"||`{w}`||" for w in cfg["custom_words"])
        e.add_field(
            name=f"📝  Banned Words ({len(cfg['custom_words'])})",
            value=words[:1000],
            inline=False
        )
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_filter", description="Toggle a specific AutoMod filter on or off")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(filter_name="Which filter to toggle")
@app_commands.choices(filter_name=[
    app_commands.Choice(name="🤬 Profanity / bad words",         value="filter_profanity"),
    app_commands.Choice(name="📨 Discord invite links",           value="filter_invites"),
    app_commands.Choice(name="🔗 All links",                      value="filter_links"),
    app_commands.Choice(name="🎣 Scam / phishing detection",      value="filter_scam"),
    app_commands.Choice(name="🔊 Excessive CAPS",                 value="filter_caps"),
    app_commands.Choice(name="⚡ Message spam",                   value="filter_spam"),
    app_commands.Choice(name="📢 Mass mentions",                  value="filter_mentions"),
    app_commands.Choice(name="🔀 Zalgo / corrupted text",         value="filter_zalgo"),
    app_commands.Choice(name="😂 Emoji spam",                     value="filter_emoji"),
    app_commands.Choice(name="📋 Duplicate messages",             value="filter_duplicates"),
    app_commands.Choice(name="📏 Long messages",                  value="filter_length"),
    app_commands.Choice(name="⚠️ Warn user on delete",           value="warn_on_delete"),
    app_commands.Choice(name="⏱️ Auto timeout on repeat offense", value="auto_timeout"),
])
async def automod_filter(interaction: discord.Interaction, filter_name: str):
    cfg             = get_automod(interaction.guild.id)
    cfg[filter_name] = not cfg.get(filter_name, False)
    on              = cfg[filter_name]
    emoji, label, desc = FILTER_LABELS.get(filter_name, ("⚙️", filter_name, ""))
    col = C_SUCCESS if on else C_ERROR
    e   = discord.Embed(color=col, timestamp=datetime.utcnow())
    e.description = (
        f"## {'✅' if on else '❌'}  {label} {'Enabled' if on else 'Disabled'}\n"
        f"> {emoji} **{label}** is now **{'🟢 ON' if on else '🔴 OFF'}**\n"
        f"> *{desc}*"
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_setlog", description="Set the channel where AutoMod logs are sent")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Channel to send AutoMod logs to")
async def automod_setlog(interaction: discord.Interaction, channel: discord.TextChannel):
    get_automod(interaction.guild.id)["log_channel"] = channel.id
    e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = (
        f"## ✅  Log Channel Set\n"
        f"> AutoMod logs will now be sent to {channel.mention}"
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_addword", description="Add a word to the custom banned words list")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(word="Word to ban (case-insensitive, partial match)")
async def automod_addword(interaction: discord.Interaction, word: str):
    cfg = get_automod(interaction.guild.id)
    w   = word.lower().strip()
    if w in cfg["custom_words"]:
        e = discord.Embed(color=C_WARNING, timestamp=datetime.utcnow())
        e.description = f"## ⚠️  Already Banned\n> `{w}` is already in the banned words list."
        return await interaction.response.send_message(embed=e, ephemeral=True)
    cfg["custom_words"].append(w)
    e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = (
        f"## ✅  Word Added\n"
        f"> ||`{w}`|| has been added to the banned list.\n"
        f"> Total custom words: **{len(cfg['custom_words'])}**"
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_removeword", description="Remove a word from the custom banned words list")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(word="Word to unban")
async def automod_removeword(interaction: discord.Interaction, word: str):
    cfg = get_automod(interaction.guild.id)
    w   = word.lower().strip()
    if w not in cfg["custom_words"]:
        e = discord.Embed(color=C_ERROR, timestamp=datetime.utcnow())
        e.description = f"## ❌  Not Found\n> `{w}` is not in the custom banned words list."
        return await interaction.response.send_message(embed=e, ephemeral=True)
    cfg["custom_words"].remove(w)
    e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = (
        f"## ✅  Word Removed\n"
        f"> `{w}` has been removed from the banned list.\n"
        f"> Remaining custom words: **{len(cfg['custom_words'])}**"
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_words", description="View all custom banned words")
@app_commands.checks.has_permissions(manage_guild=True)
async def automod_words(interaction: discord.Interaction):
    cfg = get_automod(interaction.guild.id)
    e   = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
    if not cfg["custom_words"]:
        e.description = "## 📝  Custom Banned Words\n> No custom words added yet."
    else:
        words = "\n".join(f"`{i+1}.` ||`{w}`||" for i, w in enumerate(cfg["custom_words"]))
        e.description = f"## 📝  Custom Banned Words ({len(cfg['custom_words'])})\n{words[:3000]}"
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_whitelist", description="Add or remove a role/channel from the AutoMod whitelist")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    action="Add or remove from whitelist",
    role="Role to whitelist (immune to all AutoMod)",
    channel="Channel to whitelist (AutoMod won't scan it)"
)
@app_commands.choices(action=[
    app_commands.Choice(name="✅ Add to whitelist",      value="add"),
    app_commands.Choice(name="❌ Remove from whitelist", value="remove"),
])
async def automod_whitelist(interaction: discord.Interaction,
                             action: str,
                             role: discord.Role = None,
                             channel: discord.TextChannel = None):
    if not role and not channel:
        e = discord.Embed(color=C_ERROR, timestamp=datetime.utcnow())
        e.description = "## ❌  Missing Target\n> Please provide a role or channel to whitelist."
        return await interaction.response.send_message(embed=e, ephemeral=True)

    cfg   = get_automod(interaction.guild.id)
    lines = []

    if role:
        if action == "add":
            if role.id not in cfg["whitelist_roles"]:
                cfg["whitelist_roles"].append(role.id)
            lines.append(f"✅ {role.mention} added to role whitelist")
        else:
            if role.id in cfg["whitelist_roles"]:
                cfg["whitelist_roles"].remove(role.id)
            lines.append(f"✅ {role.mention} removed from role whitelist")

    if channel:
        if action == "add":
            if channel.id not in cfg["whitelist_channels"]:
                cfg["whitelist_channels"].append(channel.id)
            lines.append(f"✅ {channel.mention} added to channel whitelist")
        else:
            if channel.id in cfg["whitelist_channels"]:
                cfg["whitelist_channels"].remove(channel.id)
            lines.append(f"✅ {channel.mention} removed from channel whitelist")

    # Show current whitelist state
    wl_roles = " ".join(f"<@&{r}>" for r in cfg["whitelist_roles"]) or "`None`"
    wl_chs   = " ".join(f"<#{c}>" for c in cfg["whitelist_channels"]) or "`None`"

    e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = "## ✅  Whitelist Updated\n> " + "\n> ".join(lines)
    e.add_field(name="🎭  Whitelisted Roles",    value=wl_roles[:1024], inline=False)
    e.add_field(name="💬  Whitelisted Channels", value=wl_chs[:1024],   inline=False)
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_reset", description="Reset all AutoMod strikes for a user")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User whose strikes to reset")
async def automod_reset(interaction: discord.Interaction, user: discord.Member):
    gid = interaction.guild.id
    automod_warn_count.setdefault(gid, {})
    old = automod_warn_count[gid].pop(user.id, 0)
    e   = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = (
        f"## ✅  Strikes Reset\n"
        f"> {user.mention}'s AutoMod strikes have been cleared.\n"
        f"> Previous strikes: **{old}** → **0**"
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="automod_strikes", description="Check how many AutoMod strikes a user has")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.describe(user="User to check")
async def automod_strikes(interaction: discord.Interaction, user: discord.Member):
    strikes = automod_warn_count.get(interaction.guild.id, {}).get(user.id, 0)
    next_to = next((t for t in sorted(STRIKE_TIMEOUT) if t > strikes), None)
    col = C_SUCCESS if strikes == 0 else C_WARNING if strikes < 5 else C_ERROR
    e   = discord.Embed(color=col, timestamp=datetime.utcnow())
    e.set_thumbnail(url=user.display_avatar.url)
    e.description = (
        f"## ⚡  AutoMod Strikes — {user.display_name}\n"
        f"> Current strikes: **{strikes}**\n"
        f"> Next timeout at: **{next_to} strikes**" if next_to else
        f"## ⚡  AutoMod Strikes — {user.display_name}\n"
        f"> Current strikes: **{strikes}**\n"
        f"> ⚠️ Maximum threshold reached"
    )
    e.set_footer(text="Crimson Gen • AutoMod", icon_url=interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)



# ══════════════════════════════════════════════════════════
#  ROLE MANAGEMENT
# ══════════════════════════════════════════════════════════

@bot.tree.command(name="role", description="Add or remove a role from a specific member")
@app_commands.checks.has_permissions(manage_roles=True)
@app_commands.describe(
    action="Add or remove the role",
    member="The member to give/remove the role from",
    role="The role to give/remove"
)
@app_commands.choices(action=[
    app_commands.Choice(name="➕ Add",    value="add"),
    app_commands.Choice(name="➖ Remove", value="remove"),
])
async def role_single(interaction: discord.Interaction, action: str, member: discord.Member, role: discord.Role):
    if role.managed:
        return await interaction.response.send_message(embed=err("Can't Modify", f"{role.mention} is a managed role."), ephemeral=True)
    if role >= interaction.guild.me.top_role:
        return await interaction.response.send_message(embed=err("Role Too High", f"{role.mention} is above my highest role."), ephemeral=True)
    if role >= interaction.user.top_role and interaction.user.id != interaction.guild.owner_id and interaction.user.id != BOT_OWNER_ID:
        return await interaction.response.send_message(embed=err("Role Too High", f"You can't assign {role.mention} — it's above your top role."), ephemeral=True)
    try:
        if action == "add":
            await member.add_roles(role, reason=f"Role added by {interaction.user}")
            e = ok("Role Added", f"Added {role.mention} to {member.mention}.")
        else:
            await member.remove_roles(role, reason=f"Role removed by {interaction.user}")
            e = ok("Role Removed", f"Removed {role.mention} from {member.mention}.")
        ft(e, "Crimson Gen", interaction.user.display_avatar.url)
        await interaction.response.send_message(embed=e, ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(embed=err("No Permission", "I don't have permission to manage that role."), ephemeral=True)


@bot.tree.command(name="roleall", description="Add or remove a role from every member in the server")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    action="Add or remove the role",
    role="The role to give/remove"
)
@app_commands.choices(action=[
    app_commands.Choice(name="➕ Add to everyone",      value="add"),
    app_commands.Choice(name="➖ Remove from everyone", value="remove"),
])
async def role_everyone(interaction: discord.Interaction, action: str, role: discord.Role):
    await interaction.response.defer(ephemeral=True)

    if role.managed:
        return await interaction.followup.send(
            embed=err("Can't Modify", f"{role.mention} is managed by an integration and can't be assigned manually."),
            ephemeral=True
        )
    if role >= interaction.guild.me.top_role:
        return await interaction.followup.send(
            embed=err("Role Too High", f"{role.mention} is above my highest role. Move my role above it first."),
            ephemeral=True
        )

    members = [m for m in interaction.guild.members if not m.bot]
    action_word = "Adding" if action == "add" else "Removing"
    direction   = "to" if action == "add" else "from"

    e = _base(f"⏳  {action_word} Role...",
              f"{action_word} {role.mention} {direction} **{len(members)}** members...",
              C_WARNING)
    ft(e, "Crimson Gen")
    await interaction.followup.send(embed=e, ephemeral=True)

    success = 0
    failed  = 0

    async def assign(member):
        nonlocal success, failed
        try:
            if action == "add":
                if role not in member.roles:
                    await member.add_roles(role, reason=f"Mass role by {interaction.user}")
            else:
                if role in member.roles:
                    await member.remove_roles(role, reason=f"Mass role by {interaction.user}")
            success += 1
        except Exception:
            failed += 1

    # Run all at once in batches of 10 to stay under rate limits but go fast
    batch_size = 10
    for i in range(0, len(members), batch_size):
        await asyncio.gather(*[assign(m) for m in members[i:i+batch_size]])

    action_done = "Added" if action == "add" else "Removed"
    direction2  = "to" if action == "add" else "from"
    fail_note   = f"\n⚠️ Failed for **{failed}** member(s)." if failed else ""
    result = ok(
        f"Role {action_done}!",
        f"{action_done} {role.mention} {direction2} **{success}** member(s).{fail_note}"
    )
    result.set_thumbnail(url=interaction.guild.icon.url if interaction.guild.icon else None)
    ft(result, f"Done by {interaction.user.name}", interaction.user.display_avatar.url)
    await interaction.edit_original_response(embed=result)

# ══════════════════════════════════════════════════════════
#  RUN
# ══════════════════════════════════════════════════════════
@bot.tree.command(name="roleperms", description="Edit a role's permissions")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    role="Role to edit",
    permission="Permission to toggle",
    enabled="Turn it on or off"
)
@app_commands.choices(permission=[
    app_commands.Choice(name="👑 Administrator",        value="administrator"),
    app_commands.Choice(name="🔨 Ban Members",          value="ban_members"),
    app_commands.Choice(name="👢 Kick Members",         value="kick_members"),
    app_commands.Choice(name="⚙️ Manage Server",        value="manage_guild"),
    app_commands.Choice(name="🎭 Manage Roles",         value="manage_roles"),
    app_commands.Choice(name="💬 Manage Channels",      value="manage_channels"),
    app_commands.Choice(name="📌 Manage Messages",      value="manage_messages"),
    app_commands.Choice(name="⏱️ Manage Threads",       value="manage_threads"),
    app_commands.Choice(name="😎 Manage Nicknames",     value="manage_nicknames"),
    app_commands.Choice(name="😂 Manage Expressions",   value="manage_expressions"),
    app_commands.Choice(name="🔇 Mute Members",         value="mute_members"),
    app_commands.Choice(name="🔕 Deafen Members",       value="deafen_members"),
    app_commands.Choice(name="📢 Mention Everyone",      value="mention_everyone"),
    app_commands.Choice(name="🔗 Embed Links",           value="embed_links"),
    app_commands.Choice(name="📁 Attach Files",          value="attach_files"),
    app_commands.Choice(name="📜 View Audit Log",        value="view_audit_log"),
])
async def roleperms(interaction: discord.Interaction, role: discord.Role, permission: str, enabled: bool):
    await interaction.response.defer(ephemeral=True)

    # Only owner ID can grant administrator
    if permission == "administrator" and interaction.user.id != BOT_OWNER_ID and interaction.user.id != interaction.guild.owner_id:
        return await interaction.followup.send(
            embed=err("Restricted", "Only the server owner can grant Administrator permission."), ephemeral=True
        )

    try:
        current_perms = role.permissions
        new_perms     = discord.Permissions(**{**dict(iter(current_perms)), permission: enabled})
        await role.edit(permissions=new_perms, reason=f"Permission edited by {interaction.user}")
        e = discord.Embed(color=C_SUCCESS if enabled else C_WARNING, timestamp=datetime.utcnow())
        e.description = (
            f"## {'✅' if enabled else '❌'}  Permission {'Enabled' if enabled else 'Disabled'}\n"
            f"> **Role:** {role.mention}\n"
            f"> **Permission:** `{permission}`\n"
            f"> **Status:** {'🟢 ON' if enabled else '🔴 OFF'}"
        )
        ft(e, f"Edited by {interaction.user.name}", interaction.user.display_avatar.url)
        await interaction.followup.send(embed=e, ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send(
            embed=err("Failed", "I don't have permission to edit that role. Make sure my role is above it."), ephemeral=True
        )
    except Exception as ex:
        await interaction.followup.send(embed=err("Error", str(ex)), ephemeral=True)
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        if interaction.user.id == BOT_OWNER_ID:
            # Re-invoke the command bypassing the permission check
            await interaction.command.callback(interaction, **{
                k: v for k, v in interaction.namespace.__dict__.items()
            })
            return
        await interaction.response.send_message(
            embed=err("No Permission", "You don't have permission to use this command."),
            ephemeral=True
        )
    else:
        try:
            await interaction.response.send_message(
                embed=err("Error", f"Something went wrong: `{error}`"), ephemeral=True
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  GIVEAWAY SYSTEM
# ══════════════════════════════════════════════════════════
import re as _re

giveaways: dict = {}  # {message_id: giveaway_data}

def parse_duration(s: str) -> int | None:
    """Parse duration string like 1d2h30m into seconds."""
    total = 0
    for val, unit in _re.findall(r"(\d+)\s*([smhd])", s.lower()):
        val = int(val)
        if unit == "s": total += val
        elif unit == "m": total += val * 60
        elif unit == "h": total += val * 3600
        elif unit == "d": total += val * 86400
    return total if total > 0 else None

def format_duration(seconds: int) -> str:
    d, r = divmod(int(seconds), 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s: parts.append(f"{s}s")
    return " ".join(parts) or "0s"

def giveaway_embed(data: dict, ended: bool = False) -> discord.Embed:
    ends_at   = data["ends_at"]
    winners_n = data["winners"]
    prize     = data["prize"]
    host_id   = data["host_id"]
    entries   = data.get("entries", [])
    req_role  = data.get("req_role")
    bonus     = data.get("bonus_entries", {})

    color = 0xFFD700 if not ended else C_ERROR
    e = discord.Embed(color=color, timestamp=datetime.utcnow())
    e.title = f"🎉  {prize}"

    if not ended:
        e.description = (
            f"React with 🎉 to enter!\n\n"
            f"**Ends:** <t:{int(ends_at)}:R>\n"
            f"**Winners:** `{winners_n}`\n"
            f"**Host:** <@{host_id}>\n"
            f"**Entries:** `{len(entries)}`"
        )
        if req_role:
            e.description += f"\n**Required Role:** <@&{req_role}>"
        if bonus:
            lines = "\n".join(f"<@&{rid}>: +{amt} entries" for rid, amt in bonus.items())
            e.add_field(name="🎁  Bonus Entries", value=lines, inline=False)
    else:
        winners = data.get("winners_list", [])
        if winners:
            win_str = " ".join(f"<@{w}>" for w in winners)
            e.description = f"**Prize:** {prize}\n**Winners:** {win_str}\n**Host:** <@{host_id}>"
        else:
            e.description = f"**Prize:** {prize}\nNo valid entries. No winners."
        e.title = f"🎊  {prize} — Ended"

    ft(e, "Crimson Gen • Giveaways")
    return e

async def end_giveaway(channel_id: int, message_id: int):
    await asyncio.sleep(0.1)
    data = giveaways.get(message_id)
    if not data or data.get("ended"): return
    data["ended"] = True

    ch = bot.get_channel(channel_id)
    if not ch: return
    try: msg = await ch.fetch_message(message_id)
    except Exception: return

    entries = data.get("entries", [])
    winners_n = data["winners"]
    winners = []
    if entries:
        pool = entries[:]
        # Add bonus entries
        bonus = data.get("bonus_entries", {})
        guild = ch.guild
        for uid in list(entries):
            member = guild.get_member(uid)
            if member:
                for rid, extra in bonus.items():
                    if any(r.id == rid for r in member.roles):
                        pool.extend([uid] * extra)
        random.shuffle(pool)
        seen = set()
        for uid in pool:
            if uid not in seen:
                seen.add(uid)
                winners.append(uid)
            if len(winners) >= winners_n:
                break

    data["winners_list"] = winners
    e = giveaway_embed(data, ended=True)
    try: await msg.edit(embed=e)
    except Exception: pass

    if winners:
        win_mentions = " ".join(f"<@{w}>" for w in winners)
        await ch.send(
            f"🎉 Congratulations {win_mentions}! You won **{data['prize']}**!\n"
            f"Hosted by <@{data['host_id']}>",
            reference=msg
        )
    else:
        await ch.send("😔 No valid entries for the giveaway. No winners.", reference=msg)

async def schedule_giveaway(channel_id: int, message_id: int, delay: float):
    await asyncio.sleep(delay)
    await end_giveaway(channel_id, message_id)


# ── Giveaway Commands ─────────────────────────────────────
@bot.tree.command(name="gstart", description="Start a giveaway")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(
    duration="Duration e.g. 1d2h30m",
    winners="Number of winners",
    prize="What you're giving away",
    channel="Channel to host in (default: current)",
    required_role="Role required to enter",
    description="Extra description",
)
async def gstart(
    interaction: discord.Interaction,
    duration: str,
    winners: int,
    prize: str,
    channel: discord.TextChannel = None,
    required_role: discord.Role = None,
    description: str = None,
):
    await interaction.response.defer(ephemeral=True)
    secs = parse_duration(duration)
    if not secs:
        return await interaction.followup.send(embed=err("Invalid Duration", "Use format like `1d`, `2h30m`, `30s`."), ephemeral=True)
    if winners < 1 or winners > 20:
        return await interaction.followup.send(embed=err("Invalid Winners", "Between 1 and 20 winners."), ephemeral=True)

    ch = channel or interaction.channel
    ends_at = datetime.utcnow().timestamp() + secs

    data = {
        "prize":        prize,
        "winners":      winners,
        "host_id":      interaction.user.id,
        "ends_at":      ends_at,
        "channel_id":   ch.id,
        "entries":      [],
        "ended":        False,
        "req_role":     required_role.id if required_role else None,
        "bonus_entries":{},
        "description":  description,
    }

    e = giveaway_embed(data)
    if description:
        e.description = description + "\n\n" + (e.description or "")

    msg = await ch.send(embed=e)
    await msg.add_reaction("🎉")
    data["message_id"] = msg.id
    giveaways[msg.id]  = data

    asyncio.create_task(schedule_giveaway(ch.id, msg.id, secs))

    confirm = ok("Giveaway Started!", f"🎉 Giveaway for **{prize}** started in {ch.mention}!\nEnds in **{format_duration(secs)}** · **{winners}** winner(s).")
    await interaction.followup.send(embed=confirm, ephemeral=True)


@bot.tree.command(name="gend", description="End a giveaway early")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(message_id="The giveaway message ID")
async def gend(interaction: discord.Interaction, message_id: str):
    await interaction.response.defer(ephemeral=True)
    mid = int(message_id) if message_id.isdigit() else None
    if not mid or mid not in giveaways:
        return await interaction.followup.send(embed=err("Not Found", "No active giveaway with that ID."), ephemeral=True)
    data = giveaways[mid]
    if data.get("ended"):
        return await interaction.followup.send(embed=err("Already Ended", "This giveaway has already ended."), ephemeral=True)
    await end_giveaway(data["channel_id"], mid)
    await interaction.followup.send(embed=ok("Giveaway Ended", "The giveaway has been ended early."), ephemeral=True)


@bot.tree.command(name="greroll", description="Reroll a giveaway winner")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(message_id="The giveaway message ID", winners="Number of new winners to pick")
async def greroll(interaction: discord.Interaction, message_id: str, winners: int = 1):
    await interaction.response.defer(ephemeral=True)
    mid = int(message_id) if message_id.isdigit() else None
    data = giveaways.get(mid)
    if not data or not data.get("ended"):
        return await interaction.followup.send(embed=err("Not Found", "No ended giveaway with that ID."), ephemeral=True)

    entries = data.get("entries", [])
    if not entries:
        return await interaction.followup.send(embed=err("No Entries", "Nobody entered this giveaway."), ephemeral=True)

    pool = random.sample(entries, min(winners, len(entries)))
    win_mentions = " ".join(f"<@{w}>" for w in pool)

    ch = bot.get_channel(data["channel_id"])
    if ch:
        try:
            msg = await ch.fetch_message(mid)
            await ch.send(
                f"🎉 **Reroll!** New winner(s): {win_mentions}\nCongratulations on winning **{data['prize']}**!",
                reference=msg
            )
        except Exception:
            await ch.send(f"🎉 **Reroll!** New winner(s): {win_mentions} — **{data['prize']}**!")

    e = ok("Rerolled!", f"New winner(s): {win_mentions}")
    await interaction.followup.send(embed=e, ephemeral=True)


@bot.tree.command(name="glist", description="List all active giveaways in this server")
async def glist(interaction: discord.Interaction):
    active = [
        (mid, d) for mid, d in giveaways.items()
        if not d.get("ended") and bot.get_channel(d["channel_id"]) and
        bot.get_channel(d["channel_id"]).guild.id == interaction.guild.id
    ]
    if not active:
        return await interaction.response.send_message(
            embed=_base("🎉  Giveaways", "No active giveaways right now.", 0xFFD700), ephemeral=True
        )
    e = discord.Embed(title="🎉  Active Giveaways", color=0xFFD700, timestamp=datetime.utcnow())
    for mid, d in active[:10]:
        ch = bot.get_channel(d["channel_id"])
        e.add_field(
            name=d["prize"],
            value=(
                f"Channel: {ch.mention}\n"
                f"Ends: <t:{int(d['ends_at'])}:R>\n"
                f"Winners: `{d['winners']}` · Entries: `{len(d.get('entries', []))}`\n"
                f"[Jump](https://discord.com/channels/{interaction.guild.id}/{d['channel_id']}/{mid})"
            ),
            inline=True
        )
    ft(e, "Crimson Gen • Giveaways")
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="gbonus", description="Set bonus entries for a role in a giveaway")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(message_id="Giveaway message ID", role="Role to give bonus entries", entries="Extra entries (0 to remove)")
async def gbonus(interaction: discord.Interaction, message_id: str, role: discord.Role, entries: int):
    mid  = int(message_id) if message_id.isdigit() else None
    data = giveaways.get(mid)
    if not data or data.get("ended"):
        return await interaction.response.send_message(embed=err("Not Found", "No active giveaway with that ID."), ephemeral=True)
    if entries <= 0:
        data["bonus_entries"].pop(role.id, None)
        msg = f"Removed bonus entries for {role.mention}."
    else:
        data["bonus_entries"][role.id] = entries
        msg = f"Set **+{entries}** bonus entries for {role.mention}."
    # Update embed
    ch = bot.get_channel(data["channel_id"])
    if ch:
        try:
            gaw_msg = await ch.fetch_message(mid)
            await gaw_msg.edit(embed=giveaway_embed(data))
        except Exception: pass
    await interaction.response.send_message(embed=ok("Bonus Updated", msg), ephemeral=True)


@bot.tree.command(name="gcancel", description="Cancel and delete a giveaway")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(message_id="Giveaway message ID")
async def gcancel(interaction: discord.Interaction, message_id: str):
    await interaction.response.defer(ephemeral=True)
    mid  = int(message_id) if message_id.isdigit() else None
    data = giveaways.get(mid)
    if not data:
        return await interaction.followup.send(embed=err("Not Found", "No giveaway with that ID."), ephemeral=True)
    data["ended"] = True
    ch = bot.get_channel(data["channel_id"])
    if ch:
        try:
            gaw_msg = await ch.fetch_message(mid)
            await gaw_msg.delete()
        except Exception: pass
    giveaways.pop(mid, None)
    await interaction.followup.send(embed=ok("Cancelled", "Giveaway has been cancelled and deleted."), ephemeral=True)


# ── Giveaway reaction handler ─────────────────────────────
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if str(payload.emoji) != "🎉": return
    data = giveaways.get(payload.message_id)
    if not data or data.get("ended"): return
    if payload.user_id == bot.user.id: return

    guild   = bot.get_guild(payload.guild_id)
    member  = guild.get_member(payload.user_id) if guild else None
    if not member or member.bot: return

    # Check required role
    req_role = data.get("req_role")
    if req_role and not any(r.id == req_role for r in member.roles):
        ch = bot.get_channel(payload.channel_id)
        if ch:
            try:
                msg = await ch.fetch_message(payload.message_id)
                await msg.remove_reaction("🎉", member)
            except Exception: pass
        return

    if payload.user_id not in data["entries"]:
        data["entries"].append(payload.user_id)
        # Update entry count on embed
        ch = bot.get_channel(payload.channel_id)
        if ch:
            try:
                msg = await ch.fetch_message(payload.message_id)
                await msg.edit(embed=giveaway_embed(data))
            except Exception: pass

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if str(payload.emoji) != "🎉": return
    data = giveaways.get(payload.message_id)
    if not data or data.get("ended"): return
    if payload.user_id in data["entries"]:
        data["entries"].remove(payload.user_id)
        ch = bot.get_channel(payload.channel_id)
        if ch:
            try:
                msg = await ch.fetch_message(payload.message_id)
                await msg.edit(embed=giveaway_embed(data))
            except Exception: pass


bot.run(TOKEN)
