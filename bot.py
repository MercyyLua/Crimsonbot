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
            "filter_links":     True,    # NOW ON BY DEFAULT
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

def is_staff(member: discord.Member) -> bool:
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
        print(f"✅  Synced {len(synced)} commands as {bot.user}")
    except Exception as e:
        print(f"❌  Sync failed: {e}")
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.watching, name="over the server"),
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

    await bot.process_commands(msg)

# ══════════════════════════════════════════════════════════
#  WELCOME / LEAVE
# ══════════════════════════════════════════════════════════
@bot.event
async def on_member_join(member: discord.Member):
    get_stats(member.guild.id)["joins"] += 1

    # ── Antinuke bot-add check ──
    if member.bot and an_on(member.guild.id):
        guild = member.guild
        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.bot_add):
            adder = entry.user
            if adder.bot or an_wl(guild.id, adder.id): break
            try: await guild.ban(member, reason="[ANTINUKE] Unauthorised bot added", delete_message_days=0)
            except Exception: pass
            adder_m = guild.get_member(adder.id)
            if adder_m: await an_punish(guild, adder_m, "Unauthorised Bot Addition", f"Bot: {member} ({member.id})")
            log_id = antinuke_log_channels.get(guild.id)
            if log_id and (ch := guild.get_channel(log_id)):
                e = _base("🤖  Antinuke — Unauthorised Bot Blocked", color=C_ERROR)
                e.set_thumbnail(url=member.display_avatar.url)
                e.add_field(name="🤖 Bot Added",  value=f"{member.mention}\n`{member.id}`", inline=True)
                e.add_field(name="👤 Added By",   value=f"{adder.mention}\n`{adder.id}`",   inline=True)
                e.add_field(name="⚙️ Action",     value="Bot banned • Adder banned",         inline=False)
                ft(e, "Crimson Gen • Antinuke")
                await ch.send(embed=e)
            break
        return

    if not WELCOME_ENABLED: return
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch: return
    msg_text = random.choice(WELCOME_MESSAGES).format(mention=member.mention, name=member.name)
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
    await ch.send(content=msg_text, embed=e)

@bot.event
async def on_member_remove(member: discord.Member):
    get_stats(member.guild.id)["leaves"] += 1
    if not LEAVE_ENABLED: return
    ch = bot.get_channel(WELCOME_CHANNEL_ID)
    if not ch: return
    msg_text = random.choice(LEAVE_MESSAGES).format(name=member.name)
    e = _base("👋  Member Left", color=C_ERROR)
    e.description = (
        f"**{member.name}** has left the server.\n\n"
        f"📥 **Joined:** <t:{int(member.joined_at.timestamp())}:R>\n"
        f"👥 **Members now:** {member.guild.member_count}"
    )
    e.set_thumbnail(url=member.display_avatar.url)
    ft(e, f"{member.guild.name} • Goodbye", member.guild.icon.url if member.guild.icon else None)
    await ch.send(content=msg_text, embed=e)

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
class TicketCategorySelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="General Support", description="General questions and help",  emoji="❓"),
            discord.SelectOption(label="Report User",     description="Report rule violations",       emoji="🚨"),
            discord.SelectOption(label="Partnership",     description="Partnership inquiries",         emoji="🤝"),
            discord.SelectOption(label="Bug Report",      description="Report bugs or issues",         emoji="🐛"),
            discord.SelectOption(label="Other",           description="Other inquiries",               emoji="📝"),
        ]
        super().__init__(placeholder="Select ticket category...", options=options, custom_id="ticket_category_select")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        global ticket_counter
        ticket_counter += 1

        selected_category = self.values[0]
        guild = interaction.guild
        category = discord.utils.get(guild.categories, name="Tickets")
        staff_role = discord.utils.get(guild.roles, name=STAFF_ROLE_NAME)

        if not category:
            category = await guild.create_category("Tickets")

        channel = await guild.create_text_channel(
            f"ticket-{str(ticket_counter).zfill(4)}",
            category=category
        )

        await channel.set_permissions(guild.default_role, read_messages=False)
        await channel.set_permissions(interaction.user, read_messages=True, send_messages=True)
        if staff_role:
            await channel.set_permissions(staff_role, read_messages=True, send_messages=True)

        welcome_text = f"👋 {interaction.user.mention}, welcome to your ticket!"

        embed = discord.Embed(
            title=f"🎟️ Support Ticket #{str(ticket_counter).zfill(4)}",
            description=(
                f"**Status:** 🟢 Open\n"
                f"**Category:** {selected_category}\n"
                f"**Created:** <t:{int(datetime.utcnow().timestamp())}:F>\n"
                f"**Ticket ID:** `{str(ticket_counter).zfill(4)}`\n\n"
                f"👤 **Creator Information**\n"
                f"• User: {interaction.user.mention}\n"
                f"• ID: `{interaction.user.id}`\n"
                f"• Joined: <t:{int(interaction.user.joined_at.timestamp())}:R>\n\n"
                f"🛡️ **Quick Actions**\n"
                f"• 🔍 View - Show ticket details\n"
                f"• ✏️ Rename - Change ticket name\n"
                f"• 👥 Users - Manage access\n"
                f"• ⭐ Claim - Handle ticket\n"
                f"• 🔒 Close - Close ticket\n"
                f"• 🗑️ Delete - Remove ticket\n\n"
                f"ℹ️ **Please describe your issue below and staff will assist you shortly.**"
            ),
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Created by {interaction.user.name}", icon_url=interaction.user.display_avatar.url)
        embed.set_image(url=TICKET_BANNER)

        await channel.send(content=welcome_text, embed=embed, view=TicketActionsView())

        info_embed = discord.Embed(title="ℹ️ Ticket Information", color=discord.Color.blue())
        info_embed.add_field(name="Creator",  value=f"{interaction.user.mention}\n`{interaction.user.id}`", inline=True)
        info_embed.add_field(name="Category", value=selected_category,                                       inline=True)
        info_embed.add_field(name="Ticket #", value=f"#{str(ticket_counter).zfill(4)}",                      inline=True)
        info_embed.add_field(name="Channel Age", value="0d 0h 0m",                                           inline=True)
        info_embed.add_field(name="Status",   value="🟢 Open",                                               inline=True)
        info_embed.add_field(name="\u200b",  value="\u200b",                                               inline=True)

        time_str = datetime.utcnow().strftime("%H:%M")
        info_embed.set_footer(
            text=f"Created by {interaction.user.name} | vandaag om {time_str}",
            icon_url=interaction.user.display_avatar.url
        )

        if staff_role:
            await channel.send(embed=info_embed, view=StaffTicketInfoView())

        ticket_creators[channel.id] = interaction.user.id
        await interaction.followup.send(f"✅ Your ticket has been created: {channel.mention}", ephemeral=True)


class StaffTicketInfoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Add User",    style=discord.ButtonStyle.green, custom_id="staff_add_user")
    async def add_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if not staff_role or staff_role not in interaction.user.roles:
            await interaction.response.send_message("❌ Only staff can use this button.", ephemeral=True)
            return
        await interaction.response.send_modal(ManageUsersModal())

    @discord.ui.button(label="Remove User", style=discord.ButtonStyle.red,   custom_id="staff_remove_user")
    async def remove_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if not staff_role or staff_role not in interaction.user.roles:
            await interaction.response.send_message("❌ Only staff can use this button.", ephemeral=True)
            return
        await interaction.response.send_modal(RemoveUserModal())


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketCategorySelect())


class TicketActionsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="View Info",    emoji="🔍", style=discord.ButtonStyle.gray,    custom_id="ticket_view",   row=0)
    async def view_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"📊 **Ticket Information**\n"
            f"Channel: {interaction.channel.mention}\n"
            f"Created: <t:{int(interaction.channel.created_at.timestamp())}:R>",
            ephemeral=True
        )

    @discord.ui.button(label="Rename",       emoji="✏️",  style=discord.ButtonStyle.primary, custom_id="ticket_rename", row=0)
    async def rename(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        if not is_staff_user:
            await interaction.response.send_message("❌ Only staff or support roles can rename tickets!", ephemeral=True)
            return
        await interaction.response.send_modal(RenameTicketModal())

    @discord.ui.button(label="Close",        emoji="🔒", style=discord.ButtonStyle.red,     custom_id="ticket_close",  row=0)
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        is_creator = interaction.channel.overwrites_for(interaction.user).read_messages is True
        if not is_staff_user and not is_creator:
            await interaction.response.send_message("❌ Only staff or the ticket creator can close tickets!", ephemeral=True)
            return
        await interaction.response.send_modal(CloseReasonModal())

    @discord.ui.button(label="Manage Users", emoji="👥", style=discord.ButtonStyle.gray,    custom_id="ticket_users",  row=1)
    async def manage_users(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        if not is_staff_user:
            await interaction.response.send_message("❌ Only staff or support roles can manage users!", ephemeral=True)
            return
        await interaction.response.send_modal(ManageUsersModal())

    @discord.ui.button(label="Claim",        emoji="⭐", style=discord.ButtonStyle.green,   custom_id="ticket_claim",  row=1)
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        if not is_staff_user:
            await interaction.response.send_message("❌ Only staff or support roles can claim tickets!", ephemeral=True)
            return
        await interaction.response.send_message(f"⭐ {interaction.user.mention} has claimed this ticket!")

    @discord.ui.button(label="Delete",       emoji="🗑️", style=discord.ButtonStyle.red,     custom_id="ticket_delete", row=1)
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        if not is_staff_user:
            await interaction.response.send_message("❌ Only administrators or support roles can delete tickets!", ephemeral=True)
            return
        await interaction.response.send_message("🗑️ Deleting ticket in 5 seconds...")
        await asyncio.sleep(5)
        await interaction.channel.delete()


async def generate_transcript(channel: discord.TextChannel) -> discord.File:
    """Scrape all messages and build an HTML transcript file."""
    messages = []
    async for msg in channel.history(limit=2000, oldest_first=True):
        messages.append(msg)

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows = ""
    for msg in messages:
        ts   = msg.created_at.strftime("%H:%M")
        name = discord.utils.escape_markdown(msg.author.display_name)
        bot_badge = " <span class='bot'>BOT</span>" if msg.author.bot else ""
        content = discord.utils.escape_mentions(msg.content or "")
        # Basic markdown-ish escaping for HTML
        content = content.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\n","<br>")
        attachments = ""
        for a in msg.attachments:
            if any(a.filename.lower().endswith(x) for x in [".png",".jpg",".jpeg",".gif",".webp"]):
                attachments += f'<br><img src="{a.url}" style="max-width:400px;max-height:300px;border-radius:8px;margin-top:6px;">'
            else:
                attachments += f'<br><a href="{a.url}" target="_blank">📎 {a.filename}</a>'
        embeds = ""
        for e in msg.embeds:
            etitle = (e.title or "").replace("&","&amp;").replace("<","&lt;")
            edesc  = (e.description or "").replace("&","&amp;").replace("<","&lt;").replace("\n","<br>")
            embeds += f'<div class="embed"><strong>{etitle}</strong><br>{edesc}</div>'
        rows += f"""
        <div class="msg">
            <img class="avatar" src="{msg.author.display_avatar.url}" onerror="this.style.display='none'">
            <div class="msg-body">
                <span class="author">{name}{bot_badge}</span>
                <span class="ts">{ts}</span>
                <div class="content">{content}{attachments}{embeds}</div>
            </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Transcript — {channel.name}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #1e1f22; color: #dcddde; font-family: 'Segoe UI', sans-serif; font-size: 14px; }}
  .header {{ background: #2b2d31; padding: 20px 30px; border-bottom: 2px solid #dc143c; }}
  .header h1 {{ color: #dc143c; font-size: 20px; }}
  .header p  {{ color: #96989d; font-size: 12px; margin-top: 4px; }}
  .messages {{ padding: 20px 30px; }}
  .msg {{ display: flex; gap: 14px; padding: 6px 0; }}
  .msg:hover {{ background: #2b2d31; border-radius: 8px; padding: 6px 10px; }}
  .avatar {{ width: 38px; height: 38px; border-radius: 50%; flex-shrink: 0; margin-top: 2px; }}
  .msg-body {{ flex: 1; }}
  .author {{ color: #fff; font-weight: 600; font-size: 14px; }}
  .bot {{ background: #5865f2; color: #fff; font-size: 10px; padding: 1px 5px; border-radius: 4px; margin-left: 5px; vertical-align: middle; }}
  .ts {{ color: #72767d; font-size: 11px; margin-left: 8px; }}
  .content {{ color: #dcddde; margin-top: 2px; line-height: 1.5; word-break: break-word; }}
  .embed {{ background: #2b2d31; border-left: 4px solid #5865f2; padding: 8px 12px; border-radius: 4px; margin-top: 6px; }}
  a {{ color: #00aff4; }}
  .footer {{ text-align: center; padding: 20px; color: #72767d; font-size: 11px; border-top: 1px solid #2b2d31; margin-top: 20px; }}
</style>
</head>
<body>
<div class="header">
  <h1>📋 Ticket Transcript — #{channel.name}</h1>
  <p>Generated: {now_str}  ·  {len(messages)} messages  ·  Crimson Gen</p>
</div>
<div class="messages">{rows}</div>
<div class="footer">Crimson Gen Ticket System  ·  {now_str}</div>
</body>
</html>"""

    buf = html.encode("utf-8")
    import io
    return discord.File(io.BytesIO(buf), filename=f"transcript-{channel.name}.html")


class ClosedTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Reopen", emoji="🔓", style=discord.ButtonStyle.green, custom_id="ticket_reopen_btn")
    async def reopen_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for member in interaction.channel.members:
            await interaction.channel.set_permissions(member, send_messages=True, read_messages=True)
        embed = discord.Embed(
            title="🔓 Ticket Reopened",
            description=f"This ticket has been reopened by {interaction.user.mention}",
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )
        await interaction.channel.send(embed=embed, view=TicketActionsView())

    @discord.ui.button(label="Transcript", emoji="📋", style=discord.ButtonStyle.primary, custom_id="ticket_transcript_btn")
    async def save_transcript(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        if not is_staff_user:
            return await interaction.response.send_message("❌ Only staff can save transcripts.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            transcript = await generate_transcript(interaction.channel)
            # Send to user ephemerally
            await interaction.followup.send(
                embed=discord.Embed(
                    description=f"📋 Transcript for **{interaction.channel.name}** generated!",
                    color=0x5865F2
                ),
                file=transcript,
                ephemeral=True
            )
            # Also save to transcript channel if set
            ch_id = ticket_transcript_channels.get(interaction.guild.id)
            if ch_id and (log_ch := interaction.guild.get_channel(ch_id)):
                transcript2 = await generate_transcript(interaction.channel)
                e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
                e.description = (
                    f"## 📋  Ticket Transcript\n"
                    f"> **Channel:** {interaction.channel.name}\n"
                    f"> **Saved by:** {interaction.user.mention}\n"
                    f"> **Closed:** <t:{int(datetime.utcnow().timestamp())}:F>"
                )
                ft(e, "Crimson Gen • Tickets", interaction.guild.me.display_avatar.url)
                await log_ch.send(embed=e, file=transcript2)
        except Exception as ex:
            await interaction.followup.send(f"❌ Failed to generate transcript: `{ex}`", ephemeral=True)

    @discord.ui.button(label="Delete", emoji="🗑️", style=discord.ButtonStyle.red, custom_id="ticket_delete_btn")
    async def delete_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        is_staff_user = (staff_role and staff_role in interaction.user.roles) or interaction.user.guild_permissions.administrator
        if not is_staff_user:
            return await interaction.response.send_message("❌ Only staff can delete tickets.", ephemeral=True)
        await interaction.response.send_message("🗑️ Deleting ticket in 3 seconds...")
        await asyncio.sleep(3)
        await interaction.channel.delete()


class CloseReasonModal(discord.ui.Modal, title="Close Ticket"):
    reason = discord.ui.TextInput(
        label="Why do you want to close this ticket?",
        style=discord.TextStyle.paragraph,
        placeholder="Enter your reason here...",
        required=True,
        max_length=500
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        await interaction.channel.set_permissions(interaction.guild.default_role, send_messages=False, read_messages=False)
        for member in interaction.channel.members:
            if staff_role and staff_role not in member.roles:
                await interaction.channel.set_permissions(member, send_messages=False)

        # ── Generate transcript ───────────────────────────────
        transcript_file  = None
        transcript_file2 = None
        try:
            transcript_file  = await generate_transcript(interaction.channel)
            transcript_file2 = await generate_transcript(interaction.channel)
        except Exception:
            pass

        # ── Close embed ───────────────────────────────────────
        embed = discord.Embed(color=discord.Color.red(), timestamp=datetime.utcnow())
        embed.description = (
            f"## 🔒  Ticket Closed\n"
            f"> Closed by {interaction.user.mention}\n"
            f"> <t:{int(datetime.utcnow().timestamp())}:F>"
        )
        embed.add_field(name="📝  Reason", value=f"```{self.reason.value}```", inline=False)
        embed.set_footer(text="Use the buttons below to reopen, view transcript, or delete.")

        content = staff_role.mention if staff_role else None
        if transcript_file:
            await interaction.channel.send(content=content, embed=embed, file=transcript_file, view=ClosedTicketView())
        else:
            await interaction.channel.send(content=content, embed=embed, view=ClosedTicketView())

        # ── DM transcript to ticket creator ───────────────────
        creator_id = ticket_creators.get(interaction.channel.id)
        if creator_id and transcript_file2:
            try:
                creator = interaction.guild.get_member(creator_id)
                if creator:
                    dm_e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
                    dm_e.description = (
                        f"## 📋  Your Ticket Transcript\n"
                        f"> **Server:** {interaction.guild.name}\n"
                        f"> **Ticket:** `{interaction.channel.name}`\n"
                        f"> **Closed by:** {interaction.user.display_name}\n"
                        f"> **Reason:** {self.reason.value}\n\n"
                        f"Your ticket transcript is attached below. Open it in any browser to view the full conversation."
                    )
                    dm_e.set_thumbnail(url=interaction.guild.icon.url if interaction.guild.icon else None)
                    ft(dm_e, "Crimson Gen • Tickets")
                    transcript_file2 = await generate_transcript(interaction.channel)
                    await creator.send(embed=dm_e, file=transcript_file2)
            except Exception:
                pass  # DMs may be closed, silently skip

        # ── Auto-send to transcript log channel ───────────────
        ch_id = ticket_transcript_channels.get(interaction.guild.id)
        if ch_id and (log_ch := interaction.guild.get_channel(ch_id)):
            try:
                transcript_log = await generate_transcript(interaction.channel)
                log_e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
                log_e.description = (
                    f"## 📋  Ticket Transcript\n"
                    f"> **Channel:** `{interaction.channel.name}`\n"
                    f"> **Closed by:** {interaction.user.mention}\n"
                    f"> **Reason:** {self.reason.value}\n"
                    f"> **Time:** <t:{int(datetime.utcnow().timestamp())}:F>"
                )
                ft(log_e, "Crimson Gen • Tickets", interaction.guild.me.display_avatar.url)
                await log_ch.send(embed=log_e, file=transcript_log)
            except Exception:
                pass


class RenameTicketModal(discord.ui.Modal, title="Rename Ticket"):
    new_name = discord.ui.TextInput(
        label="New ticket name",
        style=discord.TextStyle.short,
        placeholder="Enter new name (e.g., payment-issue)",
        required=True,
        max_length=50
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        clean_name = self.new_name.value.lower().replace(" ", "-")
        clean_name = "".join(c for c in clean_name if c.isalnum() or c == "-")
        try:
            await interaction.channel.edit(name=f"ticket-{clean_name}")
            await interaction.followup.send(f"✅ Ticket renamed to: `ticket-{clean_name}`", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to rename ticket: {str(e)}", ephemeral=True)


class ManageUsersModal(discord.ui.Modal, title="Manage Ticket Users"):
    user_input = discord.ui.TextInput(
        label="Add or Remove User",
        style=discord.TextStyle.short,
        placeholder="@username or User ID (prefix with - to remove)",
        required=True,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user_text = self.user_input.value.strip()
        remove_user = user_text.startswith("-")
        if remove_user:
            user_text = user_text[1:].strip()

        user = None
        user_text = user_text.replace("@", "").replace("<", "").replace(">", "").replace("!", "")

        if user_text.isdigit():
            try: user = await interaction.guild.fetch_member(int(user_text))
            except: pass

        if not user:
            user = discord.utils.get(interaction.guild.members, name=user_text)
        if not user:
            user = discord.utils.get(interaction.guild.members, display_name=user_text)

        if not user:
            await interaction.followup.send(f"❌ Could not find user: `{self.user_input.value}`\nTry using their User ID or @mention", ephemeral=True)
            return

        try:
            if remove_user:
                await interaction.channel.set_permissions(user, overwrite=None)
                await interaction.followup.send(f"✅ Removed {user.mention} from this ticket", ephemeral=True)
            else:
                await interaction.channel.set_permissions(user, read_messages=True, send_messages=True)
                await interaction.followup.send(f"✅ Added {user.mention} to this ticket", ephemeral=True)
                await interaction.channel.send(f"👥 {user.mention} has been added to this ticket by {interaction.user.mention}")
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to manage user: {str(e)}", ephemeral=True)


class RemoveUserModal(discord.ui.Modal, title="Remove User from Ticket"):
    user_input = discord.ui.TextInput(
        label="User to Remove",
        style=discord.TextStyle.short,
        placeholder="@username or User ID",
        required=True,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user_text = self.user_input.value.strip()
        user = None
        user_text = user_text.replace("@", "").replace("<", "").replace(">", "").replace("!", "")

        if user_text.isdigit():
            try: user = await interaction.guild.fetch_member(int(user_text))
            except: pass

        if not user:
            user = discord.utils.get(interaction.guild.members, name=user_text)
        if not user:
            user = discord.utils.get(interaction.guild.members, display_name=user_text)

        if not user:
            await interaction.followup.send(f"❌ Could not find user: `{self.user_input.value}`", ephemeral=True)
            return

        try:
            await interaction.channel.set_permissions(user, overwrite=None)
            await interaction.followup.send(f"✅ Removed {user.mention} from this ticket", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to remove user: {str(e)}", ephemeral=True)


# ── Ticket Commands ──
@bot.tree.command(name="panel", description="Send the support ticket panel")
@app_commands.checks.has_permissions(administrator=True)
async def panel(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🎟️ Support Tickets",
        description="**Need help?** Select a category below!",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="",
        value=(
            "✅ Fast response time\n"
            "✅ Professional support\n"
            "✅ Private conversation\n"
            "✅ Multiple categories"
        ),
        inline=False
    )
    embed.set_image(url=TICKET_BANNER)
    embed.set_footer(text=f"{interaction.guild.name} Support Team", icon_url=interaction.guild.icon.url if interaction.guild.icon else None)
    await interaction.channel.send(embed=embed, view=TicketPanelView())
    await interaction.response.send_message("✅ Ticket panel sent.", ephemeral=True)

@bot.tree.command(name="close", description="Close this ticket")
async def close(interaction: discord.Interaction):
    if not interaction.channel.name.startswith("ticket-"):
        await interaction.response.send_message("❌ This command can only be used in ticket channels!", ephemeral=True)
        return
    await interaction.response.send_modal(CloseReasonModal())

@bot.tree.command(name="deleteticket", description="Forcefully delete a ticket channel")
@app_commands.checks.has_permissions(manage_channels=True)
async def deleteticket(interaction: discord.Interaction):
    if not interaction.channel.name.startswith("ticket-"):
        await interaction.response.send_message("❌ This command can only be used in ticket channels!", ephemeral=True)
        return
    await interaction.response.send_message("🗑️ **Force deleting this ticket in 3 seconds...**\nThis action cannot be undone!")
    await asyncio.sleep(3)
    try:
        await interaction.channel.delete(reason=f"Ticket forcefully deleted by {interaction.user}")
    except discord.Forbidden:
        await interaction.followup.send("❌ I don't have permission to delete this channel!", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to delete ticket: {str(e)}", ephemeral=True)

@bot.tree.command(name="ticket_transcriptchannel", description="Set or clear the channel where ticket transcripts are saved")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(channel="Channel to save transcripts to (leave empty to clear)")
async def ticket_transcriptchannel(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if channel:
        ticket_transcript_channels[interaction.guild.id] = channel.id
        e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
        e.description = (
            f"## ✅  Transcript Channel Set\n"
            f"> Ticket transcripts will automatically be sent to {channel.mention} whenever a ticket is closed.\n"
            f"> Staff can also manually save transcripts using the 📋 **Transcript** button on closed tickets."
        )
    else:
        ticket_transcript_channels.pop(interaction.guild.id, None)
        e = discord.Embed(color=C_WARNING, timestamp=datetime.utcnow())
        e.description = (
            f"## ✅  Transcript Channel Cleared\n"
            f"> Transcripts will no longer be auto-saved.\n"
            f"> Staff can still generate them manually using the 📋 **Transcript** button."
        )
    ft(e, "Crimson Gen • Tickets", interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="ticket_transcript", description="Manually generate a transcript for the current ticket")
@app_commands.checks.has_permissions(manage_channels=True)
async def ticket_transcript(interaction: discord.Interaction):
    if not interaction.channel.name.startswith("ticket-"):
        return await interaction.response.send_message("❌ This command can only be used in ticket channels!", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        transcript = await generate_transcript(interaction.channel)
        e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
        e.description = f"## 📋  Transcript Generated\n> **Channel:** `{interaction.channel.name}`"
        ft(e, "Crimson Gen • Tickets", interaction.guild.me.display_avatar.url)
        await interaction.followup.send(embed=e, file=transcript, ephemeral=True)

        # Also post to transcript log channel if set
        ch_id = ticket_transcript_channels.get(interaction.guild.id)
        if ch_id and (log_ch := interaction.guild.get_channel(ch_id)):
            transcript2 = await generate_transcript(interaction.channel)
            log_e = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
            log_e.description = (
                f"## 📋  Ticket Transcript\n"
                f"> **Channel:** `{interaction.channel.name}`\n"
                f"> **Generated by:** {interaction.user.mention}\n"
                f"> **Time:** <t:{int(datetime.utcnow().timestamp())}:F>"
            )
            ft(log_e, "Crimson Gen • Tickets", interaction.guild.me.display_avatar.url)
            await log_ch.send(embed=log_e, file=transcript2)
    except Exception as ex:
        await interaction.followup.send(f"❌ Failed to generate transcript: `{ex}`", ephemeral=True)

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

@bot.tree.command(name="purge", description="Bulk delete messages in the current channel")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.describe(amount="Number of messages to delete (1–100)")
async def purge(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(ephemeral=True)
    if not 1 <= amount <= 100: return await interaction.followup.send(embed=err("Invalid Amount", "Choose between 1 and 100."), ephemeral=True)
    deleted = await interaction.channel.purge(limit=amount)
    await interaction.followup.send(embed=ok("Purged", f"Deleted **{len(deleted)}** message(s)."), ephemeral=True)

@bot.tree.command(name="clear", description="Alias for /purge")
@app_commands.checks.has_permissions(manage_messages=True)
@app_commands.describe(amount="Number of messages to delete")
async def clear(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=amount)
    await interaction.followup.send(embed=ok("Cleared", f"Deleted **{len(deleted)}** message(s)."), ephemeral=True)

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
    now = int(datetime.utcnow().timestamp())
    e   = discord.Embed(color=C_CRIMSON, timestamp=datetime.utcnow())
    e.description = f"## 📢  {title}\n\n{message}"
    e.set_author(
        name=interaction.guild.name,
        icon_url=interaction.guild.icon.url if interaction.guild.icon else None
    )
    e.add_field(name="📅  Posted", value=f"<t:{now}:F>",                  inline=True)
    e.add_field(name="📣  By",     value=interaction.user.mention,          inline=True)
    e.set_footer(
        text=f"Announcement  ·  {interaction.guild.name}",
        icon_url=bot.user.display_avatar.url
    )
    content = ping.mention if ping else None
    await channel.send(content=content, embed=e)
    conf = _base("", color=C_SUCCESS)
    conf.description = f"✅  Announcement posted in {channel.mention}"
    await interaction.response.send_message(embed=conf, ephemeral=True)

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
    bot.tree.clear_commands(guild=None)
    bot.tree.clear_commands(guild=interaction.guild)
    await asyncio.sleep(1)
    synced       = await bot.tree.sync()
    guild_synced = await bot.tree.sync(guild=interaction.guild)
    e = ok("Commands Resynced", f"Synced **{len(synced)}** global + **{len(guild_synced)}** guild commands.")
    e.set_footer(text="May take up to 10 minutes to update globally.")
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
@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if not an_on(guild.id): return
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        if entry.user.bot or an_wl(guild.id, entry.user.id): return
        if an_track(guild.id, entry.user.id):
            m = guild.get_member(entry.user.id)
            if m: await an_punish(guild, m, "Mass Channel Deletion", f"Deleted: #{channel.name}")

@bot.event
async def on_guild_role_delete(role: discord.Role):
    guild = role.guild
    if not an_on(guild.id): return
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
        if entry.user.bot or an_wl(guild.id, entry.user.id): return
        if an_track(guild.id, entry.user.id):
            m = guild.get_member(entry.user.id)
            if m: await an_punish(guild, m, "Mass Role Deletion", f"Deleted role: {role.name}")

@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    if not an_on(guild.id): return
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
        if entry.user.bot or an_wl(guild.id, entry.user.id): return
        if an_track(guild.id, entry.user.id):
            m = guild.get_member(entry.user.id)
            if m: await an_punish(guild, m, "Mass Banning", f"Banned: {user}")

@bot.event
async def on_guild_update(before: discord.Guild, after: discord.Guild):
    if not an_on(after.id): return
    async for entry in after.audit_logs(limit=1, action=discord.AuditLogAction.guild_update):
        if entry.user.bot or an_wl(after.id, entry.user.id): return
        if an_track(after.id, entry.user.id):
            m = after.get_member(entry.user.id)
            if m: await an_punish(after, m, "Suspicious Server Update", "")

@bot.event
async def on_webhooks_update(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if not an_on(guild.id): return
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
        if entry.user.bot or an_wl(guild.id, entry.user.id): return
        if an_track(guild.id, entry.user.id):
            m = guild.get_member(entry.user.id)
            if m: await an_punish(guild, m, "Mass Webhook Creation", "")

@bot.event
async def on_guild_role_create(role: discord.Role):
    guild = role.guild
    if not an_on(guild.id): return
    dangerous = discord.Permissions(administrator=True) | discord.Permissions(manage_guild=True)
    if not (role.permissions.value & dangerous.value): return
    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
        if entry.user.bot or an_wl(guild.id, entry.user.id): return
        if an_track(guild.id, entry.user.id):
            m = guild.get_member(entry.user.id)
            if m: await an_punish(guild, m, "Dangerous Role Created", f"Role: {role.name} (admin/manage perms)")

# ── Antinuke Commands ──
@bot.tree.command(name="antinuke", description="Toggle antinuke server protection")
@app_commands.checks.has_permissions(administrator=True)
async def antinuke(interaction: discord.Interaction):
    gid = interaction.guild.id
    now_on = not antinuke_enabled.get(gid, False)
    antinuke_enabled[gid] = now_on
    log_id = antinuke_log_channels.get(gid)
    e = _base("🛡️  Antinuke System",
              f"Protection is now **{'🟢 ENABLED' if now_on else '🔴 DISABLED'}**",
              C_SUCCESS if now_on else C_ERROR)
    e.add_field(
        name="🔒  Active Protections",
        value=(
            "› Mass channel deletion\n"
            "› Mass role deletion\n"
            "› Mass bans\n"
            "› Mass webhook creation\n"
            "› Dangerous role creation\n"
            "› Suspicious server updates\n"
            "› Unauthorised bot additions"
        ),
        inline=True
    )
    e.add_field(
        name="⚙️  Configuration",
        value=(
            f"Threshold: `{AN_THRESHOLD}` actions\n"
            f"Window: `{AN_TIMEFRAME}s`\n"
            f"Punishment: Derole + Ban\n"
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
    e.add_field(name="⚡ Status",       value="🟢 Enabled"   if enabled else "🔴 Disabled",        inline=True)
    e.add_field(name="📋 Log Channel",  value=f"<#{log_id}>" if log_id  else "Not set",              inline=True)
    e.add_field(name="⚙️ Threshold",   value=f"`{AN_THRESHOLD}` actions / `{AN_TIMEFRAME}s`",        inline=True)
    e.add_field(name="⚠️ Punishment",   value="Roles stripped → Banned",                             inline=True)
    e.add_field(name="📬 DM on Ban",    value="✅ Yes",                                               inline=True)
    e.add_field(name="\u200b",          value="\u200b",                                               inline=True)
    wl_str = "\n".join(f"› <@{uid}>" for uid in wl) or "No whitelisted users."
    e.add_field(name=f"✅  Whitelist ({len(wl)} users)", value=wl_str, inline=False)
    ft(e, "Crimson Gen • Antinuke", interaction.guild.me.display_avatar.url)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="antinuke_whitelist", description="Add or remove a user from the antinuke whitelist")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(user="User to whitelist or unwhitelist", action="Add or remove")
@app_commands.choices(action=[
    app_commands.Choice(name="✅ Add to whitelist",       value="add"),
    app_commands.Choice(name="❌ Remove from whitelist",  value="remove"),
])
async def antinuke_whitelist_cmd(interaction: discord.Interaction, user: discord.Member, action: str):
    gid = interaction.guild.id
    antinuke_wl.setdefault(gid, [])
    if action == "add":
        if user.id not in antinuke_wl[gid]: antinuke_wl[gid].append(user.id)
        e = ok("Whitelisted", f"{user.mention} has been added to the antinuke whitelist.\nThey can now perform actions without triggering antinuke.")
    else:
        if user.id in antinuke_wl[gid]: antinuke_wl[gid].remove(user.id)
        e = ok("Removed from Whitelist", f"{user.mention} has been removed from the antinuke whitelist.")
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


@bot.tree.command(name="automod", description="View and manage the AutoMod dashboard")
@app_commands.checks.has_permissions(administrator=True)
async def automod_cmd(interaction: discord.Interaction):
    cfg = get_automod(interaction.guild.id)
    e   = _build_automod_embed(interaction.guild, cfg)
    await interaction.response.send_message(embed=e, ephemeral=True)


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

    cfg    = get_automod(interaction.guild.id)
    lines  = []

    if role:
        lst = cfg["whitelist_roles"]
        if action == "add":
            if role.id not in lst: lst.append(role.id)
            lines.append(f"✅ {role.mention} added to role whitelist")
        else:
            if role.id in lst: lst.remove(role.id)
            lines.append(f"❌ {role.mention} removed from role whitelist")

    if channel:
        lst = cfg["whitelist_channels"]
        if action == "add":
            if channel.id not in lst: lst.append(channel.id)
            lines.append(f"✅ {channel.mention} added to channel whitelist")
        else:
            if channel.id in lst: lst.remove(channel.id)
            lines.append(f"❌ {channel.mention} removed from channel whitelist")

    e = discord.Embed(color=C_SUCCESS, timestamp=datetime.utcnow())
    e.description = f"## ✅  Whitelist Updated\n> " + "\n> ".join(lines)
    e.add_field(
        name="📊  Current Whitelist",
        value=(
            f"🎭 **{len(cfg['whitelist_roles'])}** role(s)  ·  "
            f"💬 **{len(cfg['whitelist_channels'])}** channel(s)"
        ),
        inline=False
    )
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

@bot.tree.command(name="role", description="Add or remove a role from every member in the server")
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

    # Safety checks
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

    # Send progress message
    direction = "to" if action == "add" else "from"
    e = _base(f"⏳  {action_word} Role...",
              f"{action_word} {role.mention} {direction} **{len(members)}** members.\nThis may take a moment...",
              C_WARNING)
    ft(e, "Crimson Gen")
    await interaction.followup.send(embed=e, ephemeral=True)

    success = 0
    failed  = 0

    for member in members:
        try:
            if action == "add":
                if role not in member.roles:
                    await member.add_roles(role, reason=f"Mass role add by {interaction.user}")
            else:
                if role in member.roles:
                    await member.remove_roles(role, reason=f"Mass role remove by {interaction.user}")
            success += 1
        except Exception:
            failed += 1
        # Small delay to avoid rate limits
        await asyncio.sleep(0.3)

    # Final result
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
bot.run(TOKEN)
