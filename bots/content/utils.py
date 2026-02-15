import re
import asyncio
import logging
from telegram.error import BadRequest, Forbidden, RetryAfter
from telegram.ext import ExtBot
from telegram import ChatMember, ChatPermissions
from .database import list_members, delete_group_data, mark_member_deleted, get_registered_groups

logger = logging.getLogger(__name__)

def _extract_domains_from_text(text: str) -> list[str]:
    """
    Extrahiert Domain-Namen aus Text (https://example.com, www.example.com, example.com).
    Gibt Liste von eindeutigen Domain-Namen zurück (ohne http://, www., etc.).
    """
    if not text:
        return []
    
    domains = set()
    
    # Muster 1: https://domain.com oder http://domain.com
    url_pattern = r'https?://([^\s/?#]+)'
    for match in re.finditer(url_pattern, text):
        host = match.group(1).lower()
        # Entferne Port falls vorhanden
        if ':' in host:
            host = host.split(':')[0]
        if host:
            domains.add(host)
    
    # Muster 2: www.domain.com (ohne http)
    www_pattern = r'www\.([^\s/?#]+)'
    for match in re.finditer(www_pattern, text):
        host = match.group(0).lower()  # mit www.
        if ':' in host:
            host = host.split(':')[0]
        if host:
            domains.add(host)
    
    # Muster 3: plain domain.com (mit Punkt, ohne www/http)
    # Nur wenn es aussieht wie ein Domain (z.B. mindestens ein Punkt und alphanumerisch)
    plain_pattern = r'(?:^|\s)([a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]?\.[a-zA-Z]{2,})(?:\s|$|[/?#])'
    for match in re.finditer(plain_pattern, text):
        host = match.group(1).lower()
        if host and not host.startswith('www.'):  # www. wurde schon oben gehandhabt
            domains.add(host)
    
    return sorted(list(domains))

def _extract_domains_from_message(msg) -> list[str]:
    """
    Extrahiert Domains aus Text/Captions und aus URL-Entities (inkl. versteckter Links).
    """
    if not msg:
        return []

    domains = set()
    text = msg.text or ""
    caption = msg.caption or ""

    domains.update(_extract_domains_from_text(text))
    if caption:
        domains.update(_extract_domains_from_text(caption))

    for ent in (msg.entities or []):
        try:
            if getattr(ent, "type", "") == "text_link" and getattr(ent, "url", None):
                domains.update(_extract_domains_from_text(ent.url))
            elif getattr(ent, "type", "") == "url" and text:
                part = text[ent.offset: ent.offset + ent.length]
                domains.update(_extract_domains_from_text(part))
        except Exception:
            pass

    for ent in (msg.caption_entities or []):
        try:
            if getattr(ent, "type", "") == "text_link" and getattr(ent, "url", None):
                domains.update(_extract_domains_from_text(ent.url))
            elif getattr(ent, "type", "") == "url" and caption:
                part = caption[ent.offset: ent.offset + ent.length]
                domains.update(_extract_domains_from_text(part))
        except Exception:
            pass

    return sorted(domains)

def heuristic_link_risk(domains: list[str]) -> float:
    """
    Berechnet Risiko-Score (0.0-1.0) für eine Liste von Domains.
    Basiert auf Heuristiken:
    - Verdächtige TLDs (tk, ml, ga, cf, etc.) -> höheres Risiko
    - Sehr kurze/lange Domains -> Indiz für Spam
    - Verdächtige Wörter im Domain-Namen
    Score: 0.0 = kein Risiko, 1.0 = maximales Risiko
    """
    if not domains:
        return 0.0
    
    # Verdächtige TLDs (häufig bei Spam/Phishing)
    suspicious_tlds = {'tk', 'ml', 'ga', 'cf', 'top', 'download', 'trade', 'stream', 'racing', 'party', 'cricket'}
    
    # Verdächtige Keywords in Domains
    suspicious_keywords = {'bit', 'coin', 'crypto', 'token', 'wallet', 'bank', 'secure', 'verify', 'confirm', 'urgent'}
    
    total_risk = 0.0
    max_risk = 0.0
    
    for domain in domains:
        domain_lower = domain.lower()
        domain_risk = 0.0
        
        # 1) Verdächtige TLD (extrahiere TLD)
        parts = domain_lower.split('.')
        if len(parts) > 0:
            tld = parts[-1]
            if tld in suspicious_tlds:
                domain_risk += 0.3
        
        # 2) Verdächtige Keywords
        for keyword in suspicious_keywords:
            if keyword in domain_lower:
                domain_risk += 0.2
        
        # 3) Sehr kurze Domains (< 5 Zeichen Gesamtlänge ohne TLD)
        name_part = '.'.join(parts[:-1]) if len(parts) > 1 else domain_lower
        if len(name_part) < 4:
            domain_risk += 0.1
        
        # 4) Sehr lange Domains (> 30 Zeichen = Indiz für Obfuskation)
        if len(domain_lower) > 30:
            domain_risk += 0.1
        
        # 5) Domains mit vielen Hyphens (typisch für Phishing)
        if domain_lower.count('-') > 2:
            domain_risk += 0.15
        
        # Cap bei 1.0
        domain_risk = min(1.0, domain_risk)
        max_risk = max(max_risk, domain_risk)
        total_risk += domain_risk
    
    # Durchschnitt über alle Domains
    avg_risk = total_risk / len(domains) if domains else 0.0
    
    # Kombiniere: 70% Durchschnitt + 30% max (höchster Risk)
    combined_risk = (0.7 * avg_risk) + (0.3 * max_risk)
    
    return min(1.0, combined_risk)

async def _apply_hard_permissions(context, chat_id: int, active: bool):
    """
    Setzt für den Chat harte Schreibsperren (Nachtmodus) an/aus.
    active=True  -> can_send_messages=False
    active=False -> can_send_messages=True
    """
    try:
        if active:
            await context.bot.set_chat_permissions(
                chat_id=chat_id,
                permissions=ChatPermissions(can_send_messages=False)
            )
        else:
            await context.bot.set_chat_permissions(
                chat_id=chat_id,
                permissions=ChatPermissions(can_send_messages=True)
            )
    except Exception as e:
        logger.warning(f"Nachtmodus (hard) set_chat_permissions fehlgeschlagen: {e}")

def is_deleted_account(member) -> bool:
    """
    Erkenne gelöschte Accounts anhand der Bot-API-Daten:
    - Telegram ersetzt first_name durch 'Deleted Account'
    - oder entfernt alle Namen/Username bei gelöschten Accounts
    """
    user = member.user
    first = (user.first_name or "").lower()
    # 1) Default-Titel 'Deleted Account' (manchmal variiert: 'Deleted account')
    if first.startswith("deleted account"):
        return True
    # 2) Kein Name, kein Username mehr vorhanden - auch ein Indiz für gelöschten Account
    if not any([user.first_name, user.last_name, user.username]):
        return True
    return False
async def _get_telethon_client():
    """
    Lazy import, damit utils.py nicht beim Import schon crasht, falls Telethon-Env fehlt.
    Gibt (telethon_client, start_telethon) oder (None, None) zurück.
    """
    try:
        from shared.telethon_client import telethon_client, start_telethon
        return telethon_client, start_telethon
    except Exception as e:
        logger.warning(f"[clean_delete] Telethon nicht verfügbar: {type(e).__name__}: {e}")
        return None, None

async def _iter_deleted_participants_via_telethon(chat_id: int, *, limit: int | None = None):
    """
    Iteriert *nur* gelöschte Accounts aus dem Chat via MTProto (Telethon).
    Kein Import in DB nötig.
    """
    telethon_client, start_telethon = await _get_telethon_client()
    if not telethon_client or not start_telethon:
        return

    if not telethon_client.is_connected():
        await start_telethon()

    entity = await telethon_client.get_entity(chat_id)
    seen = 0
    async for user in telethon_client.iter_participants(entity):
        # Telethon: gelöschte Accounts haben i.d.R. user.deleted == True
        if getattr(user, "deleted", False):
            yield user
        seen += 1
        if limit and seen >= limit:
            break

async def _get_member(bot: ExtBot, chat_id: int, user_id: int) -> ChatMember | None:
    try:
        return await bot.get_chat_member(chat_id, user_id)
    except RetryAfter as e:
        await asyncio.sleep(getattr(e, "retry_after", 1))
        return await bot.get_chat_member(chat_id, user_id)
    except BadRequest as e:
        # z. B. "Chat member not found" → nicht (mehr) Mitglied
        logger.debug(f"get_chat_member({chat_id},{user_id}) -> {e}")
        return None

async def clean_delete_accounts_for_chat(chat_id: int, bot: ExtBot, *,
                                         dry_run: bool = False,
                                         demote_admins: bool = False,
                                         source: str = "auto",
                                         telethon_limit: int | None = None) -> int:
    """
    Entfernt geloeschte Accounts per ban+unban.
    - Entfernt DB-Eintrag NUR, wenn Kick erfolgreich war ODER der User nicht (mehr) im Chat ist.
    - Optional: demote_admins=True versucht geloeschte Admins zu demoten (erfordert Bot-Recht 'can_promote_members').
    Logging: detailliert alle Operationen und Fehler für Debugging.
    """
    logger.info(f"[clean_delete] Starting cleanup task for chat {chat_id} (dry_run={dry_run}, demote_admins={demote_admins})")
    
    # Permission check at start
    try:
        bot_member = await bot.get_chat_member(chat_id, bot.id)
        can_restrict = bool(getattr(bot_member, "can_restrict_members", False))
        can_promote  = bool(getattr(bot_member, "can_promote_members", False))
        logger.debug(f"[clean_delete] Bot permissions: can_restrict={can_restrict}, can_promote={can_promote}")

        if not can_restrict:
            logger.warning(f"[clean_delete] ABORT in {chat_id}: missing can_restrict_members")
            return 0

        # can_promote wird nur gebraucht, wenn wir deleted Admins demoten sollen
        if demote_admins and not can_promote:
            logger.warning(f"[clean_delete] demote_admins=True, aber Bot hat kein can_promote_members -> demote deaktiviert")
            demote_admins = False
    except Exception as e:
        logger.error(f"[clean_delete] ABORT in {chat_id}: failed to get permissions: {type(e).__name__}: {e}")
        return 0
    
    removed = 0

    # Quelle wählen:
    # - "telethon": vollständiger Scan ohne DB-Import
    # - "db": scannt nur DB-known members (schnell, aber unvollständig wenn nicht importiert)
    # - "auto": bevorzugt Telethon, wenn verfügbar (damit dein Problem direkt gelöst ist)
    src = (source or "auto").lower().strip()
    
    if src in ("auto", "telethon", "mtproto"):
        # Versuche Telethon-Scan (vollständig)
        any_telethon = False
        logger.info(f"[clean_delete] Using Telethon scan for chat {chat_id} (limit={telethon_limit})")
        async for tl_user in _iter_deleted_participants_via_telethon(chat_id, limit=telethon_limit):
            any_telethon = True
            uid = int(getattr(tl_user, "id"))

            # Für Status/Admin-Check nutzen wir Bot API nur für Treffer (sparsam)
            member = await _get_member(bot, chat_id, uid)
            if member is None:
                try:
                    mark_member_deleted(chat_id, uid)
                except Exception:
                    pass

            looks_del = True  # Telethon liefert schon "deleted"

        # Admin/Owner-Handhabung
        if member.status in ("administrator", "creator"):
            if not looks_del or not demote_admins or member.status == "creator":
                # Creator (Owner) nie anfassen; Admins nur wenn demote_admins=True
                if looks_del:
                    logger.debug(f"[clean_delete] User {uid} is deleted {member.status} but demote_admins={demote_admins}, skipping")
            # Demote versuchen (alle Rechte false)
            logger.info(f"[clean_delete] Demoting deleted {member.status} {uid}...")
            try:
                await bot.promote_chat_member(
                    chat_id, uid,
                    can_manage_chat=False, can_post_messages=False, can_edit_messages=False,
                    can_delete_messages=False, can_manage_video_chats=False, can_invite_users=False,
                    can_restrict_members=False, can_pin_messages=False, can_promote_members=False,
                    is_anonymous=False
                )
                # Status neu laden
                member = await _get_member(bot, chat_id, uid)
                logger.info(f"[clean_delete] Successfully demoted {uid}")
            except Exception as e:
                logger.warning(f"[clean_delete] Demote failed for {uid}: {type(e).__name__}: {e}") 
                
        if not looks_del:
            logger.debug(f"[clean_delete] User {uid} does not look deleted, skipping")

        if dry_run:
            logger.info(f"[clean_delete] DRY_RUN: Would remove deleted account {uid}")
            removed += 1

        kicked = False
        logger.info(f"[clean_delete] Banning deleted account {uid}...")
        try:
            await bot.ban_chat_member(chat_id, uid)
            logger.debug(f"[clean_delete] Banned {uid}, now unbanning...")
            try:
                await bot.unban_chat_member(chat_id, uid, only_if_banned=True)
                logger.debug(f"[clean_delete] Unbanned {uid}")
            except BadRequest as ube:
                logger.debug(f"[clean_delete] Unban failed (might be ok): {ube}")
            kicked = True
            removed += 1
            logger.info(f"[clean_delete] Successfully removed deleted account {uid} ({removed} total so far)")
        except Forbidden as e:
            logger.warning(f"[clean_delete] Permission denied removing {uid}: {e}")
        except BadRequest as e:
            logger.warning(f"[clean_delete] Bad request removing {uid}: {e}")

        # DB nur dann mit soft-delete markieren, wenn wirklich draußen
        try:
            member_after = await _get_member(bot, chat_id, uid)
            if kicked or member_after is None or getattr(member_after, "status", "") in ("left", "kicked"):
                logger.debug(f"[clean_delete] Marking {uid} as deleted in DB (kicked={kicked}, status after={getattr(member_after, 'status', 'N/A')})")
                mark_member_deleted(chat_id, uid)  # Soft-delete mit Audit-Trail
        except Exception as e:
            logger.debug(f"[clean_delete] Failed to mark {uid} as deleted: {e}")

        if any_telethon:
            logger.info(f"[clean_delete] Cleanup complete for {chat_id} via Telethon: removed {removed} deleted accounts (dry_run={dry_run})")
            return removed

        # Falls Telethon nicht verfügbar oder liefert nichts -> optional DB-Fallback
        if src in ("telethon", "mtproto"):
            logger.warning(f"[clean_delete] Telethon scan not available/empty for {chat_id}, no DB fallback (source={src})")
            return removed

    # DB-Fallback (schnell, aber nur bekannte Member)
    user_ids = [uid for uid in list_members(chat_id) if uid and uid > 0]
    logger.info(f"[clean_delete] Using DB member list: scanning {len(user_ids)} members in {chat_id} for deleted accounts...")
    for uid in user_ids:
        member = await _get_member(bot, chat_id, uid)
        if member is None:
            try:
                mark_member_deleted(chat_id, uid)
            except Exception:
                pass
            continue

        looks_del = is_deleted_account(member)

        if member.status in ("administrator", "creator"):
            if not looks_del or not demote_admins or member.status == "creator":
                continue
            try:
                await bot.promote_chat_member(
                    chat_id, uid,
                    can_manage_chat=False, can_post_messages=False, can_edit_messages=False,
                    can_delete_messages=False, can_manage_video_chats=False, can_invite_users=False,
                    can_restrict_members=False, can_pin_messages=False, can_promote_members=False,
                    is_anonymous=False
                )
                member = await _get_member(bot, chat_id, uid)
            except Exception:
                continue

        if not looks_del:
            continue

        if dry_run:
            removed += 1
            continue

        kicked = False
        try:
            await bot.ban_chat_member(chat_id, uid)
            try:
                await bot.unban_chat_member(chat_id, uid, only_if_banned=True)
            except BadRequest:
                pass
            kicked = True
            removed += 1
        except (Forbidden, BadRequest):
            pass

        try:
            member_after = await _get_member(bot, chat_id, uid)
            if kicked or member_after is None or getattr(member_after, "status", "") in ("left", "kicked"):
                mark_member_deleted(chat_id, uid)
        except Exception:
            pass

    logger.info(f"[clean_delete] Cleanup complete for {chat_id}: removed {removed} deleted accounts (dry_run={dry_run}, source={src})")
    return removed

async def cleanup_removed_chats(context) -> None:
    """
    Prüft alle registrierten Gruppen und löscht alle DB-Daten,
    wenn der Bot nicht mehr Mitglied der Gruppe ist (left/kicked/kein Zugriff).
    """
    bot = context.bot
    try:
        groups = get_registered_groups()  # [(chat_id, title), ...]
    except Exception as e:
        logger.error(f"[group_cleanup] Konnte Gruppenliste nicht laden: {e}")
        return

    for chat_id, title in groups:
        try:
            try:
                member = await bot.get_chat_member(chat_id, bot.id)
            except RetryAfter as e:
                await asyncio.sleep(getattr(e, "retry_after", 1))
                member = await bot.get_chat_member(chat_id, bot.id)
            except (BadRequest, Forbidden) as e:
                # Kein Zugriff mehr auf die Gruppe -> alles löschen
                logger.info(f"[group_cleanup] Bot hat keinen Zugriff mehr auf {chat_id} ({title}): {e}")
                delete_group_data(chat_id)
                continue

            status = getattr(member, "status", None)
            if status in ("left", "kicked"):
                logger.info(f"[group_cleanup] Bot ist '{status}' in {chat_id} ({title}) -> lösche alle Daten.")
                delete_group_data(chat_id)
        except Exception as e:
            logger.warning(f"[group_cleanup] Fehler bei Chat {chat_id}: {e}")
        # sanfte Drosselung
        await asyncio.sleep(0.1)