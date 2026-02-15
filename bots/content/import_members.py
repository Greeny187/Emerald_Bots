import asyncio
import os
import argparse
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
from bots.content.database import add_member

# Ersetze diese Werte mit deinen API-Credentials
api_id = 29370987
api_hash = 'd3c4c05db902fbefb7944e13c1a97afa'
BOT_TOKEN = os.getenv("BOT1_TOKEN")

async def list_chats():
    client = await TelegramClient('bot', api_id, api_hash).start(bot_token=BOT_TOKEN)
    print("Verfügbare Chats (Gruppen/Kanäle):")
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if getattr(entity, 'megagroup', False) or getattr(entity, 'broadcast', False) or getattr(entity, 'gigagroup', False):
            title = getattr(entity, 'title', None) or getattr(entity, 'username', None) or str(entity.id)
            username = f"@{entity.username}" if getattr(entity, 'username', None) else "-"
            print(f" • {title:30} | ID: {entity.id:>15} | Username: {username}")
    await client.disconnect()

async def import_members(group_identifier: str, *, verbose: bool = True) -> int:
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(os.getenv("SESSION_STRING")),
    api_id, api_hash)
    await client.start()
    
    try:
        if group_identifier.lstrip('-').isdigit():
            target = None
            async for dialog in client.iter_dialogs():
                if dialog.entity.id == int(group_identifier):
                    target = dialog.entity
                    break
            if not target:
                raise ValueError("ID nicht in deinen Chats gefunden.")
            entity = target
        else:
            entity = await client.get_entity(group_identifier)
    except Exception as e:
        print(f"Fehler beim Laden der Gruppe '{group_identifier}': {e}")
        await client.disconnect()
        return

    # Bestimme die richtige chat_id für die DB
    if isinstance(entity, Channel):
        # Supergroups und Kanäle: Bot-API-Chat-ID benötigt den -100 Präfix
        chat_id_db = int(f"-100{entity.id}")
    else:
        # Normale Gruppen/Chats haben ihre ID direkt
        chat_id_db = entity.id

    if verbose:
        print(f"\nImportiere Mitglieder von: {entity.title or entity.username} (DB chat_id={chat_id_db})\n")
    count = 0
    async for user in client.iter_participants(entity):
        add_member(chat_id_db, user.id)
        if verbose:
            print(f"âœ… {user.id:<10} {user.username or '-':<20} wurde gespeichert (chat_id={chat_id_db}).")
        count += 1

    if verbose:
        print(f"\nFertig! Insgesamt {count} Mitglieder gespeichert.")
    await client.disconnect()
    return count

async def main():
    parser = argparse.ArgumentParser(description="Importiere Telegram-Mitglieder und speichere sie in der vorhandenen Datenbank")
    parser.add_argument("--list", action="store_true", help="Liste alle verfügbaren Gruppen/Kanäle auf")
    parser.add_argument("--group", "-g", help="ID oder Username (z.B. @channel) der Gruppe")
    args = parser.parse_args()

    if args.list:
        await list_chats()
        return

    if not args.group:
        print("Nutze --list, um zuerst alle Chats aufzulisten, oder gib mit --group eine ID/Username an.")
        return

    confirm = input(f"Möchtest du die Mitglieder der Gruppe '{args.group}' importieren und in der DB speichern? [j/N] ")
    if confirm.lower() != 'j':
        print("Abgebrochen.")
        return

    await import_members(args.group)

if __name__ == '__main__':
    asyncio.run(main())
