import os
from telethon import TelegramClient
from telethon.sessions import StringSession
# Beachte: SESSION optional für Import, Raise erfolgt in start_telethon()

# Telegram-API-Zugangsdaten aus den Umgebungsvariablen
_api_id  = os.getenv("TG_API_ID")
API_ID   = int(_api_id) if _api_id else None
API_HASH = os.getenv("TG_API_HASH")
# Eine StringSession für einen Benutzeraccount ist notwendig, da Bots keine GetHistoryRequests erlauben
SESSION  = os.getenv("TELETHON_SESSION")  # StringSession für Benutzer-Login

# Client-Instanz erzeugen (StringSession speichert Login-Daten)
telethon_client = TelegramClient(StringSession(SESSION) if SESSION else StringSession(), API_ID, API_HASH)

if not all([API_ID, API_HASH, SESSION]):
    print(f"API_ID: {API_ID}, API_HASH: {'gesetzt' if API_HASH else 'fehlt'}, SESSION: {'gesetzt' if SESSION else 'fehlt'}")
    raise RuntimeError("TG_API_ID, TG_API_HASH und TELETHON_SESSION müssen als Env-Vars gesetzt sein!")

async def start_telethon():
    """Stellt die Verbindung her und prüft die Autorisierung."""
    if not SESSION:
        print(f"API_ID: {API_ID}, API_HASH: {'gesetzt' if API_HASH else 'fehlt'}, SESSION: {'gesetzt' if SESSION else 'fehlt'}")
        raise RuntimeError("Die Umgebungsvariable TELETHON_SESSION ist nicht gesetzt. Bitte SESSION erzeugen und in Heroku Config hinzufügen!")
    await telethon_client.connect()
    if not await telethon_client.is_user_authorized():
        raise RuntimeError("Telethon-Client ist nicht autorisiert! Bitte SESSION prüfen.")
    
async def generate_new_session():
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.start()
    print("Neue StringSession:")
    print(client.session.save())