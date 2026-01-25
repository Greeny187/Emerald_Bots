from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from shared.translator import translate_hybrid

# Basis-Handbuch in deutscher Sprache
HELP_TEXT = """
*Emerald Content Bot - Benutzerhandbuch*

*Inhaltsverzeichnis*
1. Funktionen im Überblick
2. Bot-Chat: Menü & Miniapp
3. Gruppen-Chat: Befehle & Abläufe
4. Erweiterte Features (Pro & EMRD)
5. Support & Kontakt

---

*1. Funktionen im Überblick*

Basis-Features:
• Begrüßungsnachrichten setzen (mit optionalem Foto)
• Regeln festlegen (mit optionalem Foto)
• Abschiedsnachrichten setzen (mit optionalem Foto)
• Link- & Spam-Schutz pro Topic
• Captcha für neue Mitglieder
• Themenverantwortliche verwalten
• FAQ-Datenbank mit Kurzantworten
• RSS-Feeds je Topic
• Mood-Meter / Stimmungs-Umfragen
• Nachtmodus (zeitgesteuerte Ruhephase)
• KI-Moderation (AI-Mod) für Texte, Medien & Links
• Topic-Router zur automatischen Verteilung von Nachrichten
• Automatischer Clean-Up gelöschter Accounts
• Reports & Statistiken

Erweiterte Features & Pro:
• Pro-Plan mit verschiedenen Zahlungsoptionen
• EMRD-Rewards-System
• Detaillierte Statistik-Ansichten
• KI-gestützte FAQ-Antworten
• Erweiterte Moderation (Strike-System, Shadow-Mode)

---

*2. Bot-Chat: Menü & Miniapp*

Grundlegende Befehle im privaten Chat mit dem Bot:
/start    – Bot starten und Gruppe verbinden
/miniapp  – Einstellungsoberfläche öffnen (Telegram Miniapp)
/help     – Dieses Handbuch anzeigen
/version  – Aktuelle Version & Patchnotes anzeigen

Die Miniapp ist in Tabs (Pills) unterteilt:

*WELCOME*
• Begrüßung aktivieren/deaktivieren
• Optionales Begrüßungsbild hochladen/löschen
• Begrüßungstext mit Variablen:
  – {user}  = neuer Nutzer
  – {title} = Gruppenname
• Optional: Captcha für neue Mitglieder aktivieren

*RULES*
• Regeln aktivieren/deaktivieren
• Optionales Regelbild hochladen/löschen
• Freier Regeltext für deine Gruppe

*FAREWELL*
• Abschiedsnachricht aktivieren/deaktivieren
• Optional: Abschiedsbild
• Abschiedstext, wenn Nutzer die Gruppe verlassen

*SPAM*
• Spam-Filter aktivieren
• Links, Medien und Invite-Links blockieren
• Policy-Topic (ID) festlegen, in das Meldungen/Logs gehen
• Aktion auswählen:
  – Löschen
  – Warnen
  – Stummschalten
• Whitelist-Domains (erlaubte Links)
• Blacklist-Domains (zu blockierende Links)
• Emoji- und Flood-Limits pro Zeitraum
• Option: Antworten im gleichen Topic lassen oder in Policy-Topic verschieben

*RSS*
• RSS-Feeds hinzufügen, bearbeiten, löschen
• Je Feed:
  – Feed-URL
  – Ziel-Topic (ID)
  – Posting-Format (Titel, Text, Link)
• Optional: Zusammenfassung durch KI (wenn KI-Analyse aktiv ist)

*KI / FAQ (AI)*
• KI/FAQ global aktivieren/deaktivieren
• RSS-KI-Analyse nutzen (Inhalte werden vorgefiltert/kommentiert)
• FAQ-Hinweistext definieren (wird für Nutzer angezeigt)

*MOOD*
• Mood-/Stimmungs-Umfrage aktivieren
• Frage setzen (z. B. „Wie geht es dir heute?“)
• Ziel-Topic auswählen (0 = Hauptchat)
• Button „Umfrage jetzt senden“:
  – Versendet sofort eine Mood-Umfrage
  – Nutzer stimmen per 👍 👎 🤔 ab
• Die Umfrage kann regelmäßig über den Bot-Job ausgelöst werden (z. B. täglich).

*FAQ*
• Neue FAQ-Einträge anlegen:
  – Frage
  – Kurz-Antwort
• Bestehende Einträge in der Liste ansehen, bearbeiten, löschen
• Wird von /faq im Gruppenchat verwendet

*NIGHT (Nachtmodus)*
• Nachtmodus aktivieren/deaktivieren
• Start- & Endzeit (HH:MM) festlegen
• Schreib-Sperre (write_lock):
  – Wenn aktiv, können Nicht-Admins in der Nacht nicht schreiben
  – Optional: Nachrichten löschen statt nur blocken
• Lock-Nachricht definieren (Hinweistext, wenn Schreiben gesperrt)
• Option „Non-Admin Nachrichten löschen (Nachtzeiträume)“
• Option „Warnung anzeigen“ beim ersten Verstoß
• Zeitzone festlegen (z. B. Europe/Berlin)
• Hard-Mode:
  – Strikter Modus, bei dem der Chat komplett „zu“ sein kann

*AI-MOD (KI Moderation)*
• AI-Mod aktivieren/deaktivieren
• Shadow-Mode:
  – Aktionen nur loggen, ohne live einzugreifen
• Primär-Aktion:
  – Löschen, Warnen oder Stummschalten
• Mute-Minuten definieren
• Einstellungen für:
  – Medien-Moderation (Bilder)
  – Link-Risiko-Bewertung
  – Strike-Punkte (wie viele Punkte pro Verstoß)
  – Max. Strikes pro Nachricht
  – Tägliches Limit für Aktionen (Rate-Limit)
• Schwellenwerte pro Kategorie:
  – Toxicity
  – Hate
  – Sexual
  – Harassment

*REPORT*
• täglichen Report aktivieren/deaktivieren
• Report-Topic (ID) definieren
• „Report jetzt posten“:
  – Sofortiger Report der aktuellen Kennzahlen in das gewählte Topic

*STATS*
• Zeitraum auswählen (7 / 14 / 30 Tage)
• Statistik laden:
  – Aktivität nach Tagen
  – Top-Antwortende
  – Überblick über genutzte Topics & Features

*REWARDS (EMRD)*
• EMRD-Rewards aktivieren/deaktivieren
• Modus:
  – Claim (User können selbst claimen)
  – Auto (Owner erhält gesammelten Reward)
• Feste Raten definieren:
  – Punkte pro Nachricht / Antwort etc. (abhängig vom Backend)
• Mindestbetrag für Claims festlegen
• Caps:
  – Cap pro Nutzer/Tag
  – Cap pro Chat/Tag
• Test-Button: Claim-Funktion testen
• Hinweis: EMRD ist ein Utility-Token im TON-Netzwerk.

*SONSTIGES (MORE)*
• Bereich „Gelöschte Accounts aufräumen“:
  – Scheduler aktivieren (geplante Bereinigung)
  – Uhrzeit festlegen
  – Optionaler Wochentag (oder täglich)
  – Option: „Admins demoten“, deren Accounts gelöscht wurden
  – Option: „Ergebnis melden“ (Log-Nachricht nach Lauf)
  – Button „Jetzt ausführen“:
    › Sofortige Bereinigung gelöschter Accounts in der Gruppe

*PRO*
• Pro-Plan Zahlung konfigurieren:
  – ℹ️ Info-Text: Nutzer können via /buypro upgraden.
• Blockchain-Zahlungen:
  – TON Wallet aktivieren & Adresse hinterlegen
• Klassische Zahlungen:
  – PayPal-Link setzen
  – Telegram Stars als Zahlungsmittel aktivieren
  – Kostenloser Testzeitraum (Free-Trial in Tagen)
• Preise je Laufzeit definieren:
  – Monatlich
  – Quartalsweise
  – Jährlich
  (die Standardwerte in der Miniapp sind Vorschläge und können angepasst werden)
• PRO-Beschreibung:
  – Text, der im /buypro-Menü angezeigt wird (Leistungsumfang)
• Test-Button:
  – „PRO Payment Menü öffnen“ zum Überprüfen deiner Einstellungen

---

*3. Gruppen-Chat: Befehle & Abläufe*

Die wichtigsten Befehle im Gruppenchat (bzw. in Threads):

*Rollen & Themen*
/settopic @user
• Weist einem Nutzer die Verantwortung für das aktuelle Topic zu.

/removetopic @user
• Entfernt die Themenverantwortung des Nutzers.

*Limits & Kontingente*
/topiclimit <anzahl>       (im Thread)
/topiclimit <topic_id> <anzahl>   (im Privat-Chat)
/myquota
• Tageslimit pro Nutzer und Topic setzen und anzeigen.
• 0 = kein Limit.

*Spam & Router*
/spamlevel off|light|medium|strict [flags]
• Setzt die Spam-Policy.
• Mögliche Flags:
  – emoji=N
  – emoji_per_min=N
  – flood10s=N
  – whitelist=dom1,dom2
  – blacklist=dom3,dom4

/router list
• Listet alle aktiven Router-Regeln.

/router add <topic_id> keywords=a,b
/router add <topic_id> domains=x.com,y.com
• Fügt Router-Regeln hinzu (nach Keywords oder Domains).

/router del <rule_id>
/router toggle <rule_id> on|off
• Regeln löschen bzw. aktivieren/deaktivieren.

*FAQ & Regeln*
/faq <Stichwort>
• Durchsucht die FAQ-Datenbank der Gruppe nach passenden Einträgen.

/rules
• Zeigt den in der Miniapp hinterlegten Regeltext an.

*Clean-Up & Nightmode*
/cleandeleteaccounts
• Manuelle Bereinigung gelöschter Accounts in der Gruppe
  (ergänzt den geplanten Scheduler im Tab „Sonstiges“).

/quietnow 30m
/quietnow 2h
• Startet sofort eine Ruhephase auf Basis der Nightmode-Einstellungen.
• Praktisch bei spontanen Eskalationen oder Bedarf an kurzer Pause.

*Strikes & KI-Moderation*
/mystrikes
• Zeigt deine aktuellen Strike-Punkte in dieser Gruppe.

/strikes
• Zeigt eine Übersicht der Nutzer mit den meisten Strike-Punkten.

*Wallet & Rewards*
/wallet <TON-Adresse>
• Speichert deine TON-Wallet für EMRD-Rewards.
/wallet
• Zeigt die aktuell gespeicherte Adresse.

/buypro
• Öffnet das PRO-Zahlungsmenü (wenn in der Miniapp konfiguriert).

---

*4. Erweiterte Features (Pro & EMRD)*

*Nachtmodus*
• Zeitgesteuerte Ruhephasen für deine Gruppe
• Schreib-Sperre für Nicht-Admins
• Optionale Löschung von Nachrichten innerhalb der Nachtfenster
• Hard-Mode für sehr strikte Ruhephasen
• Softruhe per /quietnow (Dauer individuell bestimmbar)

*KI-Moderation (AI-Mod)*
• Automatischer Schutz vor Hate, Spam, Toxizität & NSFW-Inhalten
• Shadow-Mode zum Testen ohne echte Eingriffe
• Strike-System:
  – Punkte pro Verstoß, Eskalation bei Überschreitung
• Medien- & Link-Analyse integriert
• Reports & Logs pro Chat/Topic

*EMRD-Rewards*
• EMRD ist ein Utility-Token im TON-Netzwerk.
• Nutzer verdienen Punkte/Reputation für hilfreiche Beiträge.
• Rewards-Modi:
  – Claim (User claimen ihre Rewards selbst)
  – Auto (Owner erhält gesammelte Rewards)
• Limits schützen vor Missbrauch:
  – Max. Punkte pro Nutzer/Tag
  – Max. Punkte pro Chat/Tag
• Anspruchsberechtigte Beträge können später on-chain ausgezahlt werden.

*Statistiken & Reports*
• Tägliche Reports (aktivierbar im Tab „Report“)
• Detaillierte Statistiken:
  – Zeitliche Aktivität
  – Top-Antwortende
  – Nutzung von Topics & Features
• Hilft bei:
  – Moderations-Planung
  – Community-Management
  – Bewertung des Pro-Plans und der KI-Funktionen

---

*5. Support & Kontakt*

Website: https://greeny187.github.io/GreenyManagementBots/
Offizielle Telegram-Gruppe: https://t.me/EmeraldEcoSystem
PayPal: Emerald@mail.de
TON Wallet: UQBopac1WFJGC_K48T8JqcbRoH3evUoUDwS2oItlS-SgpR8L

Version & Änderungen: Nutze /version oder den entsprechenden Hinweis im Bot,
um die aktuellen Patchnotes zu sehen.
"""

async def send_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Sendet das Benutzerhandbuch in der Nutzersprache.
    """
    user_lang = update.effective_user.language_code or "de"
    
    # Übersetze den Text in die Nutzersprache
    translated = translate_hybrid(HELP_TEXT, target_lang=user_lang)
    
    # Sende das Handbuch direkt als Nachricht
    await update.message.reply_text(
        translated,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

help_handler = CommandHandler("help", send_manual)

__all__ = ["help_handler"]
