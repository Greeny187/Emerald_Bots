"""
Crossposter Bot - Multilingual Support (i18n)
Integrates with shared/translator.py for dynamic translations
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Static translations for common strings (German base)
TRANSLATIONS = {
    "de": {
        "routes_title": "Deine Routen",
        "route_created": "✅ Route erstellt",
        "route_updated": "✅ Route aktualisiert",
        "route_deleted": "✅ Route gelöscht",
        "error_invalid_tenant": "❌ Ungültiger Mandant",
        "error_missing_source": "❌ Quell-Chat-ID erforderlich",
        "error_missing_destinations": "❌ Mindestens ein Ziel erforderlich",
        "loading": "⏳ Lädt…",
        "no_routes": "Keine Routen gefunden",
        "test_send": "Test-Nachricht gesendet",
        "error_test_send": "❌ Fehler beim Test-Senden",
        "connector_saved": "✅ Connector gespeichert",
        "reward_points_earned": "💎 {points} Punkte für das Crossposting verdient!",
        "stats_updated": "📊 Statistik aktualisiert",
    },
    "en": {
        "routes_title": "Your Routes",
        "route_created": "✅ Route created",
        "route_updated": "✅ Route updated",
        "route_deleted": "✅ Route deleted",
        "error_invalid_tenant": "❌ Invalid tenant",
        "error_missing_source": "❌ Source chat ID required",
        "error_missing_destinations": "❌ At least one destination required",
        "loading": "⏳ Loading…",
        "no_routes": "No routes found",
        "test_send": "Test message sent",
        "error_test_send": "❌ Error sending test message",
        "connector_saved": "✅ Connector saved",
        "reward_points_earned": "💎 Earned {points} points for crossposting!",
        "stats_updated": "📊 Stats updated",
    },
    "fr": {
        "routes_title": "Vos itinéraires",
        "route_created": "✅ Itinéraire créé",
        "route_updated": "✅ Itinéraire mis à jour",
        "route_deleted": "✅ Itinéraire supprimé",
        "error_invalid_tenant": "❌ Locataire invalide",
        "error_missing_source": "❌ ID de chat source requise",
        "error_missing_destinations": "❌ Au moins une destination requise",
        "loading": "⏳ Chargement…",
        "no_routes": "Aucun itinéraire trouvé",
        "test_send": "Message de test envoyé",
        "error_test_send": "❌ Erreur lors de l'envoi du message de test",
        "connector_saved": "✅ Connecteur enregistré",
        "reward_points_earned": "💎 {points} points gagnés pour le partage croisé!",
        "stats_updated": "📊 Statistiques mises à jour",
    },
}

class LanguageManager:
    """Manage user language preferences and translations"""
    
    def __init__(self, db_pool=None):
        self.db_pool = db_pool
        logger.info("[I18N] LanguageManager initialized")
    
    async def get_user_language(self, user_id: int) -> str:
        """Get user's preferred language (default: 'de')"""
        if not self.db_pool:
            return "de"
        
        try:
            async with self.db_pool.acquire() as conn:
                lang = await conn.fetchval(
                    "SELECT language FROM users WHERE user_id = $1",
                    user_id
                )
            return lang or "de"
        except Exception as e:
            logger.warning(f"[I18N] Error fetching user language: {e}")
            return "de"
    
    async def set_user_language(self, user_id: int, language: str) -> bool:
        """Set user's language preference"""
        if not self.db_pool:
            return False
        
        if language not in TRANSLATIONS:
            logger.warning(f"[I18N] Unsupported language: {language}")
            return False
        
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET language = $1 WHERE user_id = $2",
                    language, user_id
                )
            logger.info(f"[I18N] User {user_id} language set to {language}")
            return True
        except Exception as e:
            logger.error(f"[I18N] Error setting user language: {e}")
            return False
    
    def get_text(self, key: str, language: str = "de", **kwargs) -> str:
        """Get translated text for a key"""
        if language not in TRANSLATIONS:
            language = "de"
        
        text = TRANSLATIONS.get(language, {}).get(key, TRANSLATIONS["de"].get(key, key))
        
        # Format placeholders
        if kwargs:
            try:
                text = text.format(**kwargs)
            except KeyError as e:
                logger.warning(f"[I18N] Missing placeholder: {e}")
        
        return text
    
    async def translate_dynamic(self, text: str, language: str = "de", translator=None) -> str:
        """Dynamically translate text using translator.py (with caching)"""
        if language == "de":  # Default language
            return text
        
        if not translator:
            logger.warning("[I18N] No translator available for dynamic translation")
            return text
        
        try:
            # Use shared translator with caching
            translated = await translator.translate(text, target_language=language)
            logger.info(f"[I18N] Dynamic translation: {language} - {translated[:50]}...")
            return translated
        except Exception as e:
            logger.error(f"[I18N] Dynamic translation error: {e}")
            return text
