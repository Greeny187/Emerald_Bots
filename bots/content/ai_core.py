import os, json, logging
from typing import Optional, Dict, Any

# Nur leichte DB-Helpers importieren (keine zirkulären Abhängigkeiten mit utils)
from .database import is_pro_chat, get_ai_settings, effective_ai_mod_policy  # vorhandene DB-Funktionen

log = logging.getLogger(__name__)

_OPENAI_KEY = os.getenv("OPENAI_API_KEY")
def ai_available() -> bool:
    return bool(_OPENAI_KEY)


def _client():
    from openai import OpenAI  # lazy import
    return OpenAI(api_key=_OPENAI_KEY)

# ---------- Feature-Gates (optional nutzbar) ----------

def is_pro(chat_id: int) -> bool:
    """Zentraler Pro-Check (DB)."""
    try:
        return is_pro_chat(chat_id)
    except Exception as e:
        log.warning(f"is_pro_chat failed for {chat_id}: {e}")
        return False

def can_use_faq_ai(chat_id: int) -> bool:
    """FAQ-KI nur, wenn Gruppe Pro *und* FAQ-KI in Settings aktiv ist."""
    faq_on, _rss_on = get_ai_settings(chat_id)
    return bool(faq_on) and is_pro(chat_id)

def can_use_aimod(chat_id: int, topic_id: Optional[int]) -> bool:
    """KI-Moderation nur, wenn Gruppe Pro *und* Policy enabled ist."""
    policy = effective_ai_mod_policy(chat_id, topic_id)
    return bool(policy.get("enabled")) and is_pro(chat_id)

# ---------- KI: Zusammenfassung ----------

async def ai_summarize(text: str, lang: str = "de") -> Optional[str]:
    """
    Sehr knapper TL;DR (1–2 Sätze) in 'lang'.
    KEIN Pro-Check hier: Der erfolgt an der Call-Site (z. B. FAQ-Fallback),
    damit alte Aufrufe abwärtskompatibel bleiben.
    """
    if not ai_available() or not text:
        return None
    try:
        client = _client()
        prompt = (
            f"Fasse die folgende News extrem knapp auf {lang} zusammen "
            f"(max. 2 Sätze, keine Floskeln):\n\n{text}"
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Du schreibst kurz, sachlich, deutsch."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=120,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        log.info(f"OpenAI summarize unavailable: {e}")
        return None

# ---------- KI: Bildmoderation ----------

async def ai_moderate_image(image_url: str) -> Optional[Dict[str, float]]:
    """
    Scores 0..1: nudity, sexual_minors, violence, weapons, gore.
    KEIN Pro-Check hier – Gate an der Call-Site (ai_moderation_enforcer).
    """
    if not ai_available() or not image_url:
        return None
    try:
        client = _client()
        prompt = ("Bewerte das Bild. Antworte NUR mit JSON-Objekt: "
                  '{"nudity":0..1,"sexual_minors":0..1,"violence":0..1,"weapons":0..1,"gore":0..1}')
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            max_tokens=120,
            messages=[
                {"role": "system", "content": "Du antwortest ausschließlich mit JSON."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}}
                ]},
            ],
        )
        data = res.choices[0].message.content.strip()
        out = json.loads(data)
        for k in ("nudity", "sexual_minors", "violence", "weapons", "gore"):
            out[k] = float(out.get(k, 0))
        return out
    except Exception as e:
        log.info(f"AI vision unavailable: {e}")
        return None

# ---------- KI: Textmoderation ----------

async def ai_moderate_text(text: str, model: str = "omni-moderation-latest") -> Optional[Dict[str, Any]]:
    """
    Rückgabe: {'categories': {'toxicity':score,...}, 'flagged': bool}
    KEIN Pro-Check hier – Gate an der Call-Site (ai_moderation_enforcer).
    """
    if not ai_available() or not text:
        return None
    try:
        client = _client()
        try:
            res = client.moderations.create(model=model, input=text)
            out = res.results[0]
            cats = getattr(out, "category_scores", {}) or {}
            scores = {
                # robuste Zuordnung – identisch zu vorher
                "toxicity":   float(cats.get("harassment/threats", 0.0) or cats.get("harassment", 0.0)),
                "hate":       float(cats.get("hate", 0.0) or cats.get("hate/threatening", 0.0)),
                "sexual":     float(cats.get("sexual/minors", 0.0) or cats.get("sexual", 0.0)),
                "harassment": float(cats.get("harassment", 0.0)),
                "selfharm":   float(cats.get("self-harm", 0.0)),
                "violence":   float(cats.get("violence", 0.0) or cats.get("violence/graphic", 0.0)),
            }
            return {"categories": scores, "flagged": bool(getattr(out, "flagged", False))}
        except Exception:
            # Fallback: Klassifikation via Chat-Modell (JSON-only)
            prompt = (
                "Klassifiziere den folgenden Text. Gib JSON zurück mit keys: "
                "toxicity,hate,sexual,harassment,selfharm,violence (Werte 0..1). "
                "Nur das JSON, keine Erklärungen.\n\n" + text[:6000]
            )
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Du antwortest nur mit JSON."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=200,
            )
            data = json.loads(res.choices[0].message.content)
            return {
                "categories": {k: float(data.get(k, 0)) for k in ["toxicity", "hate", "sexual", "harassment", "selfharm", "violence"]},
                "flagged": any(float(data.get(k, 0)) >= 0.8 for k in data),
            }
    except Exception as e:
        log.info(f"AI moderation unavailable: {e}")
        return None

