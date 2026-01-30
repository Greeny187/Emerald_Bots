"""Learning Bot - Message & Command Handlers"""

import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes, CommandHandler, MessageHandler, filters, CallbackQueryHandler, Application
from datetime import datetime

logger = logging.getLogger("bot.learning.handlers")

MINIAPP_URL = os.getenv(
    "MINIAPP_URL",
    "https://greeny187.github.io/EmeraldContentBots/miniapp/applearning.html"
)

try:
    from shared.emrd_rewards_integration import award_points
    from . import database
    from . import ai_content
except ImportError as e:
    logger.warning(f"Import warning: {e}")
    async def award_points(*args, **kwargs):
        pass
    database = None
    ai_content = None


# ===== WELCOME & START =====

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Willkommensbefehl"""
    user = update.effective_user
    chat = update.effective_chat
    logger.info(f"[CMD_START] user={user.id} chat_type={chat.type}")
    
    if chat.type == "private":
        await update.message.reply_text(
            "📚 **Emerald Academy**\n\n"
            "Lerne Blockchain, Smart Contracts, DeFi & Trading!\n\n"
            "🎓 **Was du lernst:**\n"
            "🔗 Blockchain Basics\n"
            "🪙 Smart Contracts\n"
            "🏦 DeFi Protokolle\n"
            "📈 Trading Strategien\n"
            "💱 Token Economics\n\n"
            "💰 **Verdiene EMRD Token** durch:\n"
            "✅ Kurse absolvieren\n"
            "✅ Quizze lösen\n"
            "✅ Zertifikate erhalten",
            parse_mode="Markdown"
        )
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📖 Academy öffnen",
                web_app=WebAppInfo(url=MINIAPP_URL)
            )],
            [
                InlineKeyboardButton("📊 Mein Fortschritt", callback_data="my_progress"),
                InlineKeyboardButton("🏆 Belohnungen", callback_data="my_rewards")
            ],
            [InlineKeyboardButton("❓ Hilfe", callback_data="learning_help")]
        ])
        await update.message.reply_text("Wähle eine Option:", reply_markup=keyboard)
    else:
        await update.message.reply_text("📚 **Emerald Academy** aktiv!\nTyp /learning für private Nachrichten!")


# ===== COURSES & ENROLLMENT =====

async def cmd_courses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List available courses"""
    user = update.effective_user
    logger.info(f"[CMD_COURSES] user={user.id}")
    
    if not database:
        logger.warning("[CMD_COURSES] Database connection unavailable")
        await update.message.reply_text("⚠️ Datenbankverbindung fehlt")
        return
    
    try:
        logger.debug("[CMD_COURSES] Fetching courses by level...")
        courses_by_level = {
            "beginner": database.get_all_courses("beginner"),
            "intermediate": database.get_all_courses("intermediate"),
            "advanced": database.get_all_courses("advanced")
        }
        logger.debug(f"[CMD_COURSES] Courses fetched: beginner={len(courses_by_level.get('beginner', []))} intermediate={len(courses_by_level.get('intermediate', []))} advanced={len(courses_by_level.get('advanced', []))}")
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"🟢 Anfänger ({len(courses_by_level.get('beginner', []))} Kurse)",
                callback_data="courses_beginner"
            )],
            [InlineKeyboardButton(
                f"🟡 Mittelstufe ({len(courses_by_level.get('intermediate', []))} Kurse)",
                callback_data="courses_intermediate"
            )],
            [InlineKeyboardButton(
                f"🔴 Fortgeschrittene ({len(courses_by_level.get('advanced', []))} Kurse)",
                callback_data="courses_advanced"
            )]
        ])
        
        await update.message.reply_text(
            "📚 **Verfügbare Kurse**\n\n"
            "Wähle dein Level:",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"[CMD_COURSES] Error: {e}", exc_info=True)
        await update.message.reply_text("❌ Fehler beim Laden der Kurse")


# ===== PROGRESS & STATS =====

async def cmd_progress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's learning progress"""
    user = update.effective_user
    logger.info(f"[CMD_PROGRESS] user={user.id}")
    
    if not database:
        logger.warning("[CMD_PROGRESS] Database connection unavailable")
        await update.message.reply_text("⚠️ Datenbankverbindung fehlt")
        return
    
    try:
        logger.debug(f"[CMD_PROGRESS] Fetching stats for {user.id}")
        stats = database.get_course_stats(user.id)
        logger.debug(f"[CMD_PROGRESS] Stats: {stats}")
        
        progress_text = f"""
📊 **Dein Lernfortschritt**

📚 Kurse: {stats.get('completed_courses', 0)}/{stats.get('total_courses', 0)} abgeschlossen
💰 Verdient: {stats.get('emrd_earned', 0):.2f} EMRD
🔥 Streak: {stats.get('current_streak', 0)} Tage
🏅 Best Streak: {stats.get('longest_streak', 0)} Tage

📝 **Öffne die Mini-App** für detaillierte Statistiken und zum Fortfahren!
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📖 Zur Academy",
                web_app=WebAppInfo(url=MINIAPP_URL)
            )],
            [InlineKeyboardButton("🔄 Aktualisieren", callback_data="refresh_progress")]
        ])
        
        await update.message.reply_text(progress_text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[CMD_PROGRESS] Error: {e}", exc_info=True)
        await update.message.reply_text("❌ Fehler beim Laden des Fortschritts")


# ===== REWARDS & CLAIMS =====

async def cmd_rewards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show learning rewards"""
    user = update.effective_user
    logger.info(f"[CMD_REWARDS] user={user.id}")
    
    if not database:
        logger.warning("[CMD_REWARDS] Database connection unavailable")
        await update.message.reply_text("⚠️ Datenbankverbindung fehlt")
        return
    
    try:
        # Get unclaimed rewards
        logger.debug(f"[CMD_REWARDS] Fetching rewards for {user.id}")
        stats = database.get_course_stats(user.id)
        
        rewards_text = f"""
🏆 **Learning Rewards**

💰 **Du hast verdient:** {stats.get('emrd_earned', 0):.2f} EMRD

*Verdiene mehr durch:*
✅ Kurse abschließen: +50-100 EMRD
✅ Alle Module: +30 EMRD
✅ Quizze 100% richtig: +20 EMRD
✅ Zertifikat erhalten: +50 EMRD

🔥 **Streak Bonuses:**
- 7 Tage: +50 EMRD
- 30 Tage: +200 EMRD
- 100 Tage: +1000 EMRD

🎯 **Nächstes Ziel:** Öffne die Mini-App zum Claimen!
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💸 Belohnungen Claimen",
                web_app=WebAppInfo(url=MINIAPP_URL)
            )],
            [InlineKeyboardButton("📊 Mein Fortschritt", callback_data="my_progress")]
        ])
        
        await update.message.reply_text(rewards_text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[CMD_REWARDS] Error: {e}", exc_info=True)
        await update.message.reply_text("❌ Fehler beim Laden der Belohnungen")


# ===== ACHIEVEMENTS & LEADERBOARD =====

async def cmd_achievements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user achievements"""
    user = update.effective_user
    logger.info(f"[CMD_ACHIEVEMENTS] user={user.id}")
    
    if not database:
        logger.warning("[CMD_ACHIEVEMENTS] Database connection unavailable")
        await update.message.reply_text("⚠️ Datenbankverbindung fehlt")
        return
    
    try:
        logger.debug(f"[CMD_ACHIEVEMENTS] Fetching achievements for {user.id}")
        achievements = database.get_user_achievements(user.id)
        
        if not achievements:
            achievements_text = "🎯 Noch keine Achievements. Starte einen Kurs um zu beginnen!"
        else:
            achievements_text = "🏅 **Deine Achievements:**\n\n"
            for a in achievements:
                achievements_text += f"{a['achievement_icon']} {a['achievement_name']}\n"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📖 Zur Academy",
                web_app=WebAppInfo(url=MINIAPP_URL)
            )]
        ])
        
        await update.message.reply_text(achievements_text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[CMD_ACHIEVEMENTS] Error: {e}", exc_info=True)
        await update.message.reply_text("❌ Fehler beim Laden der Achievements")


# ===== HELP =====

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help"""
    logger.info(f"[CMD_HELP] user={update.effective_user.id}")
    help_text = """
📚 **Learning Bot - Hilfe**

**🎯 Befehle:**
/start - Willkommen
/learning - Academy öffnen
/courses - Verfügbare Kurse
/progress - Dein Fortschritt
/rewards - Verdiente EMRD
/achievements - Deine Badges
/help - Dieser Text

**🌟 Mini-App Features:**
📖 Interaktive Kurse
❓ KI-generierte Quizze
🏆 Abzeichen & Zertifikate
💰 Automatisches Reward-System
📊 Detaillierte Statistiken
🔥 Streak-Tracking
🎯 Adaptive Inhalte basierend auf Fortschritt

**💡 Tipps:**
• Öffne die Mini-App für beste Erfahrung
• Täglich lernen für Streak-Bonuses
• 100% in Quizzen für Extra-EMRD
• Komplette Kurse = Zertifikat!

**❓ Support:**
Probleme? Schreib @support oder nutze die Chat-Funktion in der App!
🏆 Abzeichen & Zertifikate
💰 Reward-System
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")


# ===== CALLBACK HANDLERS =====

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Button callbacks"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    logger.info(f"[BUTTON_CALLBACK] user={user.id} data={query.data}")
    
    if query.data == "learning_help":
        await cmd_help(update, context)
    
    elif query.data == "my_progress":
        update.message = query
        await cmd_progress(update, context)
    
    elif query.data == "my_rewards":
        update.message = query
        await cmd_rewards(update, context)
    
    elif query.data == "refresh_progress":
        try:
            user = query.from_user
            if database:
                stats = database.get_course_stats(user.id)
                progress_text = f"""
📊 **Dein Lernfortschritt** (Aktualisiert)

📚 Kurse: {stats.get('completed_courses', 0)}/{stats.get('total_courses', 0)} abgeschlossen
💰 Verdient: {stats.get('emrd_earned', 0):.2f} EMRD
🔥 Streak: {stats.get('current_streak', 0)} Tage
🏅 Best Streak: {stats.get('longest_streak', 0)} Tage
"""
                await query.edit_message_text(progress_text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error refreshing: {e}")
    
    elif query.data.startswith("courses_"):
        level = query.data.split("_")[1]
        if database:
            courses = database.get_all_courses(level)
            course_list = "📚 **Verfügbare Kurse:**\n\n"
            for course in courses:
                icon = course.get('icon', '📖')
                duration = course.get('duration_minutes', 0)
                course_list += f"{icon} **{course['title']}**\n"
                course_list += f"⏱️ {duration} Min | 🏆 {course.get('reward_points', 0)} EMRD\n"
                course_list += f"{course.get('description', 'Kursinhalt')}\n\n"
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "📖 Zur Academy",
                    web_app=WebAppInfo(url=MINIAPP_URL)
                )]
            ])
            await query.edit_message_text(course_list, reply_markup=keyboard, parse_mode="Markdown")


# ===== TEXT HANDLERS =====

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text handler"""
    user = update.effective_user
    text = update.message.text.lower()
    
    try:
        # Award points for interaction
        await award_points(user.id, "learning_interaction", update.effective_chat.id)
    except Exception:
        pass
    
    if any(word in text for word in ["help", "hilfe", "wie", "wo", "was"]):
        await cmd_help(update, context)
    elif any(word in text for word in ["kurse", "course", "lernen", "learn"]):
        await cmd_courses(update, context)
    elif any(word in text for word in ["fortschritt", "progress", "stats"]):
        await cmd_progress(update, context)
    elif any(word in text for word in ["belohnung", "reward", "verdient", "emrd"]):
        await cmd_rewards(update, context)
    else:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📖 Academy öffnen",
                web_app=WebAppInfo(url=MINIAPP_URL)
            )],
            [InlineKeyboardButton("❓ Hilfe", callback_data="learning_help")]
        ])
        await update.message.reply_text(
            "💡 Nutze die Befehle:\n"
            "/courses - Kurse anzeigen\n"
            "/progress - Fortschritt sehen\n"
            "/rewards - Belohnungen\n"
            "/help - Hilfe",
            reply_markup=keyboard
        )


# ===== REGISTRATION =====

def register_handlers(app: Application):
    """Register all handlers"""
    logger.info("[REGISTER] Registering learning handlers...")
    try:
        # Commands
        app.add_handler(CommandHandler("start", cmd_start), group=0)
        app.add_handler(CommandHandler("learning", cmd_start), group=0)
        app.add_handler(CommandHandler("academy", cmd_start), group=0)
        app.add_handler(CommandHandler("courses", cmd_courses), group=0)
        app.add_handler(CommandHandler("progress", cmd_progress), group=0)
        app.add_handler(CommandHandler("rewards", cmd_rewards), group=0)
        app.add_handler(CommandHandler("achievements", cmd_achievements), group=0)
        app.add_handler(CommandHandler("help", cmd_help), group=0)
        logger.debug("[REGISTER] Command handlers registered")
        
        # Callbacks
        app.add_handler(CallbackQueryHandler(button_callback), group=1)
        
        # Text messages
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler), group=2)
        
        logger.info("✅ [REGISTER] Learning handlers registered successfully")
    except Exception as e:
        logger.error(f"❌ [REGISTER] Failed to register handlers: {e}", exc_info=True)
