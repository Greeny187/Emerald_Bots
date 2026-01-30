"""Learning Bot - Mini-App & API Routes"""

import json
import logging
import os
from aiohttp import web
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import CommandHandler, Application
from datetime import datetime

logger = logging.getLogger("bot.learning.miniapp")

MINIAPP_URL = os.getenv(
    "MINIAPP_URL",
    "https://greeny187.github.io/EmeraldContentBots/miniapp/applearning.html"
)

try:
    from . import database
    from . import ai_content
except ImportError as e:
    logger.warning(f"Import warning: {e}")
    database = None
    ai_content = None


# ===== COMMANDS =====

async def cmd_open_learning(update, context):
    """Open Learning Mini-App"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "📖 Academy öffnen",
            web_app=WebAppInfo(url=MINIAPP_URL)
        )]
    ])
    await update.message.reply_text(
        "📚 **Emerald Academy**\n"
        "Starte dein Lernjourney jetzt!",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )


def register_miniapp(app: Application):
    """Register miniapp commands"""
    logger.info("[MINIAPP] Registering miniapp commands...")
    try:
        app.add_handler(CommandHandler("learning", cmd_open_learning))
        app.add_handler(CommandHandler("academy", cmd_open_learning))
        logger.info("✅ [MINIAPP] Miniapp commands registered")
    except Exception as e:
        logger.error(f"❌ [MINIAPP] Failed to register commands: {e}", exc_info=True)


# ===== API ROUTES =====

async def register_miniapp_routes(webapp: web.Application, app: Application):
    """Register HTTP routes for mini-app"""
    logger.info("[API_INIT] Registering learning API routes...")
    
    # ===== COURSES =====
    @webapp.post("/api/learning/courses")
    async def api_courses(request):
        """Get available courses"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            level = data.get("level")
            logger.debug(f"[API_COURSES] Request: user_id={user_id} level={level}")
            
            if not database:
                logger.error("[API_COURSES] Database unavailable")
                return web.json_response({"error": "Database unavailable"}, status=500)
            
            if user_id:
                courses = database.get_user_courses(user_id)
            else:
                courses = database.get_all_courses(level)
            
            logger.debug(f"[API_COURSES] Returned {len(courses or [])} courses")
            return web.json_response({
                "status": "ok",
                "data": [dict(c) for c in (courses or [])]
            })
        except Exception as e:
            logger.error(f"[API_COURSES] Error: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)
    
    
    @webapp.post("/api/learning/enroll")
    async def api_enroll(request):
        """Enroll in course"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            course_id = data.get("course_id")
            
            if not database or not user_id or not course_id:
                return web.json_response({"error": "Invalid data"}, status=400)
            
            success = database.enroll_course(user_id, course_id)
            return web.json_response({
                "status": "ok" if success else "error",
                "message": "Enrolled successfully" if success else "Enrollment failed"
            })
        except Exception as e:
            logger.error(f"Enroll API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== MODULES & CONTENT =====
    @webapp.post("/api/learning/modules")
    async def api_modules(request):
        """Get course modules"""
        try:
            data = await request.json()
            course_id = data.get("course_id")
            
            if not database or not course_id:
                return web.json_response({"error": "Invalid data"}, status=400)
            
            modules = database.get_course_modules(course_id)
            return web.json_response({
                "status": "ok",
                "data": [dict(m) for m in (modules or [])]
            })
        except Exception as e:
            logger.error(f"Modules API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== QUIZZES =====
    @webapp.post("/api/learning/quiz")
    async def api_quiz(request):
        """Get quiz or submit answer"""
        try:
            data = await request.json()
            action = data.get("action")
            
            if not database:
                return web.json_response({"error": "Database unavailable"}, status=500)
            
            if action == "get":
                module_id = data.get("module_id")
                quizzes = database.get_module_quizzes(module_id)
                return web.json_response({
                    "status": "ok",
                    "data": [dict(q) for q in (quizzes or [])]
                })
            
            elif action == "submit":
                user_id = data.get("user_id")
                quiz_id = data.get("quiz_id")
                answer = data.get("answer")
                time_taken = data.get("time_taken", 0)
                
                result = database.submit_quiz_answer(user_id, quiz_id, answer, time_taken)
                return web.json_response({
                    "status": "ok",
                    "is_correct": result.get("is_correct"),
                    "points": result.get("points"),
                    "explanation": result.get("explanation", "")
                })
            
            return web.json_response({"error": "Invalid action"}, status=400)
        except Exception as e:
            logger.error(f"Quiz API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== PROGRESS =====
    @webapp.post("/api/learning/progress")
    async def api_progress(request):
        """Get user progress"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            
            if not database or not user_id:
                return web.json_response({"error": "Invalid data"}, status=400)
            
            stats = database.get_course_stats(user_id)
            certificates = database.get_user_certificates(user_id)
            achievements = database.get_user_achievements(user_id)
            
            return web.json_response({
                "status": "ok",
                "stats": stats,
                "certificates": [dict(c) for c in (certificates or [])],
                "achievements": [dict(a) for a in (achievements or [])]
            })
        except Exception as e:
            logger.error(f"Progress API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    @webapp.post("/api/learning/mark_complete")
    async def api_mark_complete(request):
        """Mark module as complete"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            module_id = data.get("module_id")
            course_id = data.get("course_id")
            time_spent = data.get("time_spent", 0)
            
            if not database:
                return web.json_response({"error": "Database unavailable"}, status=500)
            
            # Mark module complete
            database.mark_module_complete(user_id, module_id, time_spent)
            
            # Update course progress
            progress = database.update_course_progress(user_id, course_id)
            
            # Update streak
            streak = database.update_streak(user_id)
            
            # Check for achievements
            achievements_unlocked = []
            
            if progress == 100:
                # Course completed!
                cert_hash = database.issue_certificate(user_id, course_id)
                database.unlock_achievement(
                    user_id,
                    f"course_complete_{course_id}",
                    "Kurs abgeschlossen! 🎓",
                    "🎓"
                )
                achievements_unlocked.append("Kurs abgeschlossen!")
            
            if streak == 7:
                database.unlock_achievement(user_id, "streak_7", "7-Tage Streak!", "🔥")
                achievements_unlocked.append("7-Tage Streak!")
            
            if streak == 30:
                database.unlock_achievement(user_id, "streak_30", "30-Tage Streak!", "🔥")
                achievements_unlocked.append("30-Tage Streak!")
            
            return web.json_response({
                "status": "ok",
                "progress": progress,
                "streak": streak,
                "achievements_unlocked": achievements_unlocked
            })
        except Exception as e:
            logger.error(f"Mark complete API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== CERTIFICATES =====
    @webapp.post("/api/learning/certificate")
    async def api_certificate(request):
        """Get or issue certificate"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            course_id = data.get("course_id")
            
            if not database:
                return web.json_response({"error": "Database unavailable"}, status=500)
            
            certificates = database.get_user_certificates(user_id)
            cert = next((c for c in certificates if c['course_id'] == course_id), None)
            
            if cert:
                return web.json_response({
                    "status": "ok",
                    "certificate": dict(cert)
                })
            else:
                return web.json_response({
                    "status": "not_earned",
                    "message": "Certificate not yet earned"
                })
        except Exception as e:
            logger.error(f"Certificate API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== REWARDS & CLAIMING =====
    @webapp.post("/api/learning/claim_reward")
    async def api_claim_reward(request):
        """Claim rewards"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            course_id = data.get("course_id")
            
            if not database:
                return web.json_response({"error": "Database unavailable"}, status=500)
            
            success = database.claim_reward(user_id, course_id)
            stats = database.get_course_stats(user_id)
            
            return web.json_response({
                "status": "ok" if success else "error",
                "emrd_balance": stats.get('emrd_earned', 0)
            })
        except Exception as e:
            logger.error(f"Claim reward API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== AI CONTENT GENERATION =====
    @webapp.post("/api/learning/generate_quiz")
    async def api_generate_quiz(request):
        """Generate AI quiz question"""
        try:
            data = await request.json()
            topic = data.get("topic")
            difficulty = data.get("difficulty", "medium")
            
            if not ai_content:
                return web.json_response({"error": "AI unavailable"}, status=500)
            
            question_data = await ai_content.generate_quiz_question(topic, difficulty)
            
            if question_data:
                return web.json_response({
                    "status": "ok",
                    "data": question_data
                })
            else:
                return web.json_response({"error": "Failed to generate"}, status=500)
        except Exception as e:
            logger.error(f"Generate quiz API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    @webapp.post("/api/learning/adaptive_content")
    async def api_adaptive_content(request):
        """Generate personalized content"""
        try:
            data = await request.json()
            user_id = data.get("user_id")
            weak_areas = data.get("weak_areas", [])
            
            if not ai_content:
                return web.json_response({"error": "AI unavailable"}, status=500)
            
            content = await ai_content.generate_adaptive_content(user_id, weak_areas)
            
            if content:
                return web.json_response({
                    "status": "ok",
                    "data": content
                })
            else:
                return web.json_response({"error": "Failed to generate"}, status=500)
        except Exception as e:
            logger.error(f"Adaptive content API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # ===== LEADERBOARD & STATS =====
    @webapp.get("/api/learning/stats/{user_id}")
    async def api_get_stats(request):
        """Get user learning stats"""
        try:
            user_id = int(request.match_info.get("user_id"))
            
            if not database:
                return web.json_response({"error": "Database unavailable"}, status=500)
            
            stats = database.get_course_stats(user_id)
            achievements = database.get_user_achievements(user_id)
            
            return web.json_response({
                "status": "ok",
                "stats": stats,
                "achievements_count": len(achievements or [])
            })
        except Exception as e:
            logger.error(f"Stats API error: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    
    # Health check
    @webapp.get("/api/learning/health")
    async def health_check(request):
        """Health check endpoint"""
        return web.json_response({"status": "healthy", "timestamp": str(datetime.now())})
    
    
    logger.info("Learning miniapp routes registered")
