"""Learning Bot - Database"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import os
from typing import Optional, List, Dict
import json
from datetime import datetime, timedelta

logger = logging.getLogger("bot.learning.database")

def get_db_connection():
    try:
        return psycopg2.connect(os.getenv("DATABASE_URL"))
    except Exception as e:
        logger.error(f"[DB_CONNECT] Database connection error: {e}", exc_info=True)
        return None


def init_all_schemas():
    """Initialize Learning database schemas"""
    logger.info("[DB_SCHEMA] Initializing learning database schemas...")
    conn = get_db_connection()
    if not conn:
        logger.error("[DB_SCHEMA] Failed to get database connection")
        return
    
    try:
        cur = conn.cursor()
        
        # Courses
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_courses (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                level VARCHAR(50),
                duration_minutes INTEGER,
                category VARCHAR(100),
                icon VARCHAR(10),
                reward_points INTEGER DEFAULT 100,
                total_modules INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Course Modules
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_modules (
                id SERIAL PRIMARY KEY,
                course_id INTEGER REFERENCES learning_courses(id) ON DELETE CASCADE,
                title VARCHAR(255),
                order_index INTEGER,
                content TEXT,
                video_url VARCHAR(500),
                duration_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # User Enrollments
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_enrollments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                course_id INTEGER REFERENCES learning_courses(id) ON DELETE CASCADE,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                progress_percentage INTEGER DEFAULT 0,
                UNIQUE(user_id, course_id)
            )
        """)
        
        # Module Progress
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_progress (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                module_id INTEGER REFERENCES learning_modules(id) ON DELETE CASCADE,
                completed_at TIMESTAMP,
                time_spent_seconds INTEGER DEFAULT 0,
                UNIQUE(user_id, module_id)
            )
        """)
        
        # AI-Generated Quizzes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_quizzes (
                id SERIAL PRIMARY KEY,
                module_id INTEGER REFERENCES learning_modules(id) ON DELETE CASCADE,
                topic VARCHAR(255) NOT NULL,
                question TEXT NOT NULL,
                question_type VARCHAR(20) DEFAULT 'multiple_choice',
                options JSONB,
                correct_answer VARCHAR(500),
                explanation TEXT,
                difficulty VARCHAR(20) DEFAULT 'medium',
                points INTEGER DEFAULT 10,
                ai_generated BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Quiz Results
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_quiz_results (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                quiz_id INTEGER REFERENCES learning_quizzes(id) ON DELETE CASCADE,
                module_id INTEGER REFERENCES learning_modules(id),
                course_id INTEGER REFERENCES learning_courses(id),
                answer TEXT,
                is_correct BOOLEAN,
                points_earned INTEGER,
                time_taken_seconds INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Certificates
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_certificates (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                course_id INTEGER REFERENCES learning_courses(id) ON DELETE CASCADE,
                issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                certificate_hash VARCHAR(255) UNIQUE,
                skill_tags JSONB
            )
        """)
        
        # Rewards & Progress
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_rewards (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                course_id INTEGER REFERENCES learning_courses(id),
                reward_type VARCHAR(50),
                points_earned INTEGER,
                emrd_earned NUMERIC(18,8),
                claimed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Learning Streaks
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_streaks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL UNIQUE,
                current_streak INTEGER DEFAULT 1,
                longest_streak INTEGER DEFAULT 1,
                last_activity_date DATE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # User Achievements
        cur.execute("""
            CREATE TABLE IF NOT EXISTS learning_achievements (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                achievement_type VARCHAR(50),
                achievement_name VARCHAR(255),
                achievement_icon VARCHAR(10),
                earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, achievement_type)
            )
        """)
        
        conn.commit()
        logger.info("✅ [DB_SCHEMA] Learning schemas initialized successfully")
    except Exception as e:
        logger.error(f"❌ [DB_SCHEMA] Schema initialization error: {e}", exc_info=True)
        conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ===== COURSE MANAGEMENT =====

def get_all_courses(level: Optional[str] = None) -> List[Dict]:
    """Get all available courses, optionally filtered by level"""
    logger.debug(f"[DB_GET_COURSES] Fetching courses: level={level}")
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if level:
            cur.execute("""
                SELECT * FROM learning_courses 
                WHERE level = %s 
                ORDER BY id
            """, (level,))
        else:
            cur.execute("SELECT * FROM learning_courses ORDER BY id")
        courses = cur.fetchall() or []
        logger.debug(f"[DB_GET_COURSES] Returned {len(courses)} courses")
        return courses
    except Exception as e:
        logger.error(f"[DB_GET_COURSES] Error: {e}", exc_info=True)
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def add_course(title: str, description: str, level: str, duration: int, category: str, icon: str, reward_points: int = 100) -> Optional[int]:
    """Add new course"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_courses (title, description, level, duration_minutes, category, icon, reward_points)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (title, description, level, duration, category, icon, reward_points))
        course_id = cur.fetchone()[0]
        conn.commit()
        return course_id
    except Exception as e:
        logger.error(f"Error adding course: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ===== ENROLLMENT =====

def enroll_course(user_id: int, course_id: int) -> bool:
    """Enroll user in course"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_enrollments (user_id, course_id, progress_percentage)
            VALUES (%s, %s, 0)
            ON CONFLICT (user_id, course_id) DO NOTHING
        """, (user_id, course_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error enrolling: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_user_courses(user_id: int) -> List[Dict]:
    """Get user's enrolled courses with progress"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT c.*, e.progress_percentage, e.started_at, e.completed_at
            FROM learning_courses c
            JOIN learning_enrollments e ON c.id = e.course_id
            WHERE e.user_id = %s
            ORDER BY e.started_at DESC
        """, (user_id,))
        return cur.fetchall() or []
    except Exception as e:
        logger.error(f"Error fetching courses: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_course_stats(user_id: int) -> Dict:
    """Get user's learning statistics"""
    logger.debug(f"[DB_GET_STATS] Retrieving stats for user {user_id}")
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Total courses
        cur.execute("""
            SELECT COUNT(*) as total FROM learning_enrollments WHERE user_id = %s
        """, (user_id,))
        total_courses = cur.fetchone()['total'] or 0
        
        # Completed courses
        cur.execute("""
            SELECT COUNT(*) as total FROM learning_enrollments 
            WHERE user_id = %s AND completed_at IS NOT NULL
        """, (user_id,))
        completed_courses = cur.fetchone()['total'] or 0
        
        # Total EMRD earned
        cur.execute("""
            SELECT SUM(emrd_earned) as total FROM learning_rewards 
            WHERE user_id = %s AND claimed_at IS NOT NULL
        """, (user_id,))
        emrd_earned = float(cur.fetchone()['total'] or 0)
        
        # Streak info
        cur.execute("""
            SELECT current_streak, longest_streak FROM learning_streaks WHERE user_id = %s
        """, (user_id,))
        streak_data = cur.fetchone() or {'current_streak': 0, 'longest_streak': 0}
        
        stats = {
            'total_courses': total_courses,
            'completed_courses': completed_courses,
            'emrd_earned': emrd_earned,
            'current_streak': streak_data['current_streak'],
            'longest_streak': streak_data['longest_streak']
        }
        logger.debug(f"[DB_GET_STATS] Stats retrieved: {stats}")
        return stats
    except Exception as e:
        logger.error(f"[DB_GET_STATS] Error: {e}", exc_info=True)
        return {}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ===== MODULES & QUIZZES =====

def get_course_modules(course_id: int) -> List[Dict]:
    """Get all modules for a course"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM learning_modules 
            WHERE course_id = %s 
            ORDER BY order_index
        """, (course_id,))
        return cur.fetchall() or []
    except Exception as e:
        logger.error(f"Error fetching modules: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def add_module(course_id: int, title: str, content: str, order_index: int, duration: int = 5) -> Optional[int]:
    """Add module to course"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_modules (course_id, title, content, order_index, duration_minutes)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (course_id, title, content, order_index, duration))
        module_id = cur.fetchone()[0]
        conn.commit()
        return module_id
    except Exception as e:
        logger.error(f"Error adding module: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_module_quizzes(module_id: int) -> List[Dict]:
    """Get all quizzes for a module"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, topic, question, question_type, options, difficulty, points
            FROM learning_quizzes
            WHERE module_id = %s
            ORDER BY id
        """, (module_id,))
        quizzes = cur.fetchall() or []
        # Parse JSONB options
        for quiz in quizzes:
            if isinstance(quiz['options'], dict):
                quiz['options'] = quiz['options']
        return quizzes
    except Exception as e:
        logger.error(f"Error fetching quizzes: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def add_quiz(module_id: int, topic: str, question: str, options: List[str], 
             correct_answer: str, explanation: str, difficulty: str = "medium", points: int = 10) -> Optional[int]:
    """Add quiz question"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_quizzes 
            (module_id, topic, question, question_type, options, correct_answer, explanation, difficulty, points, ai_generated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
            RETURNING id
        """, (module_id, topic, question, 'multiple_choice', 
              json.dumps(options), correct_answer, explanation, difficulty, points))
        quiz_id = cur.fetchone()[0]
        conn.commit()
        return quiz_id
    except Exception as e:
        logger.error(f"Error adding quiz: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def submit_quiz_answer(user_id: int, quiz_id: int, answer: str, time_taken: int = 0) -> Dict:
    """Submit quiz answer and get result"""
    conn = get_db_connection()
    if not conn:
        return {"is_correct": False, "points": 0}
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get quiz info
        cur.execute("""
            SELECT id, module_id, correct_answer, points, explanation
            FROM learning_quizzes WHERE id = %s
        """, (quiz_id,))
        quiz = cur.fetchone()
        
        if not quiz:
            return {"is_correct": False, "points": 0}
        
        is_correct = answer.lower() == quiz['correct_answer'].lower()
        points_earned = quiz['points'] if is_correct else 0
        
        # Save result
        cur.execute("""
            INSERT INTO learning_quiz_results 
            (user_id, quiz_id, module_id, answer, is_correct, points_earned, time_taken_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, quiz_id, quiz['module_id'], answer, is_correct, points_earned, time_taken))
        
        # Award points
        if is_correct:
            cur.execute("""
                INSERT INTO learning_rewards (user_id, reward_type, points_earned)
                VALUES (%s, 'quiz_correct', %s)
            """, (user_id, points_earned))
        
        conn.commit()
        
        return {
            "is_correct": is_correct,
            "points": points_earned,
            "explanation": quiz['explanation']
        }
    except Exception as e:
        logger.error(f"Error submitting quiz: {e}")
        return {"is_correct": False, "points": 0}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ===== PROGRESS TRACKING =====

def mark_module_complete(user_id: int, module_id: int, time_spent: int = 0) -> bool:
    """Mark module as completed"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_progress (user_id, module_id, completed_at, time_spent_seconds)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (user_id, module_id) DO UPDATE SET completed_at = CURRENT_TIMESTAMP
        """, (user_id, module_id, time_spent))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error marking complete: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def update_course_progress(user_id: int, course_id: int) -> int:
    """Calculate and update course progress percentage"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cur = conn.cursor()
        
        # Get total modules
        cur.execute("""
            SELECT COUNT(*) as total FROM learning_modules WHERE course_id = %s
        """, (course_id,))
        total_modules = cur.fetchone()[0] or 1
        
        # Get completed modules
        cur.execute("""
            SELECT COUNT(DISTINCT module_id) as completed FROM learning_progress
            WHERE user_id = %s AND module_id IN 
            (SELECT id FROM learning_modules WHERE course_id = %s)
        """, (user_id, course_id))
        completed_modules = cur.fetchone()[0] or 0
        
        progress = int((completed_modules / total_modules) * 100)
        
        # Update enrollment
        cur.execute("""
            UPDATE learning_enrollments 
            SET progress_percentage = %s,
                completed_at = CASE WHEN %s = 100 THEN CURRENT_TIMESTAMP ELSE completed_at END
            WHERE user_id = %s AND course_id = %s
        """, (progress, progress, user_id, course_id))
        
        conn.commit()
        return progress
    except Exception as e:
        logger.error(f"Error updating progress: {e}")
        return 0
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ===== CERTIFICATES =====

def issue_certificate(user_id: int, course_id: int, skills: List[str] = None) -> Optional[str]:
    """Issue course certificate"""
    import hashlib
    
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cert_hash = hashlib.sha256(
            f"{user_id}_{course_id}_{int(__import__('time').time())}".encode()
        ).hexdigest()
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_certificates (user_id, course_id, certificate_hash, skill_tags)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (certificate_hash) DO NOTHING
        """, (user_id, course_id, cert_hash, json.dumps(skills or [])))
        conn.commit()
        return cert_hash
    except Exception as e:
        logger.error(f"Error issuing certificate: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_user_certificates(user_id: int) -> List[Dict]:
    """Get user's certificates"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT c.*, lc.title FROM learning_certificates c
            JOIN learning_courses lc ON c.course_id = lc.id
            WHERE c.user_id = %s
            ORDER BY c.issued_at DESC
        """, (user_id,))
        return cur.fetchall() or []
    except Exception as e:
        logger.error(f"Error fetching certificates: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ===== REWARDS & STREAKS =====

def claim_reward(user_id: int, course_id: int, emrd_amount: float = 0) -> bool:
    """Claim learning reward"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE learning_rewards
            SET claimed_at = CURRENT_TIMESTAMP
            WHERE user_id = %s AND course_id = %s AND claimed_at IS NULL
        """, (user_id, course_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error claiming reward: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def update_streak(user_id: int) -> int:
    """Update user's learning streak"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        cur = conn.cursor()
        today = datetime.now().date()
        
        # Check if streak exists
        cur.execute("SELECT * FROM learning_streaks WHERE user_id = %s", (user_id,))
        streak_record = cur.fetchone()
        
        if not streak_record:
            cur.execute("""
                INSERT INTO learning_streaks (user_id, current_streak, last_activity_date)
                VALUES (%s, 1, %s)
            """, (user_id, today))
            new_streak = 1
        else:
            last_date = streak_record[4]  # last_activity_date
            current_streak = streak_record[1]
            
            if last_date == today:
                # Already counted today
                new_streak = current_streak
            elif (today - last_date).days == 1:
                # Continuous streak
                new_streak = current_streak + 1
                cur.execute("""
                    UPDATE learning_streaks
                    SET current_streak = %s, longest_streak = GREATEST(longest_streak, %s), last_activity_date = %s
                    WHERE user_id = %s
                """, (new_streak, new_streak, today, user_id))
            else:
                # Streak broken
                new_streak = 1
                cur.execute("""
                    UPDATE learning_streaks
                    SET current_streak = 1, last_activity_date = %s
                    WHERE user_id = %s
                """, (today, user_id))
        
        conn.commit()
        return new_streak
    except Exception as e:
        logger.error(f"Error updating streak: {e}")
        return 0
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def unlock_achievement(user_id: int, achievement_type: str, name: str, icon: str = "🏆") -> bool:
    """Unlock achievement"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO learning_achievements (user_id, achievement_type, achievement_name, achievement_icon)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, achievement_type) DO NOTHING
        """, (user_id, achievement_type, name, icon))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error unlocking achievement: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_user_achievements(user_id: int) -> List[Dict]:
    """Get user's achievements"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT achievement_type, achievement_name, achievement_icon, earned_at
            FROM learning_achievements
            WHERE user_id = %s
            ORDER BY earned_at DESC
        """, (user_id,))
        return cur.fetchall() or []
    except Exception as e:
        logger.error(f"Error fetching achievements: {e}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

