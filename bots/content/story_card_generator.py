"""
Story Share Card Generator
Generiert automatisch Branding-Bilder für Story-Sharing
"""

import logging
from typing import Optional, Tuple, Dict, Any
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import io
import os

logger = logging.getLogger(__name__)

# Emerald Brand Colors
COLORS = {
    "primary": "#10C7A0",      # Emerald Green
    "dark": "#0C1424",         # Dark Background
    "light": "#E7ECF4",        # Light Text
    "accent": "#22c55e",       # Secondary Green
}

# Card Templates
CARD_STYLES = {
    "group_bot": {
        "title": "Emerald Group Bot",
        "subtitle": "Automatisiert. Moderat. Intelligent.",
        "icon": "🤖",
        "bg_gradient": ("10C7A0", "0FA890")
    },
    "stats": {
        "title": "Meine Gruppen Stats",
        "subtitle": "Mit Emerald Analytics",
        "icon": "📊",
        "bg_gradient": ("10C7A0", "0899C8")
    },
    "content": {
        "title": "Emerald Content",
        "subtitle": "Automatische Posts & Moderation",
        "icon": "📝",
        "bg_gradient": ("10C7A0", "0FA890")
    },
    "emrd_rewards": {
        "title": "EMRD Rewards",
        "subtitle": "Verdiene Emerald Tokens",
        "icon": "💎",
        "bg_gradient": ("10C7A0", "22c55e")
    },
    "affiliate": {
        "title": "Emerald Affiliate",
        "subtitle": "Verdiene mit deinem Netzwerk",
        "icon": "🔥",
        "bg_gradient": ("22c55e", "10C7A0")
    }
}


def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    """Convert hex color to RGB tuple"""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def create_gradient_bg(
    width: int,
    height: int,
    color1: str,
    color2: str
) -> Image.Image:
    """Create a gradient background image"""
    
    img = Image.new('RGB', (width, height))
    pixels = img.load()
    
    rgb1 = hex_to_rgb(color1)
    rgb2 = hex_to_rgb(color2)
    
    for y in range(height):
        ratio = y / height
        r = int(rgb1[0] * (1 - ratio) + rgb2[0] * ratio)
        g = int(rgb1[1] * (1 - ratio) + rgb2[1] * ratio)
        b = int(rgb1[2] * (1 - ratio) + rgb2[2] * ratio)
        
        for x in range(width):
            pixels[x, y] = (r, g, b)
    
    return img


def generate_share_card(
    template: str,
    group_name: str = "Meine Gruppe",
    referral_link: str = "",
    user_display: str = "",
    width: int = 1080,
    height: int = 1920,
    meta: Optional[Dict[str, Any]] = None
    ) -> Optional[bytes]:
    """
    Generate a story share card image
    
    Returns: PNG bytes or None
    """
    
    try:
        if template not in CARD_STYLES:
            logger.error(f"Unknown template: {template}")
            return None
        
        style = CARD_STYLES[template]

        # Spezial: Stats-Card mit echten Zahlen (Story-Format)
        try:
            stats = (meta or {}).get("stats") if isinstance(meta, dict) else None
            if template == "stats" and isinstance(stats, dict):
                return _generate_stats_story_card(group_name, stats, width=width, height=height)
        except Exception:
            pass

        # Skalierung (Story-Format): halte Proportionen flexibel
        scale = height / 1920.0
        def S(px: int) -> int:
            return max(12, int(px * scale))
        
        # Create gradient background
        img = create_gradient_bg(width, height, style["bg_gradient"][0], style["bg_gradient"][1])
        draw = ImageDraw.Draw(img)
        
        # Try to load custom fonts, fallback to default
        try:
            title_font = ImageFont.truetype("DejaVuSans-Bold.ttf", S(74))
            subtitle_font = ImageFont.truetype("DejaVuSans.ttf", S(46))
            text_font = ImageFont.truetype("DejaVuSans.ttf", S(40))
            small_font = ImageFont.truetype("DejaVuSans.ttf", S(30))
        except:
            # Fallback to default
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
            text_font = ImageFont.load_default()
            small_font = ImageFont.load_default()
        
        # Add semi-transparent overlay for text legibility
        overlay = Image.new('RGBA', (width, height), (0, 0, 0, 100))
        img = Image.alpha_composite(
            img.convert('RGBA'),
            overlay
        ).convert('RGB')
        
        # Redraw with overlay applied
        draw = ImageDraw.Draw(img)
        
        # Text color
        text_color = hex_to_rgb(COLORS["light"])
        accent_color = hex_to_rgb(COLORS["primary"])
        
        # Draw emoji/icon
        emoji_y = int(height * 0.15)
        draw.text((width // 2, emoji_y), style["icon"], fill=text_color, font=title_font, anchor="mm")
        
        # Draw title
        title_y = int(height * 0.35)
        draw.text((width // 2, title_y), style["title"], fill=accent_color, font=title_font, anchor="mm")
        
        # Draw subtitle
        subtitle_y = int(height * 0.50)
        draw.text((width // 2, subtitle_y), style["subtitle"], fill=text_color, font=subtitle_font, anchor="mm")
        
        # Draw group/user info
        info_y = int(height * 0.65)
        info_text = f"📍 {group_name}"
        draw.text((width // 2, info_y), info_text, fill=text_color, font=text_font, anchor="mm")
        
        # Draw CTA
        cta_y = int(height * 0.78)
        cta_text = "💚 Emerald nutzen"
        draw.text((width // 2, cta_y), cta_text, fill=accent_color, font=text_font, anchor="mm")
        
        # Draw branding footer
        footer_text = "https://greeny187.github.io/EmeraldContentBots/ | powered by TON"

        footer_y = int(height * 0.92)
        draw.text((width // 2, footer_y), footer_text, fill=(200, 200, 200), font=small_font, anchor="mm")
        
        # Convert to PNG bytes
        img_bytes = io.BytesIO()
        img.save(img_bytes, format='PNG', quality=95)
        img_bytes.seek(0)
        
        logger.info(f"Share card generated: {template}")
        return img_bytes.getvalue()
        
    except Exception as e:
        logger.error(f"Generate card error: {e}")
        return None

def _fmt_short(n: Any) -> str:
    try:
        n = int(n or 0)
    except Exception:
        return str(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".replace(".0","")
    if n >= 1_000:
        return f"{n/1_000:.1f}k".replace(".0","")
    return str(n)

def _generate_stats_story_card(group_name: str, stats: Dict[str, Any], width: int = 1080, height: int = 1920) -> Optional[bytes]:
    try:
        # Basis
        img = create_gradient_bg(width, height, "10C7A0", "0899C8")
        overlay = Image.new('RGBA', (width, height), (0, 0, 0, 110))
        img = Image.alpha_composite(img.convert('RGBA'), overlay).convert('RGB')
        draw = ImageDraw.Draw(img)

        scale = height / 1920.0
        def S(px: int) -> int:
            return max(12, int(px * scale))

        try:
            title_font = ImageFont.truetype("DejaVuSans-Bold.ttf", S(74))
            sub_font   = ImageFont.truetype("DejaVuSans.ttf", S(44))
            stat_font  = ImageFont.truetype("DejaVuSans-Bold.ttf", S(84))
            label_font = ImageFont.truetype("DejaVuSans.ttf", S(36))
            small_font = ImageFont.truetype("DejaVuSans.ttf", S(28))
        except:
            title_font = sub_font = stat_font = label_font = small_font = ImageFont.load_default()
            
        text_color   = hex_to_rgb(COLORS["light"])
        accent_color = hex_to_rgb(COLORS["primary"])

        days = stats.get("days", 7)
        p_start = stats.get("period_start")
        p_end = stats.get("period_end")
        subtitle = f"Letzte {days} Tage"
        if p_start and p_end:
            subtitle = f"Letzte {days} Tage • {str(p_start)[5:]}–{str(p_end)[5:]}"

        # Header
        draw.text((width//2, int(height*0.14)), "📊 Gruppen-Statistik", fill=accent_color, font=title_font, anchor="mm")
        draw.text((width//2, int(height*0.22)), f"📍 {group_name}", fill=text_color, font=sub_font, anchor="mm")
        draw.text((width//2, int(height*0.28)), subtitle, fill=text_color, font=label_font, anchor="mm")

        # Werte
        msgs   = stats.get("messages_total", 0)
        aus    = stats.get("active_users", 0)
        joins  = stats.get("joins", 0)
        leaves = stats.get("leaves", 0)
        growth = int(joins or 0) - int(leaves or 0)

        boxes = [
            ("💬 Nachrichten", _fmt_short(msgs)),
            ("👥 Aktive Nutzer", _fmt_short(aus)),
            ("➕ Beitritte", _fmt_short(joins)),
            ("📈 Wachstum", f"{growth:+d}"),
        ]

        # 2x2 Grid
        grid_top = int(height*0.40)
        pad = S(28)
        bw = (width - pad*3)//2
        bh = S(260)
        for i,(label,val) in enumerate(boxes):
            col = i % 2
            row = i // 2
            x0 = pad + col*(bw+pad)
            y0 = grid_top + row*(bh+pad)
            x1 = x0 + bw
            y1 = y0 + bh
            draw.rounded_rectangle([x0,y0,x1,y1], radius=S(28), outline=accent_color, width=S(4))
            draw.text((x0 + bw//2, y0 + int(bh*0.40)), str(val), fill=accent_color, font=stat_font, anchor="mm")
            draw.text((x0 + bw//2, y0 + int(bh*0.78)), label, fill=text_color, font=label_font, anchor="mm")

        # Footer
        draw.text((width//2, int(height*0.88)), "Mit Emerald Analytics 💚", fill=text_color, font=sub_font, anchor="mm")
        draw.text((width//2, int(height*0.93)), "Support: t.me/EmeraldEcoSystem", fill=text_color, font=small_font, anchor="mm")

        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG", quality=95)
        img_bytes.seek(0)
        return img_bytes.getvalue()
    except Exception as e:
        logger.error(f"Generate stats story card error: {e}")
        return None

def generate_stats_card(
    group_name: str,
    member_count: int,
    message_count: int,
    user_display: str = "",
    width: int = 1200,
    height: int = 630
) -> Optional[bytes]:
    """
    Generate a statistics share card
    """
    
    try:
        # Create gradient background
        img = create_gradient_bg(width, height, "10C7A0", "0899C8")
        draw = ImageDraw.Draw(img)
        
        # Try to load fonts
        try:
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 56)
            stat_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 48)
            label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 32)
        except:
            title_font = ImageFont.load_default()
            stat_font = ImageFont.load_default()
            label_font = ImageFont.load_default()
        
        # Add overlay
        overlay = Image.new('RGBA', (width, height), (0, 0, 0, 100))
        img = Image.alpha_composite(
            img.convert('RGBA'),
            overlay
        ).convert('RGB')
        
        draw = ImageDraw.Draw(img)
        text_color = hex_to_rgb(COLORS["light"])
        accent_color = hex_to_rgb(COLORS["primary"])
        
        # Title
        draw.text((width // 2, 60), "📊 Meine Gruppen Stats", fill=accent_color, font=title_font, anchor="mm")
        
        # Group name
        draw.text((width // 2, 150), f"Gruppe: {group_name}", fill=text_color, font=label_font, anchor="mm")
        
        # Stats boxes
        box_width = 280
        box_height = 200
        box_y = 260
        
        # Box 1: Members
        box1_x = 150
        draw.rectangle([(box1_x, box_y), (box1_x + box_width, box_y + box_height)], outline=accent_color, width=3)
        draw.text((box1_x + box_width//2, box_y + 50), str(member_count), fill=accent_color, font=stat_font, anchor="mm")
        draw.text((box1_x + box_width//2, box_y + 130), "👥 Mitglieder", fill=text_color, font=label_font, anchor="mm")
        
        # Box 2: Messages
        box2_x = 460
        draw.rectangle([(box2_x, box_y), (box2_x + box_width, box_y + box_height)], outline=accent_color, width=3)
        draw.text((box2_x + box_width//2, box_y + 50), str(message_count), fill=accent_color, font=stat_font, anchor="mm")
        draw.text((box2_x + box_width//2, box_y + 130), "💬 Nachrichten", fill=text_color, font=label_font, anchor="mm")
        
        # Box 3: Emerald
        box3_x = 770
        draw.rectangle([(box3_x, box_y), (box3_x + box_width, box_y + box_height)], outline=accent_color, width=3)
        draw.text((box3_x + box_width//2, box_y + 50), "✨", fill=accent_color, font=stat_font, anchor="mm")
        draw.text((box3_x + box_width//2, box_y + 130), "Powered by Emerald", fill=text_color, font=label_font, anchor="mm")
        
        # Footer CTA
        draw.text((width // 2, height - 80), "💚 Nutze auch du Emerald!", fill=accent_color, font=label_font, anchor="mm")
        draw.text((width // 2, height - 30), "t.me/emerald_bot", fill=text_color, font=label_font, anchor="mm")
        
        # Convert to PNG
        img_bytes = io.BytesIO()
        img.save(img_bytes, format='PNG', quality=95)
        img_bytes.seek(0)
        
        return img_bytes.getvalue()
        
    except Exception as e:
        logger.error(f"Generate stats card error: {e}")
        return None


# Fallback: Simple HTML-based card generator if PIL not available
def generate_share_card_html(
    template: str,
    group_name: str = "Meine Gruppe",
    referral_link: str = ""
) -> str:
    """
    Fallback HTML card generator (for use in web/miniapp)
    """
    
    style = CARD_STYLES.get(template, CARD_STYLES["group_bot"])
    
    html = f"""
    <div style="
        width: 1200px;
        height: 630px;
        background: linear-gradient(135deg, #{style['bg_gradient'][0]} 0%, #{style['bg_gradient'][1]} 100%);
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        color: #E7ECF4;
        text-align: center;
        overflow: hidden;
        border-radius: 20px;
        box-shadow: 0 20px 60px rgba(0,0,0,0.3);
    ">
        <div style="font-size: 120px; margin-bottom: 30px;">{style['icon']}</div>
        
        <h1 style="
            font-size: 64px;
            margin: 0 0 20px;
            color: #10C7A0;
            font-weight: 900;
            text-shadow: 0 2px 10px rgba(0,0,0,0.3);
        ">{style['title']}</h1>
        
        <h2 style="
            font-size: 40px;
            margin: 0 0 40px;
            color: #E7ECF4;
            font-weight: 500;
            opacity: 0.95;
        ">{style['subtitle']}</h2>
        
        <div style="
            font-size: 36px;
            margin-bottom: 20px;
            color: #E7ECF4;
        ">📍 {group_name}</div>
        
        <div style="
            font-size: 32px;
            color: #10C7A0;
            font-weight: 700;
            margin-top: 30px;
        ">💚 Emerald nutzen</div>
        
        <div style="
            position: absolute;
            bottom: 30px;
            font-size: 20px;
            color: rgba(255,255,255,0.7);
        ">emerald.systems | powered by TON</div>
    </div>
    """
    
    return html
