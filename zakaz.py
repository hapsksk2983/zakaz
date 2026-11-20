import asyncio
import html
import logging
import os
from io import BytesIO
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove, InputMediaPhoto, BufferedInputFile,
)
from dotenv import load_dotenv

# CORSET — единый файл zakaz.py
# Установка: pip install aiogram aiosqlite python-dotenv
# Переменные окружения: BOT_TOKEN, ADMIN_ID, DB_PATH, FEEDBACK_URL

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)
DB_PATH = os.getenv("DB_PATH", "data/corset.db")
FEEDBACK_URL = os.getenv("FEEDBACK_URL", "").strip()
TAILOR_USERNAME = os.getenv("TAILOR_USERNAME", "").strip().lstrip("@")
TAILOR_URL = os.getenv("TAILOR_URL", "").strip() or (f"https://t.me/{TAILOR_USERNAME}" if TAILOR_USERNAME else "")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("corset")
router = Router()
_UI_LOCKS = {}

def ui_lock(uid):
    return _UI_LOCKS.setdefault(uid, asyncio.Lock())

MEASUREMENTS = [
    ("height", "Рост", "Укажите рост в сантиметрах. Например: 168"),
    ("bust", "Обхват груди", "Измерьте обхват груди по наиболее выступающим точкам. См."),
    ("underbust", "Обхват под грудью", "Измерьте обхват непосредственно под грудью. См."),
    ("waist", "Обхват талии", "Измерьте наиболее узкую часть талии. См."),
    ("belly", "Обхват живота", "Измерьте обхват живота примерно на 8–10 см ниже талии. См."),
    ("hips", "Обхват бёдер", "Измерьте наиболее выступающую часть бёдер. См."),
    ("bust_height", "Высота груди", "Укажите расстояние от плечевой точки до центра груди по инструкции портного. См."),
    ("bust_distance", "Расстояние между центрами груди", "Измерьте расстояние между центрами груди. См."),
    ("back_waist", "Длина спины до талии", "Измерьте длину от основания шеи до линии талии по спине. См."),
    ("front_waist", "Длина переда до талии", "Измерьте длину от плечевой точки через грудь до линии талии. См."),
    ("back_width", "Ширина спины", "Измерьте ширину спины между задними точками подмышечных впадин. См."),
    ("side_waist", "Расстояние от подмышки до талии", "Измерьте расстояние сбоку от подмышечной впадины до талии. См."),
    ("corset_length", "Желаемая длина корсета спереди", "Укажите желаемую длину изделия по центру переда. См."),
]

class OrderFlow(StatesGroup):
    consent = State()
    model = State()
    fabric = State()
    measurements = State()
    photos = State()
    notes = State()

class AddItemFlow(StatesGroup):
    name = State()
    description = State()
    photo = State()

class ProfileFlow(StatesGroup):
    name = State()
    phone = State()

class TailorPriceFlow(StatesGroup):
    total = State()
    deposit = State()
    details = State()

class ClientPaymentFlow(StatesGroup):
    proof = State()

@asynccontextmanager
async def connect_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA foreign_keys=ON")
        yield db
    finally:
        await db.close()

async def init_db():
    async with connect_db() as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'client', phone TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS models(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
            description TEXT DEFAULT '', photo_id TEXT DEFAULT '', active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS fabrics(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
            description TEXT DEFAULT '', photo_id TEXT DEFAULT '', active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS orders(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            model_id INTEGER NOT NULL, fabric_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'Новый', notes TEXT DEFAULT '',
            total_price INTEGER DEFAULT 0, deposit_amount INTEGER DEFAULT 0,
            payment_details TEXT DEFAULT '', payment_state TEXT DEFAULT 'not_set',
            payment_proof_file_id TEXT DEFAULT '', payment_proof_text TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(model_id) REFERENCES models(id),
            FOREIGN KEY(fabric_id) REFERENCES fabrics(id)
        );
        CREATE TABLE IF NOT EXISTS measurements(
            order_id INTEGER PRIMARY KEY,
            height REAL DEFAULT 0, bust REAL DEFAULT 0, underbust REAL DEFAULT 0,
            waist REAL DEFAULT 0, belly REAL DEFAULT 0, hips REAL DEFAULT 0,
            bust_height REAL DEFAULT 0, bust_distance REAL DEFAULT 0,
            back_waist REAL DEFAULT 0, front_waist REAL DEFAULT 0,
            back_width REAL DEFAULT 0, side_waist REAL DEFAULT 0,
            corset_length REAL DEFAULT 0,
            FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS order_photos(
            id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL,
            file_id TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS ui_messages(
            user_id INTEGER PRIMARY KEY, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL
        );
        """)
        # Совместимость с базой прежней версии.
        cols = {r["name"] for r in await (await db.execute("PRAGMA table_info(users)")).fetchall()}
        if "phone" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN phone TEXT DEFAULT ''")
        ocols = {r["name"] for r in await (await db.execute("PRAGMA table_info(orders)")).fetchall()}
        order_migrations = {
            "total_price": "INTEGER DEFAULT 0",
            "deposit_amount": "INTEGER DEFAULT 0",
            "payment_details": "TEXT DEFAULT ''",
            "payment_state": "TEXT DEFAULT 'not_set'",
            "payment_proof_file_id": "TEXT DEFAULT ''",
            "payment_proof_text": "TEXT DEFAULT ''",
        }
        for name, definition in order_migrations.items():
            if name not in ocols:
                await db.execute(f"ALTER TABLE orders ADD COLUMN {name} {definition}")
        mcols = {r["name"] for r in await (await db.execute("PRAGMA table_info(measurements)")).fetchall()}
        for name in [x[0] for x in MEASUREMENTS]:
            if name not in mcols:
                await db.execute(f"ALTER TABLE measurements ADD COLUMN {name} REAL DEFAULT 0")
        defaults = {
            "models": [("Классический overbust", "Корсет с закрытием груди."), ("Классический underbust", "Корсет под грудью."), ("Удлинённый корсет", "Модель с удлинённой линией низа.")],
            "fabrics": [("Атлас", "Гладкая ткань с лёгким блеском."), ("Хлопок", "Матовая ткань с натуральной фактурой."), ("Бархат", "Мягкая ткань с выразительной фактурой.")],
        }
        for table, items in defaults.items():
            count = await (await db.execute(f"SELECT COUNT(*) n FROM {table}")).fetchone()
            if count["n"] == 0:
                await db.executemany(f"INSERT INTO {table}(name,description) VALUES(?,?)", items)
        await db.commit()

async def save_user(user):
    role = "tailor" if ADMIN_ID and user.id == ADMIN_ID else "client"
    async with connect_db() as db:
        await db.execute("""INSERT INTO users(user_id,username,full_name,role) VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
        full_name=excluded.full_name, role=excluded.role""", (user.id, user.username, user.full_name, role))
        await db.commit()

async def get_user(uid):
    async with connect_db() as db:
        row = await (await db.execute("SELECT * FROM users WHERE user_id=?", (uid,))).fetchone()
        return dict(row) if row else None

async def catalog(table):
    if table not in ("models", "fabrics"):
        raise ValueError("Unknown catalog")
    async with connect_db() as db:
        rows = await (await db.execute(f"SELECT id,name,description,photo_id FROM {table} WHERE active=1 ORDER BY id")).fetchall()
        return [dict(r) for r in rows]

async def add_catalog_item(table, name, description, photo_id):
    async with connect_db() as db:
        await db.execute(f"INSERT INTO {table}(name,description,photo_id) VALUES(?,?,?)", (name, description, photo_id))
        await db.commit()

async def create_order(uid, model_id, fabric_id, measures, photos, notes):
    async with connect_db() as db:
        cur = await db.execute("INSERT INTO orders(user_id,model_id,fabric_id,notes) VALUES(?,?,?,?)", (uid, model_id, fabric_id, notes))
        oid = cur.lastrowid
        measure_cols = [x[0] for x in MEASUREMENTS]
        measure_values = list(measures)
        existing_cols = {r["name"] for r in await (await db.execute("PRAGMA table_info(measurements)")).fetchall()}
        # Старые версии базы содержали поля front/back с обязательным заполнением.
        if "front" in existing_cols:
            measure_cols.append("front")
            measure_values.append(measures[9])
        if "back" in existing_cols:
            measure_cols.append("back")
            measure_values.append(measures[8])
        cols = ",".join(measure_cols)
        marks = ",".join("?" for _ in measure_cols)
        await db.execute(f"INSERT INTO measurements(order_id,{cols}) VALUES(?,{marks})", (oid, *measure_values))
        if photos:
            await db.executemany("INSERT INTO order_photos(order_id,file_id) VALUES(?,?)", [(oid, p) for p in photos])
        await db.commit()
        return oid

async def get_order(oid):
    async with connect_db() as db:
        row = await (await db.execute("""SELECT o.*,u.user_id,u.full_name,u.username,u.phone,
        m.name model_name,f.name fabric_name,x.* FROM orders o
        JOIN users u ON u.user_id=o.user_id JOIN models m ON m.id=o.model_id
        JOIN fabrics f ON f.id=o.fabric_id JOIN measurements x ON x.order_id=o.id
        WHERE o.id=?""", (oid,))).fetchone()
        if not row:
            return None
        data = dict(row)
        photos = await (await db.execute("SELECT file_id FROM order_photos WHERE order_id=?", (oid,))).fetchall()
        data["photos"] = [p["file_id"] for p in photos]
        return data

async def get_orders(uid=None, limit=30):
    async with connect_db() as db:
        if uid is None:
            q = """SELECT o.id,o.status,o.created_at,u.full_name,m.name model_name
            FROM orders o JOIN users u ON u.user_id=o.user_id JOIN models m ON m.id=o.model_id
            ORDER BY o.id DESC LIMIT ?"""
            args = (limit,)
        else:
            q = """SELECT o.id,o.status,o.created_at,m.name model_name FROM orders o
            JOIN models m ON m.id=o.model_id WHERE o.user_id=? ORDER BY o.id DESC LIMIT ?"""
            args = (uid, limit)
        return [dict(r) for r in await (await db.execute(q, args)).fetchall()]

async def save_order_quote(oid, total, deposit, details):
    async with connect_db() as db:
        await db.execute("""UPDATE orders SET total_price=?, deposit_amount=?, payment_details=?,
            payment_state='awaiting', status='Ожидает предоплату' WHERE id=?""",
            (total, deposit, details, oid))
        await db.commit()

async def save_payment_proof(oid, file_id="", proof_text=""):
    async with connect_db() as db:
        await db.execute("UPDATE orders SET payment_proof_file_id=?, payment_proof_text=?, payment_state='proof_sent' WHERE id=?",
                         (file_id, proof_text, oid))
        await db.commit()

async def confirm_deposit(oid):
    async with connect_db() as db:
        cur = await db.execute("SELECT payment_state FROM orders WHERE id=?", (oid,))
        row = await cur.fetchone()
        if not row or row["payment_state"] != "proof_sent":
            return False
        await db.execute("UPDATE orders SET payment_state='paid', status='В работе' WHERE id=?", (oid,))
        await db.commit()
        return True

async def update_status(oid, status):
    """Атомарно обновляет статус; возвращает False, если он уже такой."""
    async with connect_db() as db:
        cur = await db.execute("SELECT status FROM orders WHERE id=?", (oid,))
        row = await cur.fetchone()
        if not row:
            return None
        if row["status"] == status:
            return False
        await db.execute("UPDATE orders SET status=? WHERE id=?", (status, oid))
        await db.commit()
        return True

async def update_profile(uid, field, value):
    if field not in ("full_name", "phone"):
        return
    async with connect_db() as db:
        await db.execute(f"UPDATE users SET {field}=? WHERE user_id=?", (value, uid))
        await db.commit()

async def delete_my_data(uid):
    async with connect_db() as db:
        await db.execute("DELETE FROM orders WHERE user_id=?", (uid,))
        await db.execute("DELETE FROM users WHERE user_id=?", (uid,))
        await db.execute("DELETE FROM ui_messages WHERE user_id=?", (uid,))
        await db.commit()

async def remember_ui(uid, chat_id, message_id):
    async with connect_db() as db:
        await db.execute("""INSERT INTO ui_messages(user_id,chat_id,message_id) VALUES(?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id,message_id=excluded.message_id""", (uid, chat_id, message_id))
        await db.commit()

async def get_ui(uid):
    async with connect_db() as db:
        r = await (await db.execute("SELECT chat_id,message_id FROM ui_messages WHERE user_id=?", (uid,))).fetchone()
        return dict(r) if r else None

async def show_screen(bot, uid, chat_id, text, markup=None):
    # На одного пользователя — одна операция изменения главного экрана за раз.
    async with ui_lock(uid):
        old = await get_ui(uid)
        if old and old["chat_id"] == chat_id:
            try:
                await bot.edit_message_text(
                    text, chat_id=chat_id, message_id=old["message_id"],
                    reply_markup=markup, disable_web_page_preview=True,
                )
                return old["message_id"]
            except Exception as exc:
                if "message is not modified" in str(exc).lower():
                    return old["message_id"]
                # Если прежний главный экран был фотографией, удаляем его:
                # меню клиента и портного всегда должно быть обычным текстовым сообщением.
                log.info("Заменяем старый главный экран пользователя %s: %s", uid, exc)
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=old["message_id"])
                except Exception as delete_exc:
                    log.info("Не удалось удалить старый экран пользователя %s: %s", uid, delete_exc)
        sent = await bot.send_message(
            chat_id, text, reply_markup=markup, disable_web_page_preview=True
        )
        await remember_ui(uid, chat_id, sent.message_id)
        return sent.message_id

async def delete_user_message(message: Message):
    try:
        await message.delete()
    except Exception:
        pass

def esc(v):
    return html.escape(str(v or ""))

def is_admin(uid):
    return bool(ADMIN_ID and uid == ADMIN_ID)

def client_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👗 Заказать корсет", callback_data="client:new")],
        [InlineKeyboardButton(text="📦 Мои заказы", callback_data="client:orders"), InlineKeyboardButton(text="📏 Мои мерки", callback_data="client:measures")],
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="client:profile")],
        [InlineKeyboardButton(text="🗑 Удалить мои данные", callback_data="client:delete")],
        [InlineKeyboardButton(text="🧵 Каталог моделей", callback_data="catalog:models"), InlineKeyboardButton(text="🎨 Каталог тканей", callback_data="catalog:fabrics")],
        [InlineKeyboardButton(text="ℹ️ О проекте", callback_data="client:about")],
        *([[InlineKeyboardButton(text="💬 Связаться с портным", url=TAILOR_URL)]] if TAILOR_URL else []),
    ])

def tailor_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Все заказы", callback_data="tailor:orders")],
        [InlineKeyboardButton(text="👗 Каталог моделей", callback_data="catalog:models")],
        [InlineKeyboardButton(text="🧵 Управление моделями", callback_data="tailor:add:model")],
        [InlineKeyboardButton(text="🎨 Управление тканями", callback_data="tailor:add:fabric")],
        [InlineKeyboardButton(text="↩️ Главное меню", callback_data="home")],
    ])

def back_menu():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="↩️ В меню", callback_data="home")]])

def wizard_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Назад", callback_data="wizard:back")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
    ])

def choose_keyboard(items, prefix):
    rows = [[InlineKeyboardButton(text=x["name"][:60], callback_data=f"{prefix}:{x['id']}")] for x in items]
    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="wizard:back")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def status_menu(oid, current="Новый", payment_state="not_set"):
    def mark(label, value):
        return ("🔘 " if current == value else "") + label
    rows = [
        [InlineKeyboardButton(text=mark("🆕 Новый", "Новый"), callback_data=f"status:{oid}:Новый")],
        [InlineKeyboardButton(text=mark("💰 Ожидает предоплату", "Ожидает предоплату"), callback_data=f"status:{oid}:Ожидает предоплату")],
        [InlineKeyboardButton(text=mark("🧵 В работе", "В работе"), callback_data=f"status:{oid}:В работе")],
        [InlineKeyboardButton(text=mark("📐 Лекала готовы", "Лекала готовы"), callback_data=f"status:{oid}:Лекала готовы")],
        [InlineKeyboardButton(text=mark("✂️ Пошив", "Пошив"), callback_data=f"status:{oid}:Пошив")],
        [InlineKeyboardButton(text=mark("✅ Завершён", "Завершён"), callback_data=f"status:{oid}:Завершён")],
        [InlineKeyboardButton(text="💰 Рассчитать стоимость и предоплату", callback_data=f"tailor:price:{oid}")],
    ]
    if payment_state == "proof_sent":
        rows.append([InlineKeyboardButton(text="✅ Предоплата получена", callback_data=f"tailor:deposit:{oid}")])
    rows += [[InlineKeyboardButton(text="↩️ К заказам", callback_data="tailor:orders")],
             [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def client_status_menu(oid, completed=False):
    rows = [[InlineKeyboardButton(text="📦 Открыть заказ", callback_data=f"order:view:{oid}")],
            [InlineKeyboardButton(text="↩️ Мои заказы", callback_data="client:orders")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")]]
    if completed and FEEDBACK_URL:
        rows.insert(0, [InlineKeyboardButton(text="⭐ Оставить отзыв", url=FEEDBACK_URL)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def order_details_text(order, heading="📋 <b>Заказ</b>"):
    lines = [
        f"{heading} #{order['id']}",
        f"Клиент: {esc(order['full_name'])}",
        f"Username: @{esc(order['username']) if order['username'] else 'не указан'}",
        f"Телефон: {esc(order['phone']) or 'не указан'}",
        f"Модель: {esc(order['model_name'])}",
        f"Ткань: {esc(order['fabric_name'])}",
        f"Статус: <b>{esc(order['status'])}</b>",
        f"Стоимость: {int(order.get('total_price') or 0):,} ₽".replace(",", " ") if int(order.get('total_price') or 0) else "Стоимость: ещё не рассчитана",
        f"Предоплата: {int(order.get('deposit_amount') or 0):,} ₽".replace(",", " ") if int(order.get('deposit_amount') or 0) else "Предоплата: ещё не указана",
        f"Оплата: {esc({'not_set': 'не рассчитана', 'awaiting': 'ожидает предоплату', 'proof_sent': 'подтверждение отправлено', 'paid': 'предоплата подтверждена'}.get(order.get('payment_state'), 'неизвестно'))}",
        "", "<b>Мерки, см:</b>",
    ]
    lines.extend(f"{esc(label)}: {order.get(key, 0)}" for key, label, _ in MEASUREMENTS)
    lines.extend(["", f"Пожелания: {esc(order['notes']) or 'нет'}", f"Фотографий: {len(order['photos'])}"])
    return "\n".join(lines)

def order_album_caption(order):
    # Весь заказ помещается в подпись первого снимка альбома.
    lines = [
        f"🆕 <b>Заказ #{order['id']}</b>",
        f"Клиент: {esc(order['full_name'])}",
        f"Контакт: @{esc(order['username']) if order['username'] else 'не указан'} · {esc(order['phone']) or 'телефон не указан'}",
        f"Модель: {esc(order['model_name'])}",
        f"Ткань: {esc(order['fabric_name'])}",
        "<b>Мерки, см:</b>",
    ]
    lines.extend(f"{esc(label)}: {order.get(key, 0)}" for key, label, _ in MEASUREMENTS)
    notes = esc((order['notes'] or '')[:80]) or 'нет'
    lines.append(f"Пожелания: {notes}")
    return "\n".join(lines)

def parse_measure(value):
    try:
        n = float(value.replace(",", ".").strip())
        return n if 1 <= n <= 250 else None
    except (ValueError, AttributeError):
        return None

async def render_home(bot, uid, chat_id):
    if is_admin(uid):
        return await show_screen(bot, uid, chat_id, "🧵 <b>Панель портного</b>\n\nУправляйте заказами, каталогом моделей и тканей.", tailor_menu())
    user = await get_user(uid)
    name = esc(user["full_name"] if user else "Клиент")
    orders = await get_orders(uid, 1)
    last = f"\nПоследний заказ: <b>#{orders[0]['id']} · {esc(orders[0]['status'])}</b>" if orders else "\nУ вас пока нет заказов."
    return await show_screen(bot, uid, chat_id, f"🧵 <b>CORSET</b>\n\nЗдравствуйте, {name}!\nИндивидуальный пошив корсетов — от выбора модели до готового изделия.{last}\n\nВыберите нужный раздел:", client_menu())

async def make_order_image(bot, file_ids):
    """Объединяет до пяти снимков в один файл, чтобы карточка заказа была одним сообщением."""
    if not file_ids:
        return None
    if len(file_ids) == 1:
        return file_ids[0]
    try:
        from PIL import Image, ImageOps, ImageDraw
        tiles = []
        for file_id in file_ids[:5]:
            tg_file = await bot.get_file(file_id)
            data = BytesIO()
            await bot.download_file(tg_file.file_path, destination=data)
            data.seek(0)
            img = Image.open(data).convert("RGB")
            img.thumbnail((720, 900))
            canvas = Image.new("RGB", (740, 940), "white")
            x = (740 - img.width) // 2
            y = (900 - img.height) // 2
            canvas.paste(img, (x, y))
            draw = ImageDraw.Draw(canvas)
            draw.text((12, 910), f"Фото {len(tiles) + 1}", fill="black")
            tiles.append(canvas)
        cols = 2 if len(tiles) > 1 else 1
        rows = (len(tiles) + cols - 1) // cols
        sheet = Image.new("RGB", (cols * 740, rows * 940), "#eeeeee")
        for i, tile in enumerate(tiles):
            sheet.paste(tile, ((i % cols) * 740, (i // cols) * 940))
        out = BytesIO()
        sheet.save(out, format="JPEG", quality=88, optimize=True)
        return BufferedInputFile(out.getvalue(), filename="order_photos.jpg")
    except Exception:
        log.exception("Не удалось собрать фотографии в одно изображение")
        # Без Pillow можно гарантировать одно сообщение только с первым снимком.
        return file_ids[0]

async def send_order_to_tailor(bot, oid):
    if not ADMIN_ID:
        log.warning("ADMIN_ID не задан; уведомление о заказе не отправлено")
        return
    order = await get_order(oid)
    if not order:
        return
    caption = order_album_caption(order)
    try:
        # Каждому заказу отправляется отдельное сообщение; предыдущие не удаляются.
        if order["photos"]:
            media = await make_order_image(bot, order["photos"])
            sent = await bot.send_photo(
                ADMIN_ID, media, caption=caption[:1024],
                reply_markup=status_menu(oid, order["status"], order.get("payment_state", "not_set")), parse_mode=ParseMode.HTML,
            )
        else:
            sent = await bot.send_message(
                ADMIN_ID, order_details_text(order, "🆕 <b>Новый заказ</b>"),
                reply_markup=status_menu(oid, order["status"], order.get("payment_state", "not_set")),
            )
        # Не записываем карточку заказа как главное меню портного.
    except Exception:
        log.exception("Не удалось отправить карточку заказа #%s портному", oid)

@router.message(CommandStart())
async def start(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    await save_user(message.from_user)
    await delete_user_message(message)
    await render_home(bot, message.from_user.id, message.chat.id)

@router.message(Command("myid"))
async def myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@router.message(Command("cancel"))
async def cancel(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    await delete_user_message(message)
    await render_home(bot, message.from_user.id, message.chat.id)

@router.callback_query(F.data == "home")
async def home(call: CallbackQuery, state: FSMContext, bot: Bot):
    await state.clear()
    await render_home(bot, call.from_user.id, call.message.chat.id)
    await call.answer()

@router.callback_query(F.data == "wizard:back")
async def wizard_back(call: CallbackQuery, state: FSMContext, bot: Bot):
    current = await state.get_state()
    data = await state.get_data()
    if current == OrderFlow.consent.state:
        await state.clear()
        await render_home(bot, call.from_user.id, call.message.chat.id)
    elif current == OrderFlow.model.state:
        await state.set_state(OrderFlow.consent)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Продолжить", callback_data="consent:yes")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ])
        await show_screen(bot, call.from_user.id, call.message.chat.id,
            "Подтвердите согласие на передачу портному мерок и фотографий для оформления заказа.", kb)
    elif current == OrderFlow.fabric.state:
        items = await catalog("models")
        await state.set_state(OrderFlow.model)
        await show_screen(bot, call.from_user.id, call.message.chat.id,
            "Выберите модель корсета:", choose_keyboard(items, "model"))
    elif current == OrderFlow.measurements.state:
        idx = int(data.get("measure_index", 0))
        measures = list(data.get("measures", []))
        if idx > 0 and measures:
            measures.pop()
            idx -= 1
            await state.update_data(measures=measures, measure_index=idx)
            await show_screen(bot, call.from_user.id, call.message.chat.id,
                f"📏 <b>Снятие мерок · {idx + 1} из {len(MEASUREMENTS)}</b>\n\n{esc(MEASUREMENTS[idx][1])}\n{esc(MEASUREMENTS[idx][2])}\n\nВведите значение в сантиметрах.", wizard_keyboard())
        else:
            items = await catalog("fabrics")
            await state.set_state(OrderFlow.fabric)
            await show_screen(bot, call.from_user.id, call.message.chat.id,
                "Выберите ткань:", choose_keyboard(items, "fabric"))
    elif current == OrderFlow.photos.state:
        measures = list(data.get("measures", []))
        if measures:
            measures.pop()
        idx = len(measures)
        await state.update_data(measures=measures, measure_index=idx)
        await state.set_state(OrderFlow.measurements)
        await show_screen(bot, call.from_user.id, call.message.chat.id,
            f"📏 <b>Снятие мерок · {idx + 1} из {len(MEASUREMENTS)}</b>\n\n{esc(MEASUREMENTS[idx][1])}\n{esc(MEASUREMENTS[idx][2])}\n\nВведите значение в сантиметрах.", wizard_keyboard())
    elif current == OrderFlow.notes.state:
        await state.set_state(OrderFlow.photos)
        await show_screen(bot, call.from_user.id, call.message.chat.id,
            "📸 Отправьте до 5 фотографий или пропустите этот шаг.", photos_keyboard())
    else:
        await state.clear()
        await render_home(bot, call.from_user.id, call.message.chat.id)
    await call.answer()

@router.callback_query(F.data == "client:about")
async def about(call: CallbackQuery, bot: Bot):
    await show_screen(bot, call.from_user.id, call.message.chat.id,
        "🧵 <b>О проекте CORSET</b>\n\nСервис индивидуального пошива корсетов. Вы выбираете модель и ткань, заполняете мерки и отправляете фотографии. Портной проверяет данные и связывается с вами по заказу.", back_menu())
    await call.answer()

@router.callback_query(F.data == "client:new")
async def begin_order(call: CallbackQuery, state: FSMContext, bot: Bot):
    await state.clear()
    await state.set_state(OrderFlow.consent)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Продолжить", callback_data="consent:yes")],
        [InlineKeyboardButton(text="↩️ Назад", callback_data="home")],
    ])
    await show_screen(bot, call.from_user.id, call.message.chat.id,
        "Перед оформлением заказа подтвердите, что согласны передать портному свои мерки и фотографии для подготовки индивидуального изделия.\n\nОтправляйте только те фотографии, которыми готовы поделиться. Ознакомьтесь с политикой обработки персональных данных владельца сервиса.", kb)
    await call.answer()

@router.callback_query(F.data == "consent:yes", OrderFlow.consent)
async def consent_yes(call: CallbackQuery, state: FSMContext, bot: Bot):
    items = await catalog("models")
    if not items:
        await state.clear()
        await show_screen(bot, call.from_user.id, call.message.chat.id, "Каталог моделей пока пуст.", back_menu())
    else:
        await state.set_state(OrderFlow.model)
        await show_screen(bot, call.from_user.id, call.message.chat.id, "<b>Шаг 1 из 2.</b> Выберите модель корсета:", choose_keyboard(items, "model"))
    await call.answer()

@router.callback_query(F.data.startswith("model:"), OrderFlow.model)
async def choose_model(call: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        mid = int(call.data.split(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный выбор", show_alert=True)
    await state.update_data(model_id=mid)
    items = await catalog("fabrics")
    if not items:
        await state.clear()
        await show_screen(bot, call.from_user.id, call.message.chat.id, "Каталог тканей пока пуст.", back_menu())
    else:
        await state.set_state(OrderFlow.fabric)
        await show_screen(bot, call.from_user.id, call.message.chat.id, "<b>Шаг 2 из 2.</b> Выберите ткань:", choose_keyboard(items, "fabric"))
    await call.answer()

@router.callback_query(F.data.startswith("fabric:"), OrderFlow.fabric)
async def choose_fabric(call: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        fid = int(call.data.split(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный выбор", show_alert=True)
    await state.update_data(fabric_id=fid, measures=[], measure_index=0, photos=[])
    await state.set_state(OrderFlow.measurements)
    await show_screen(bot, call.from_user.id, call.message.chat.id,
        "📏 <b>Снятие мерок · 1 из 13</b>\n\n" + MEASUREMENTS[0][2] + "\n\nВводите значение числом.", wizard_keyboard())
    await call.answer()

@router.message(OrderFlow.measurements)
async def measurement_input(message: Message, state: FSMContext, bot: Bot):
    value = parse_measure(message.text)
    await delete_user_message(message)
    if value is None:
        return await show_screen(bot, message.from_user.id, message.chat.id, "Введите значение от 1 до 250 см числом. Попробуйте ещё раз.")
    data = await state.get_data()
    idx = int(data.get("measure_index", 0))
    measures = list(data.get("measures", []))
    if idx >= len(MEASUREMENTS):
        await state.clear()
        return await render_home(bot, message.from_user.id, message.chat.id)
    measures.append(value)
    idx += 1
    await state.update_data(measures=measures, measure_index=idx)
    if idx < len(MEASUREMENTS):
        _, label, prompt = MEASUREMENTS[idx]
        text = f"📏 <b>Снятие мерок · {idx+1} из {len(MEASUREMENTS)}</b>\n\n<b>{esc(label)}</b>\n{esc(prompt)}\n\nВведите значение в сантиметрах."
        await show_screen(bot, message.from_user.id, message.chat.id, text, wizard_keyboard())
    else:
        await state.set_state(OrderFlow.photos)
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Пропустить фотографии", callback_data="photos:skip")], [InlineKeyboardButton(text="Отмена", callback_data="home")]])
        await show_screen(bot, message.from_user.id, message.chat.id,
            "✅ Все 13 мерок заполнены.\n\n📸 Теперь отправьте до 5 фотографий: спереди, сбоку и при необходимости сзади. Фото нужны портному для оценки посадки и особенностей фигуры. Можно пропустить этот шаг.", kb)

@router.message(OrderFlow.photos, F.photo)
async def collect_photo(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    photos = list(data.get("photos", []))
    if len(photos) >= 5:
        await delete_user_message(message)
        return await show_screen(bot, message.from_user.id, message.chat.id, "Достигнут лимит — 5 фотографий. Нажмите «Завершить загрузку» или продолжите без новых фото.", photos_keyboard())
    photos.append(message.photo[-1].file_id)
    await state.update_data(photos=photos)
    await delete_user_message(message)
    await show_screen(bot, message.from_user.id, message.chat.id, f"📸 Фото добавлено: {len(photos)}/5.\n\nОтправьте ещё фото или нажмите «Завершить загрузку».", photos_keyboard())

def photos_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Готово, продолжить", callback_data="photos:done")], [InlineKeyboardButton(text="Пропустить", callback_data="photos:skip")], [InlineKeyboardButton(text="↩️ Назад", callback_data="wizard:back")], [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")]])

@router.callback_query(F.data.in_({"photos:done", "photos:skip"}), OrderFlow.photos)
async def photos_done(call: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(OrderFlow.notes)
    await show_screen(bot, call.from_user.id, call.message.chat.id,
        "✍️ Напишите пожелания к заказу: особенности модели, посадки, декора или другие детали.\n\nЕсли пожеланий нет, отправьте дефис «-».", wizard_keyboard())
    await call.answer()

@router.message(OrderFlow.photos)
async def photos_other(message: Message, bot: Bot):
    await delete_user_message(message)
    await show_screen(bot, message.from_user.id, message.chat.id, "Пожалуйста, отправьте фотографию или воспользуйтесь кнопками на экране.", photos_keyboard())

@router.message(OrderFlow.notes)
async def complete_order(message: Message, state: FSMContext, bot: Bot):
    if not message.text:
        await delete_user_message(message)
        return await show_screen(bot, message.from_user.id, message.chat.id, "Напишите пожелания текстом или отправьте «-».")
    data = await state.get_data()
    if len(data.get("measures", [])) != len(MEASUREMENTS) or not all(k in data for k in ("model_id", "fabric_id")):
        await state.clear()
        await delete_user_message(message)
        return await render_home(bot, message.from_user.id, message.chat.id)
    notes = "" if message.text.strip() == "-" else message.text.strip()[:1500]
    oid = await create_order(message.from_user.id, data["model_id"], data["fabric_id"], data["measures"], data.get("photos", []), notes)
    await state.clear()
    await delete_user_message(message)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📦 Открыть мои заказы", callback_data="client:orders")], [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")]])
    await show_screen(bot, message.from_user.id, message.chat.id, f"✅ <b>Заказ #{oid} оформлен!</b>\n\nДанные переданы портному. Вы сможете следить за изменением статуса в разделе «Мои заказы».", kb)
    await send_order_to_tailor(bot, oid)

@router.callback_query(F.data == "client:orders")
async def client_orders(call: CallbackQuery, bot: Bot):
    rows = await get_orders(call.from_user.id)
    if not rows:
        text = "📦 <b>Мои заказы</b>\n\nЗаказов пока нет."
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👗 Оформить заказ", callback_data="client:new")], [InlineKeyboardButton(text="↩️ В меню", callback_data="home")]])
    else:
        lines = ["📦 <b>Мои заказы</b>"]
        buttons = []
        for r in rows:
            lines.append(f"\n<b>#{r['id']}</b> · {esc(r['model_name'])}\nСтатус: {esc(r['status'])}\nДата: {esc(r['created_at'])}")
            buttons.append([InlineKeyboardButton(text=f"Подробнее · заказ #{r['id']}", callback_data=f"order:view:{r['id']}")])
        buttons.append([InlineKeyboardButton(text="↩️ В меню", callback_data="home")])
        text, kb = "".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)
    await show_screen(bot, call.from_user.id, call.message.chat.id, text, kb)
    await call.answer()

@router.callback_query(F.data.startswith("order:view:"))
async def order_view(call: CallbackQuery, bot: Bot):
    try:
        oid = int(call.data.rsplit(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный номер", show_alert=True)
    order = await get_order(oid)
    if not order or (order["user_id"] != call.from_user.id and not is_admin(call.from_user.id)):
        return await call.answer("Заказ не найден", show_alert=True)
    text = f"📦 <b>Заказ #{oid}</b>\nМодель: {esc(order['model_name'])}\nТкань: {esc(order['fabric_name'])}\nСтатус: <b>{esc(order['status'])}</b>\nСоздан: {esc(order['created_at'])}\nФотографий: {len(order['photos'])}\nПожелания: {esc(order['notes']) or 'нет'}"
    if order.get("total_price"):
        text += (f"\n\nПолная стоимость: <b>{int(order['total_price']):,} ₽</b>".replace(",", " ") +
                 f"\nПредоплата: <b>{int(order['deposit_amount']):,} ₽</b>".replace(",", " ") +
                 f"\nРеквизиты: {esc(order.get('payment_details') or 'не указаны')}" +
                 f"\nСостояние оплаты: <b>{esc(order.get('payment_state'))}</b>")
    nav_rows = []
    if order.get("payment_state") == "awaiting":
        nav_rows.append([InlineKeyboardButton(text="📤 Отправить подтверждение оплаты", callback_data=f"client:payment:{oid}")])
    nav_rows += [[InlineKeyboardButton(text="↩️ К моим заказам", callback_data="client:orders")],
                 [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")]]
    order_nav = InlineKeyboardMarkup(inline_keyboard=nav_rows)
    await show_screen(bot, call.from_user.id, call.message.chat.id, text, order_nav)
    await call.answer()

@router.callback_query(F.data == "client:profile")
async def client_profile(call: CallbackQuery, bot: Bot):
    user = await get_user(call.from_user.id)
    orders = await get_orders(call.from_user.id)
    text = (f"👤 <b>Мой профиль</b>\n\nИмя: {esc(user['full_name'] if user else call.from_user.full_name)}\n"
            f"Username: @{esc(user['username']) if user and user['username'] else 'не указан'}\n"
            f"Телефон: {esc(user['phone']) if user and user['phone'] else 'не указан'}\n"
            f"Telegram ID: <code>{call.from_user.id}</code>\nЗаказов: {len(orders)}")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Изменить имя", callback_data="profile:name")], [InlineKeyboardButton(text="Добавить телефон", callback_data="profile:phone")], [InlineKeyboardButton(text="↩️ В меню", callback_data="home")]])
    await show_screen(bot, call.from_user.id, call.message.chat.id, text, kb)
    await call.answer()

@router.callback_query(F.data == "profile:name")
async def profile_name_start(call: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(ProfileFlow.name)
    await show_screen(bot, call.from_user.id, call.message.chat.id, "Введите имя, которое будет отображаться в профиле.", back_menu())
    await call.answer()

@router.callback_query(F.data == "profile:phone")
async def profile_phone_start(call: CallbackQuery, state: FSMContext, bot: Bot):
    await state.set_state(ProfileFlow.phone)
    await show_screen(bot, call.from_user.id, call.message.chat.id, "Введите номер телефона в международном формате. Например: +7 900 000 00 00", back_menu())
    await call.answer()

@router.message(ProfileFlow.name)
async def profile_name_save(message: Message, state: FSMContext, bot: Bot):
    value = (message.text or "").strip()
    await delete_user_message(message)
    if not 2 <= len(value) <= 80:
        return await show_screen(bot, message.from_user.id, message.chat.id, "Имя должно содержать от 2 до 80 символов. Попробуйте ещё раз.")
    await update_profile(message.from_user.id, "full_name", value)
    await state.clear()
    await show_screen(bot, message.from_user.id, message.chat.id, "✅ Имя профиля обновлено.", back_menu())

@router.message(ProfileFlow.phone)
async def profile_phone_save(message: Message, state: FSMContext, bot: Bot):
    value = (message.text or "").strip()
    await delete_user_message(message)
    digits = "".join(ch for ch in value if ch.isdigit())
    if not 7 <= len(digits) <= 15:
        return await show_screen(bot, message.from_user.id, message.chat.id, "Проверьте номер: укажите от 7 до 15 цифр, можно с + и пробелами.")
    await update_profile(message.from_user.id, "phone", value[:30])
    await state.clear()
    await show_screen(bot, message.from_user.id, message.chat.id, "✅ Номер телефона сохранён.", back_menu())

@router.callback_query(F.data == "client:measures")
async def client_measures(call: CallbackQuery, bot: Bot):
    async with connect_db() as db:
        row = await (await db.execute("""SELECT x.* FROM measurements x JOIN orders o ON o.id=x.order_id
        WHERE o.user_id=? ORDER BY o.id DESC LIMIT 1""", (call.from_user.id,))).fetchone()
    if not row:
        text = "📏 <b>Мои мерки</b>\n\nСохранённых мерок пока нет. Они появятся после оформления первого заказа."
    else:
        text = "📏 <b>Мои мерки из последнего заказа</b>\n\n" + "\n".join(f"{esc(label)}: {row[key]} см" for key, label, _ in MEASUREMENTS)
        text += "\n\nПеред новым заказом проверьте актуальность значений."
    await show_screen(bot, call.from_user.id, call.message.chat.id, text, back_menu())
    await call.answer()

@router.callback_query(F.data.startswith("catalog:"))
async def show_catalog(call: CallbackQuery, bot: Bot):
    table = call.data.split(":", 1)[1]
    if table not in ("models", "fabrics"):
        return await call.answer("Раздел не найден", show_alert=True)
    items = await catalog(table)
    if not items:
        return await call.answer("Каталог пока пуст", show_alert=True)
    lines = ["👗 <b>Каталог моделей</b>" if table == "models" else "🎨 <b>Каталог тканей</b>"]
    for item in items:
        lines.append(f"\n<b>{esc(item['name'])}</b>\n{esc(item['description']) or 'Описание скоро появится.'}")
    await show_screen(bot, call.from_user.id, call.message.chat.id, "\n".join(lines), back_menu())
    await call.answer()

@router.callback_query(F.data == "tailor:orders")
async def tailor_orders(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return await call.answer("Доступ запрещён", show_alert=True)
    rows = await get_orders()
    if not rows:
        await show_screen(bot, call.from_user.id, call.message.chat.id, "📥 <b>Заказы</b>\n\nНовых заказов пока нет.", tailor_menu())
    else:
        lines = ["📥 <b>Последние заказы</b>"]
        buttons = []
        for r in rows:
            lines.append(f"\n<b>#{r['id']}</b> · {esc(r['model_name'])}\nКлиент: {esc(r['full_name'])}\nСтатус: {esc(r['status'])}")
            buttons.append([InlineKeyboardButton(text=f"{r['status']} · заказ #{r['id']}", callback_data=f"tailor:view:{r['id']}")])
        buttons.append([InlineKeyboardButton(text="↩️ В панель", callback_data="home")])
        await show_screen(bot, call.from_user.id, call.message.chat.id, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons))
    await call.answer()

@router.callback_query(F.data.startswith("tailor:view:"))
async def tailor_view(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return await call.answer("Доступ запрещён", show_alert=True)
    try:
        oid = int(call.data.rsplit(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный номер", show_alert=True)
    order = await get_order(oid)
    if not order:
        return await call.answer("Заказ не найден", show_alert=True)
    await show_screen(bot, call.from_user.id, call.message.chat.id,
                      order_details_text(order), status_menu(oid, order["status"], order.get("payment_state", "not_set")))
    if order["photos"]:
        try:
            if len(order["photos"]) == 1:
                await bot.send_photo(call.message.chat.id, order["photos"][0], caption=f"📸 Фото к заказу #{oid}")
            else:
                await bot.send_media_group(call.message.chat.id, [InputMediaPhoto(media=fid) for fid in order["photos"][:10]])
        except Exception:
            log.exception("Не удалось показать фотографии заказа #%s", oid)
    await call.answer()

@router.callback_query(F.data.startswith("tailor:add:"))
async def add_item_begin(call: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(call.from_user.id):
        return await call.answer("Доступ запрещён", show_alert=True)
    kind = call.data.rsplit(":", 1)[1]
    if kind not in ("model", "fabric"):
        return await call.answer("Раздел не найден", show_alert=True)
    await state.clear()
    await state.update_data(table="models" if kind == "model" else "fabrics")
    await state.set_state(AddItemFlow.name)
    await show_screen(bot, call.from_user.id, call.message.chat.id, "Введите название новой позиции.", back_menu())
    await call.answer()

@router.message(AddItemFlow.name)
async def add_item_name(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    name = (message.text or "").strip()
    await delete_user_message(message)
    if not 1 <= len(name) <= 100:
        return await show_screen(bot, message.from_user.id, message.chat.id, "Название должно содержать от 1 до 100 символов.")
    await state.update_data(item_name=name)
    await state.set_state(AddItemFlow.description)
    await show_screen(bot, message.from_user.id, message.chat.id, "Введите описание или отправьте дефис «-», если его нет.")

@router.message(AddItemFlow.description)
async def add_item_description(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    desc = (message.text or "").strip()
    await delete_user_message(message)
    if len(desc) > 500:
        return await show_screen(bot, message.from_user.id, message.chat.id, "Описание не должно превышать 500 символов.")
    await state.update_data(item_description="" if desc == "-" else desc)
    await state.set_state(AddItemFlow.photo)
    await show_screen(bot, message.from_user.id, message.chat.id, "Отправьте фотографию позиции или дефис «-» без фотографии.")

@router.message(AddItemFlow.photo)
async def add_item_finish(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    photo_id = message.photo[-1].file_id if message.photo else ""
    await delete_user_message(message)
    if not photo_id and (message.text or "").strip() != "-":
        return await show_screen(bot, message.from_user.id, message.chat.id, "Отправьте фото или дефис «-».")
    data = await state.get_data()
    await add_catalog_item(data["table"], data["item_name"], data["item_description"], photo_id)
    await state.clear()
    await show_screen(bot, message.from_user.id, message.chat.id, "✅ Позиция добавлена в каталог.", tailor_menu())

@router.callback_query(F.data.startswith("tailor:price:"))
async def tailor_price_begin(call: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(call.from_user.id):
        return await call.answer("Доступ запрещён", show_alert=True)
    try:
        oid = int(call.data.rsplit(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный номер заказа", show_alert=True)
    if not await get_order(oid):
        return await call.answer("Заказ не найден", show_alert=True)
    await state.clear()
    await state.update_data(quote_order_id=oid)
    await state.set_state(TailorPriceFlow.total)
    await show_screen(bot, call.from_user.id, call.message.chat.id,
                      f"💰 Расчёт заказа #{oid}\n\nВведите полную стоимость корсета в рублях (только число).")
    await call.answer()

@router.message(TailorPriceFlow.total)
async def tailor_price_total(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    raw = (message.text or "").replace(" ", "").replace(",", "")
    if not raw.isdigit() or not 1 <= int(raw) <= 100000000:
        await delete_user_message(message)
        return await show_screen(bot, message.from_user.id, message.chat.id, "Введите стоимость целым числом в рублях, например 15000.")
    await state.update_data(total_price=int(raw))
    await state.set_state(TailorPriceFlow.deposit)
    await delete_user_message(message)
    await show_screen(bot, message.from_user.id, message.chat.id, "Введите сумму предоплаты в рублях. Она должна быть меньше полной стоимости.")

@router.message(TailorPriceFlow.deposit)
async def tailor_price_deposit(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    raw = (message.text or "").replace(" ", "").replace(",", "")
    data = await state.get_data()
    total = int(data.get("total_price", 0))
    if not raw.isdigit() or not 1 <= int(raw) < total:
        await delete_user_message(message)
        return await show_screen(bot, message.from_user.id, message.chat.id, f"Предоплата должна быть числом больше нуля и меньше {total} ₽. Введите сумму ещё раз.")
    await state.update_data(deposit_amount=int(raw))
    await state.set_state(TailorPriceFlow.details)
    await delete_user_message(message)
    await show_screen(bot, message.from_user.id, message.chat.id, "Отправьте реквизиты для оплаты: банк, номер карты/счёта или платёжную ссылку. Эти данные увидит клиент.")

@router.message(TailorPriceFlow.details)
async def tailor_price_details(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear(); return
    details = (message.text or "").strip()
    if not details or len(details) > 1000:
        await delete_user_message(message)
        return await show_screen(bot, message.from_user.id, message.chat.id, "Отправьте текстовые реквизиты длиной до 1000 символов.")
    data = await state.get_data()
    oid = int(data["quote_order_id"])
    await save_order_quote(oid, int(data["total_price"]), int(data["deposit_amount"]), details)
    await state.clear()
    await delete_user_message(message)
    order = await get_order(oid)
    client_text = (f"💰 <b>Расчёт стоимости заказа #{oid}</b>\n\n"
                   f"Модель: {esc(order['model_name'])}\n"
                   f"Полная стоимость: <b>{int(order['total_price']):,} ₽</b>\n".replace(",", " ") +
                   f"Предоплата: <b>{int(order['deposit_amount']):,} ₽</b>\n".replace(",", " ") +
                   f"Остаток после предоплаты: <b>{int(order['total_price'])-int(order['deposit_amount']):,} ₽</b>\n\n".replace(",", " ") +
                   f"💳 <b>Реквизиты для оплаты</b>\n{esc(details)}\n\n"
                   "После перевода откройте заказ и отправьте подтверждение оплаты. Заказ начнётся после проверки поступления предоплаты портным.")
    client_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить подтверждение оплаты", callback_data=f"client:payment:{oid}")],
        [InlineKeyboardButton(text="📦 Мои заказы", callback_data="client:orders")],
    ])
    await show_screen(bot, order["user_id"], order["user_id"], client_text, client_kb)
    await show_screen(bot, ADMIN_ID, ADMIN_ID,
                      order_details_text(order), status_menu(oid, order["status"], order.get("payment_state", "awaiting")))

@router.callback_query(F.data.startswith("client:payment:"))
async def client_payment_begin(call: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        oid = int(call.data.rsplit(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный номер заказа", show_alert=True)
    order = await get_order(oid)
    if not order or order["user_id"] != call.from_user.id or order.get("payment_state") not in ("awaiting", "proof_sent"):
        return await call.answer("Для этого заказа сейчас нельзя отправить подтверждение", show_alert=True)
    await state.clear()
    await state.update_data(payment_order_id=oid)
    await state.set_state(ClientPaymentFlow.proof)
    await show_screen(bot, call.from_user.id, call.message.chat.id,
                      "Отправьте фото или документ с подтверждением перевода. Можно также написать текстовый комментарий.")
    await call.answer()

@router.message(ClientPaymentFlow.proof)
async def client_payment_proof(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    oid = int(data.get("payment_order_id", 0))
    file_id = message.photo[-1].file_id if message.photo else (message.document.file_id if message.document else "")
    proof_text = (message.caption or message.text or "").strip()[:1000]
    if not file_id and not proof_text:
        await delete_user_message(message)
        return await show_screen(bot, message.from_user.id, message.chat.id, "Пришлите фото, документ или текстовое подтверждение оплаты.")
    await save_payment_proof(oid, file_id, proof_text)
    await state.clear()
    await delete_user_message(message)
    order = await get_order(oid)
    await show_screen(bot, message.from_user.id, message.chat.id,
                      f"✅ Подтверждение оплаты по заказу #{oid} отправлено портному.\nОжидайте проверки поступления предоплаты.",
                      client_status_menu(oid))
    proof_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Предоплата получена", callback_data=f"tailor:deposit:{oid}")],
                                                      [InlineKeyboardButton(text="📂 Открыть заказ", callback_data=f"tailor:view:{oid}")]])
    try:
        await bot.send_message(ADMIN_ID, f"📩 <b>Подтверждение предоплаты · заказ #{oid}</b>\nКлиент: {esc(order['full_name'])}\nКомментарий: {esc(proof_text) or 'нет'}", reply_markup=proof_kb)
        if file_id:
            if message.document:
                await bot.send_document(ADMIN_ID, file_id)
            else:
                await bot.send_photo(ADMIN_ID, file_id)
    except Exception:
        log.exception("Не удалось передать подтверждение оплаты по заказу #%s", oid)

@router.callback_query(F.data.startswith("tailor:deposit:"))
async def tailor_deposit_confirm(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return await call.answer("Доступ запрещён", show_alert=True)
    try:
        oid = int(call.data.rsplit(":", 1)[1])
    except ValueError:
        return await call.answer("Некорректный номер заказа", show_alert=True)
    if not await confirm_deposit(oid):
        return await call.answer("Сначала клиент должен отправить подтверждение оплаты", show_alert=True)
    order = await get_order(oid)
    try:
        await bot.send_message(order["user_id"], f"✅ Предоплата по заказу #{oid} подтверждена. Заказ принят в работу.")
    except Exception:
        log.exception("Не удалось уведомить клиента о предоплате")
    await show_screen(bot, call.from_user.id, call.message.chat.id,
                      order_details_text(order), status_menu(oid, order["status"], "paid"))
    await call.answer("Предоплата подтверждена")

@router.callback_query(F.data.startswith("status:"))
async def change_status(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return await call.answer("Доступ запрещён", show_alert=True)
    try:
        _, raw_id, status = call.data.split(":", 2)
        oid = int(raw_id)
    except (ValueError, TypeError):
        return await call.answer("Некорректные данные", show_alert=True)
    allowed = {"Новый", "Ожидает предоплату", "В работе", "Лекала готовы", "Пошив", "Завершён"}
    if status not in allowed:
        return await call.answer("Недопустимый статус", show_alert=True)
    order = await get_order(oid)
    if not order:
        return await call.answer("Заказ не найден", show_alert=True)
    if status in {"В работе", "Лекала готовы", "Пошив", "Завершён"} and order.get("payment_state") != "paid":
        return await call.answer("Сначала подтвердите получение предоплаты", show_alert=True)
    changed = await update_status(oid, status)
    if changed is None:
        return await call.answer("Заказ не найден", show_alert=True)
    if changed is False:
        # Повторное нажатие не создаёт повторное уведомление.
        await show_screen(bot, call.from_user.id, call.message.chat.id,
                          order_details_text(order), status_menu(oid, order["status"], order.get("payment_state", "not_set")))
        return await call.answer("Этот статус уже установлен")

    order = await get_order(oid)
    if status == "Завершён":
        tailor_contact = f"@{TAILOR_USERNAME}" if TAILOR_USERNAME else "портному в Telegram"
        client_text = (
            f"✅ <b>Ваш заказ «{esc(order['model_name'])}» готов!</b>\n\n"
            f"Напишите {esc(tailor_contact)}, чтобы согласовать получение и дальнейшие детали."
        )
        client_kb = InlineKeyboardMarkup(inline_keyboard=[
            *([[InlineKeyboardButton(text="💬 Написать портному", url=TAILOR_URL)]] if TAILOR_URL else []),
            *([[InlineKeyboardButton(text="⭐ Оставить отзыв", url=FEEDBACK_URL)]] if FEEDBACK_URL else []),
            [InlineKeyboardButton(text="📦 Мои заказы", callback_data="client:orders")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="home")],
        ])
    else:
        client_text = (
            f"📦 <b>Обновление по заказу #{oid}</b>\n\n"
            f"Заказ: {esc(order['model_name'])}\n"
            f"Ткань: {esc(order['fabric_name'])}\n"
            f"Новый статус: <b>{esc(status)}</b>"
        )
        client_kb = client_status_menu(oid, completed=False)
    # Экран клиента обновляется даже если он находился в главном меню или другом разделе.
    try:
        await show_screen(bot, order["user_id"], order["user_id"], client_text, client_kb)
    except Exception:
        log.exception("Не удалось обновить экран клиента по заказу #%s", oid)

    # У портного в том же главном сообщении остаётся карточка с актуальным статусом.
    await show_screen(bot, call.from_user.id, call.message.chat.id,
                      order_details_text(order), status_menu(oid, order["status"], order.get("payment_state", "not_set")))
    await call.answer("Статус обновлён")

@router.callback_query(F.data == "client:delete")
async def delete_prompt(call: CallbackQuery, bot: Bot):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Да, удалить данные", callback_data="delete:yes")], [InlineKeyboardButton(text="Отмена", callback_data="home")]])
    await show_screen(bot, call.from_user.id, call.message.chat.id, "Удалить профиль, заказы, мерки и сохранённые ссылки на фотографии? Действие необратимо. Сообщения и фотографии, уже отправленные портному, могут потребовать отдельного удаления.", kb)
    await call.answer()

@router.callback_query(F.data == "delete:yes")
async def delete_yes(call: CallbackQuery, bot: Bot, state: FSMContext):
    await delete_my_data(call.from_user.id)
    await state.clear()
    await save_user(call.from_user)
    await show_screen(bot, call.from_user.id, call.message.chat.id, "Данные профиля и заказы удалены из базы бота. Профиль создан заново.", back_menu())
    await call.answer()

@router.message()
async def fallback(message: Message, bot: Bot):
    await delete_user_message(message)
    await render_home(bot, message.from_user.id, message.chat.id)

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN. Укажите его в .env или переменных окружения хостинга.")
    await init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    log.info("CORSET запущен")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
