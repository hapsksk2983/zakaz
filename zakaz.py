import asyncio
from contextlib import asynccontextmanager
import html
import logging
import os
from pathlib import Path

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

# CORSET AI — единый файл zakaz.py
# Установка: pip install aiogram aiosqlite python-dotenv
# .env: BOT_TOKEN=... / ADMIN_ID=123456789 / DB_PATH=data/corset_ai.db
# Запуск: python zakaz.py

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)
DB_PATH = os.getenv("DB_PATH", "data/corset_ai.db")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("corset_ai")
router = Router()


# -------------------- СОСТОЯНИЯ --------------------

class OrderFlow(StatesGroup):
    consent = State()
    model = State()
    fabric = State()
    bust = State()
    underbust = State()
    waist = State()
    hips = State()
    front = State()
    back = State()
    photos = State()
    notes = State()


class AddItemFlow(StatesGroup):
    name = State()
    description = State()
    photo = State()


# -------------------- БАЗА ДАННЫХ --------------------

@asynccontextmanager
async def connect_db():
    """Открывает соединение SQLite и гарантированно закрывает его."""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA foreign_keys=ON")
        yield db
    finally:
        await db.close()


async def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    async with connect_db() as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'client', created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            FOREIGN KEY(model_id) REFERENCES models(id),
            FOREIGN KEY(fabric_id) REFERENCES fabrics(id)
        );
        CREATE TABLE IF NOT EXISTS measurements(
            order_id INTEGER PRIMARY KEY, bust REAL NOT NULL, underbust REAL NOT NULL,
            waist REAL NOT NULL, hips REAL NOT NULL, front REAL NOT NULL, back REAL NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS order_photos(
            id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL, file_id TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
        );
        """)
        for table, defaults in {
            "models": [
                ("Классический overbust", "Корсет с закрытием груди."),
                ("Классический underbust", "Корсет под грудью."),
                ("Удлинённый корсет", "Модель с удлинённой линией низа.")
            ],
            "fabrics": [
                ("Атлас", "Гладкая ткань с лёгким блеском."),
                ("Хлопок", "Матовая ткань с натуральной фактурой."),
                ("Бархат", "Мягкая ткань с выразительной фактурой.")
            ]
        }.items():
            cur = await db.execute(f"SELECT COUNT(*) AS n FROM {table}")
            if (await cur.fetchone())["n"] == 0:
                await db.executemany(
                    f"INSERT INTO {table}(name,description) VALUES(?,?)", defaults
                )
        await db.commit()


async def save_user(user):
    role = "tailor" if ADMIN_ID and user.id == ADMIN_ID else "client"
    async with connect_db() as db:
        await db.execute("""
        INSERT INTO users(user_id,username,full_name,role) VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
        username=excluded.username, full_name=excluded.full_name, role=excluded.role
        """, (user.id, user.username, user.full_name, role))
        await db.commit()


async def catalog(table):
    if table not in ("models", "fabrics"):
        raise ValueError("Unknown catalog")
    async with connect_db() as db:
        cur = await db.execute(
            f"SELECT id,name,description,photo_id FROM {table} WHERE active=1 ORDER BY id"
        )
        return [dict(x) for x in await cur.fetchall()]


async def add_catalog_item(table, name, description, photo_id):
    if table not in ("models", "fabrics"):
        raise ValueError("Unknown catalog")
    async with connect_db() as db:
        await db.execute(
            f"INSERT INTO {table}(name,description,photo_id) VALUES(?,?,?)",
            (name, description, photo_id)
        )
        await db.commit()


async def create_order(user_id, model_id, fabric_id, measures, photos, notes):
    async with connect_db() as db:
        cur = await db.execute(
            "INSERT INTO orders(user_id,model_id,fabric_id,notes) VALUES(?,?,?,?)",
            (user_id, model_id, fabric_id, notes)
        )
        oid = cur.lastrowid
        await db.execute(
            "INSERT INTO measurements VALUES(?,?,?,?,?,?,?)",
            (oid, *measures)
        )
        await db.executemany(
            "INSERT INTO order_photos(order_id,file_id) VALUES(?,?)",
            [(oid, p) for p in photos]
        )
        await db.commit()
        return oid


async def get_order(oid):
    async with connect_db() as db:
        cur = await db.execute("""
        SELECT o.*,u.user_id,u.full_name,u.username,m.name model_name,f.name fabric_name,
        x.bust,x.underbust,x.waist,x.hips,x.front,x.back
        FROM orders o JOIN users u ON u.user_id=o.user_id
        JOIN models m ON m.id=o.model_id JOIN fabrics f ON f.id=o.fabric_id
        JOIN measurements x ON x.order_id=o.id WHERE o.id=?
        """, (oid,))
        row = await cur.fetchone()
        if not row:
            return None
        data = dict(row)
        cur = await db.execute("SELECT file_id FROM order_photos WHERE order_id=?", (oid,))
        data["photos"] = [r["file_id"] for r in await cur.fetchall()]
        return data


async def get_orders(user_id=None, limit=20):
    async with connect_db() as db:
        if user_id is None:
            cur = await db.execute("""
            SELECT o.id,o.status,o.created_at,u.full_name,m.name model_name
            FROM orders o JOIN users u ON u.user_id=o.user_id
            JOIN models m ON m.id=o.model_id ORDER BY o.id DESC LIMIT ?
            """, (limit,))
        else:
            cur = await db.execute("""
            SELECT o.id,o.status,o.created_at,m.name model_name
            FROM orders o JOIN models m ON m.id=o.model_id
            WHERE o.user_id=? ORDER BY o.id DESC LIMIT ?
            """, (user_id, limit))
        return [dict(r) for r in await cur.fetchall()]


async def update_status(oid, status):
    async with connect_db() as db:
        await db.execute("UPDATE orders SET status=? WHERE id=?", (status, oid))
        await db.commit()


async def delete_my_data(user_id):
    async with connect_db() as db:
        cur = await db.execute("SELECT id FROM orders WHERE user_id=?", (user_id,))
        ids = [r["id"] for r in await cur.fetchall()]
        for oid in ids:
            await db.execute("DELETE FROM order_photos WHERE order_id=?", (oid,))
            await db.execute("DELETE FROM measurements WHERE order_id=?", (oid,))
            await db.execute("DELETE FROM orders WHERE id=?", (oid,))
        await db.execute("DELETE FROM users WHERE user_id=?", (user_id,))
        await db.commit()


# -------------------- КЛАВИАТУРЫ И ВСПОМОГАТЕЛЬНОЕ --------------------

def admin(uid):
    return bool(ADMIN_ID and uid == ADMIN_ID)


def main_menu(is_admin=False):
    rows = [
        [KeyboardButton(text="👗 Каталог корсетов"), KeyboardButton(text="🧵 Каталог тканей")],
        [KeyboardButton(text="➕ Создать заказ"), KeyboardButton(text="📦 Мои заказы")],
        [KeyboardButton(text="ℹ️ О проекте"), KeyboardButton(text="🗑 Удалить мои данные")]
    ]
    if is_admin:
        rows.append([KeyboardButton(text="🛠 Панель портного")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def choose_keyboard(items, prefix):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=x["name"][:60], callback_data=f"{prefix}:{x['id']}")]
        for x in items
    ])


def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить модель", callback_data="add:model")],
        [InlineKeyboardButton(text="➕ Добавить ткань", callback_data="add:fabric")],
        [InlineKeyboardButton(text="📋 Последние заказы", callback_data="admin:orders")]
    ])


def status_menu(oid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧵 Принять в работу", callback_data=f"status:{oid}:В работе")],
        [InlineKeyboardButton(text="📐 Лекала подготовлены", callback_data=f"status:{oid}:Лекала готовы")],
        [InlineKeyboardButton(text="✅ Заказ завершён", callback_data=f"status:{oid}:Завершён")]
    ])


def esc(value):
    return html.escape(str(value or ""))


def parse_measure(text):
    try:
        n = float(text.replace(",", ".").strip())
        return n if 1 <= n <= 250 else None
    except (ValueError, AttributeError):
        return None


async def show_catalog(message, table):
    items = await catalog(table)
    if not items:
        return await message.answer("Каталог пока пуст.")
    for item in items:
        caption = f"<b>{esc(item['name'])}</b>"
        if item["description"]:
            caption += "\n" + esc(item["description"])
        if item["photo_id"]:
            await message.answer_photo(item["photo_id"], caption=caption)
        else:
            await message.answer(caption)
    await message.answer("Для оформления выберите «➕ Создать заказ».")


async def send_order_to_tailor(bot, oid):
    if not ADMIN_ID:
        log.warning("ADMIN_ID не задан — уведомление портному не отправлено.")
        return
    order = await get_order(oid)
    if not order:
        return
    username = "@" + esc(order["username"]) if order["username"] else "не указан"
    text = (
        f"🆕 <b>Новый заказ #{oid}</b>\n\n"
        f"Клиент: {esc(order['full_name'])}\nUsername: {username}\n"
        f"Модель: {esc(order['model_name'])}\nТкань: {esc(order['fabric_name'])}\n\n"
        f"<b>Мерки, см:</b>\nГрудь: {order['bust']}\nПод грудью: {order['underbust']}\n"
        f"Талия: {order['waist']}\nБёдра: {order['hips']}\n"
        f"Длина переда: {order['front']}\nДлина спинки: {order['back']}\n"
        f"\nПожелания: {esc(order['notes']) or 'нет'}"
    )
    try:
        await bot.send_message(ADMIN_ID, text, reply_markup=status_menu(oid))
        for i, photo in enumerate(order["photos"], 1):
            await bot.send_photo(ADMIN_ID, photo, caption=f"Фото {i}, заказ #{oid}")
    except Exception:
        log.exception("Ошибка отправки заказа портному")


# -------------------- КОМАНДЫ И КЛИЕНТСКАЯ ЧАСТЬ --------------------

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await save_user(message.from_user)
    await message.answer(
        "🧵 <b>CORSET AI</b>\n\n"
        "Платформа индивидуального пошива корсетов.\n"
        "Выберите раздел в меню. Клиент может выбрать модель и ткань, "
        "указать мерки и передать фотографии портному.",
        reply_markup=main_menu(admin(message.from_user.id))
    )


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Действие отменено.", reply_markup=main_menu(admin(message.from_user.id)))


@router.message(F.text == "👗 Каталог корсетов")
async def client_models(message: Message):
    await show_catalog(message, "models")


@router.message(F.text == "🧵 Каталог тканей")
async def client_fabrics(message: Message):
    await show_catalog(message, "fabrics")


@router.message(F.text == "➕ Создать заказ")
async def begin_order(message: Message, state: FSMContext):
    await save_user(message.from_user)
    await state.clear()
    await state.set_state(OrderFlow.consent)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Согласен(на), продолжить", callback_data="consent:yes")],
        [InlineKeyboardButton(text="Отмена", callback_data="consent:no")]
    ])
    await message.answer(
        "Перед оформлением заказа подтвердите согласие на передачу портному "
        "ваших мерок и фотографий для подготовки индивидуальной конструкции.\n\n"
        "Отправляйте только те фотографии, которыми готовы поделиться. "
        "Перед запуском сервиса владелец должен предоставить клиентам полную "
        "политику конфиденциальности.",
        reply_markup=kb
    )


@router.callback_query(F.data == "consent:no", OrderFlow.consent)
async def consent_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Оформление отменено.")
    await call.answer()


@router.callback_query(F.data == "consent:yes", OrderFlow.consent)
async def consent_accept(call: CallbackQuery, state: FSMContext):
    items = await catalog("models")
    if not items:
        await state.clear()
        await call.message.edit_text("Каталог моделей пока пуст.")
        return await call.answer()
    await state.set_state(OrderFlow.model)
    await call.message.edit_text("Шаг 1/2. Выберите модель:", reply_markup=choose_keyboard(items, "model"))
    await call.answer()


@router.callback_query(F.data.startswith("model:"), OrderFlow.model)
async def order_choose_model(call: CallbackQuery, state: FSMContext):
    await state.update_data(model_id=int(call.data.split(":")[1]))
    items = await catalog("fabrics")
    if not items:
        await state.clear()
        await call.message.edit_text("Каталог тканей пока пуст.")
        return await call.answer()
    await state.set_state(OrderFlow.fabric)
    await call.message.edit_text("Шаг 2/2. Выберите ткань:", reply_markup=choose_keyboard(items, "fabric"))
    await call.answer()


@router.callback_query(F.data.startswith("fabric:"), OrderFlow.fabric)
async def order_choose_fabric(call: CallbackQuery, state: FSMContext):
    await state.update_data(fabric_id=int(call.data.split(":")[1]), measures=[], photos=[])
    await state.set_state(OrderFlow.bust)
    await call.message.edit_text("Введите мерки в сантиметрах.\n\n<b>1/6 — Обхват груди.</b>\nНапример: 92")
    await call.answer()


async def ask_measure(message, state, next_state, prompt):
    value = parse_measure(message.text)
    if value is None:
        return await message.answer("Введите число от 1 до 250 см, например 92. Для отмены: /cancel")
    data = await state.get_data()
    measures = data.get("measures", [])
    measures.append(value)
    await state.update_data(measures=measures)
    await state.set_state(next_state)
    await message.answer(prompt)


@router.message(OrderFlow.bust)
async def m_bust(message: Message, state: FSMContext):
    await ask_measure(message, state, OrderFlow.underbust, "<b>2/6 — Обхват под грудью, см:</b>")


@router.message(OrderFlow.underbust)
async def m_underbust(message: Message, state: FSMContext):
    await ask_measure(message, state, OrderFlow.waist, "<b>3/6 — Обхват талии, см:</b>")


@router.message(OrderFlow.waist)
async def m_waist(message: Message, state: FSMContext):
    await ask_measure(message, state, OrderFlow.hips, "<b>4/6 — Обхват бёдер, см:</b>")


@router.message(OrderFlow.hips)
async def m_hips(message: Message, state: FSMContext):
    await ask_measure(message, state, OrderFlow.front, "<b>5/6 — Длина переда до талии, см:</b>")


@router.message(OrderFlow.front)
async def m_front(message: Message, state: FSMContext):
    await ask_measure(message, state, OrderFlow.back, "<b>6/6 — Длина спинки до талии, см:</b>")


@router.message(OrderFlow.back)
async def m_back(message: Message, state: FSMContext):
    value = parse_measure(message.text)
    if value is None:
        return await message.answer("Введите число от 1 до 250 см, например 40.")
    data = await state.get_data()
    measures = data.get("measures", []) + [value]
    if len(measures) != 6:
        await state.clear()
        return await message.answer("Ошибка анкеты. Начните оформление заново.")
    await state.update_data(measures=measures)
    await state.set_state(OrderFlow.photos)
    await message.answer(
        "📸 <b>Загрузите фотографии для портного.</b>\n"
        "Желательно отправить виды спереди, сбоку и сзади. "
        "Можно загрузить до 5 фото. После загрузки отправьте /done."
    )


@router.message(OrderFlow.photos, F.photo)
async def collect_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photos", [])
    if len(photos) >= 5:
        return await message.answer("Максимум 5 фотографий. Отправьте /done.")
    photos.append(message.photo[-1].file_id)
    await state.update_data(photos=photos)
    await message.answer(f"Фото добавлено: {len(photos)}/5. Отправьте ещё или /done.")


@router.message(OrderFlow.photos, Command("done"))
async def photos_done(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("photos"):
        return await message.answer("Добавьте хотя бы одну фотографию или отмените заказ: /cancel")
    await state.set_state(OrderFlow.notes)
    await message.answer("Напишите пожелания к заказу или отправьте «-», если комментариев нет.")


@router.message(OrderFlow.photos)
async def photos_other(message: Message):
    await message.answer("Отправьте фото или /done, когда закончите.")


@router.message(OrderFlow.notes)
async def complete_order(message: Message, state: FSMContext, bot: Bot):
    if not message.text:
        return await message.answer("Напишите пожелания текстом или отправьте «-».")
    data = await state.get_data()
    if not all(k in data for k in ("model_id", "fabric_id", "measures", "photos")):
        await state.clear()
        return await message.answer("Данные заказа потеряны. Начните оформление заново.")
    notes = "" if message.text.strip() == "-" else message.text.strip()[:1500]
    oid = await create_order(
        message.from_user.id, data["model_id"], data["fabric_id"],
        data["measures"], data["photos"], notes
    )
    await state.clear()
    await message.answer(
        f"✅ <b>Заказ #{oid} создан!</b>\nПортной получил данные для проверки.",
        reply_markup=main_menu(admin(message.from_user.id))
    )
    await send_order_to_tailor(bot, oid)


@router.message(F.text == "📦 Мои заказы")
async def client_orders(message: Message):
    rows = await get_orders(None if admin(message.from_user.id) else message.from_user.id)
    if not rows:
        return await message.answer("Заказов пока нет.")
    lines = ["📦 <b>Заказы</b>"]
    for row in rows:
        line = f"#{row['id']} — {esc(row['status'])} — {esc(row['model_name'])}"
        if admin(message.from_user.id):
            line += f" — {esc(row.get('full_name', ''))}"
        lines.append(line)
    await message.answer("\n".join(lines))


# -------------------- ПАНЕЛЬ ПОРТНОГО --------------------

@router.message(F.text == "🛠 Панель портного")
async def tailor_panel(message: Message):
    if not admin(message.from_user.id):
        return
    await message.answer("🛠 <b>Панель портного</b>", reply_markup=admin_menu())


@router.callback_query(F.data.startswith("add:"))
async def add_item_begin(call: CallbackQuery, state: FSMContext):
    if not admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    kind = call.data.split(":")[1]
    if kind not in ("model", "fabric"):
        return await call.answer("Неизвестный раздел", show_alert=True)
    await state.clear()
    await state.update_data(table="models" if kind == "model" else "fabrics")
    await state.set_state(AddItemFlow.name)
    await call.message.answer("Введите название позиции:")
    await call.answer()


@router.message(AddItemFlow.name)
async def add_item_name(message: Message, state: FSMContext):
    if not admin(message.from_user.id):
        await state.clear()
        return
    name = (message.text or "").strip()
    if not name or len(name) > 100:
        return await message.answer("Название должно быть от 1 до 100 символов.")
    await state.update_data(item_name=name)
    await state.set_state(AddItemFlow.description)
    await message.answer("Введите описание или «-», если его нет.")


@router.message(AddItemFlow.description)
async def add_item_description(message: Message, state: FSMContext):
    if not admin(message.from_user.id):
        await state.clear()
        return
    desc = (message.text or "").strip()
    if len(desc) > 500:
        return await message.answer("Описание не должно превышать 500 символов.")
    await state.update_data(item_description="" if desc == "-" else desc)
    await state.set_state(AddItemFlow.photo)
    await message.answer("Отправьте фото позиции или «-» без фотографии.")


@router.message(AddItemFlow.photo)
async def add_item_finish(message: Message, state: FSMContext):
    if not admin(message.from_user.id):
        await state.clear()
        return
    photo_id = message.photo[-1].file_id if message.photo else ""
    if not photo_id and (message.text or "").strip() != "-":
        return await message.answer("Отправьте фото или напишите «-».")
    data = await state.get_data()
    await add_catalog_item(data["table"], data["item_name"], data["item_description"], photo_id)
    await state.clear()
    await message.answer("✅ Позиция добавлена в каталог.", reply_markup=main_menu(True))


@router.callback_query(F.data == "admin:orders")
async def tailor_orders(call: CallbackQuery):
    if not admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    rows = await get_orders()
    if not rows:
        await call.message.answer("Заказов пока нет.")
    for row in rows:
        await call.message.answer(
            f"📦 <b>Заказ #{row['id']}</b>\nКлиент: {esc(row.get('full_name', ''))}\n"
            f"Модель: {esc(row['model_name'])}\nСтатус: {esc(row['status'])}",
            reply_markup=status_menu(row["id"])
        )
    await call.answer()


@router.callback_query(F.data.startswith("status:"))
async def tailor_change_status(call: CallbackQuery, bot: Bot):
    if not admin(call.from_user.id):
        return await call.answer("Нет доступа", show_alert=True)
    try:
        _, raw_id, status = call.data.split(":", 2)
        oid = int(raw_id)
    except ValueError:
        return await call.answer("Некорректные данные", show_alert=True)
    if status not in {"В работе", "Лекала готовы", "Завершён"}:
        return await call.answer("Недопустимый статус", show_alert=True)
    order = await get_order(oid)
    if not order:
        return await call.answer("Заказ не найден", show_alert=True)
    await update_status(oid, status)
    try:
        await bot.send_message(
            order["user_id"],
            f"📦 Статус вашего заказа <b>#{oid}</b> изменён: <b>{esc(status)}</b>."
        )
    except Exception:
        log.exception("Не удалось уведомить клиента")
    await call.message.answer(f"Статус заказа #{oid} обновлён: {esc(status)}")
    await call.answer("Готово")


@router.message(F.text == "ℹ️ О проекте")
async def about(message: Message):
    await message.answer(
        "🧵 <b>CORSET AI</b>\n\n"
        "Сервис оформления индивидуальных заказов на корсеты. "
        "Клиент выбирает модель и ткань, передаёт мерки и фотографии. "
        "Портной проверяет данные и готовит конструкцию изделия."
    )


@router.message(F.text == "🗑 Удалить мои данные")
async def delete_prompt(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, удалить", callback_data="delete:yes")],
        [InlineKeyboardButton(text="Отмена", callback_data="delete:no")]
    ])
    await message.answer(
        "Удалить профиль, заказы, мерки и ссылки на фотографии из базы бота? "
        "Действие необратимо. Фотографии, уже пересланные портному в Telegram, "
        "нужно будет удалить отдельно.",
        reply_markup=kb
    )


@router.callback_query(F.data == "delete:no")
async def delete_no(call: CallbackQuery):
    await call.message.edit_text("Удаление отменено.")
    await call.answer()


@router.callback_query(F.data == "delete:yes")
async def delete_yes(call: CallbackQuery):
    await delete_my_data(call.from_user.id)
    await call.message.edit_text(
        "Данные удалены из базы бота. Копии фотографий и сообщений, уже отправленные "
        "портному в Telegram, могут потребовать отдельного удаления."
    )
    await call.answer()


@router.message()
async def fallback(message: Message):
    await message.answer("Используйте кнопки меню или отправьте /start.")


# -------------------- ЗАПУСК --------------------

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN. Создайте .env рядом с zakaz.py.")
    await init_db()
    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    log.info("CORSET AI запущен")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
