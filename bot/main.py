import os
import time
import asyncio
import aiohttp
import json
import csv
from aiogram.types import FSInputFile

from typing import Any, Dict, Awaitable, Callable, Union, List

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message

from states import CsvState
from config import BOT_TOKEN, BACKEND_URL


# ==================== MIDDLEWARE ДЛЯ АЛЬБОМОВ ====================
class AlbumMiddleware(BaseMiddleware):
    def __init__(self, latency: float = 0.3):
        self.latency = latency
        self.albums: Dict[str, List[Message]] = {}

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:

        media_group = event.media_group_id

        # Если сообщение не относится к альбому → обрабатываем сразу
        if not media_group:
            return await handler(event, data)

        # Добавляем сообщение в альбом
        self.albums.setdefault(media_group, []).append(event)

        await asyncio.sleep(self.latency)

        # Передаём весь альбом только один раз — по последнему сообщению
        if media_group in self.albums:
            album = self.albums.pop(media_group)
            data["album"] = album
            return await handler(album[0], data)


# ==================== НАСТРОЙКА ====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

dp.message.outer_middleware(AlbumMiddleware())

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ==================== КОМАНДА START ====================
@dp.message(Command("start"))
async def start_cmd(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(CsvState.waiting_for_files)
    
    requirements1 = (
    "📌 <b>Требования к CSV-файлу transactions:</b>\n\n"
    "1️⃣ <b>Транзакции (transactions):</b>\n"
    "Обязательные колонки и типы:\n"
    "• <code>cst_dim_id</code> — float\n"
    "• <code>transdate</code> — datetime\n"
    "• <code>transdatetime</code> — string\n"
    "• <code>amount</code> — float\n"
    "• <code>docno</code> — int\n"
    "• <code>direction</code> — string\n"
    "• <code>target</code> — int (0/1)\n"
)

    requirements2 = (
        "📌 <b>Требования к CSV-файлу patterns:</b>\n\n"
        "2️⃣ <b>Паттерны (patterns):</b>\n"
        "Обязательные колонки и типы:\n"
        "• <code>transdate</code> — datetime\n"
        "• <code>cst_dim_id</code> — float\n"
        "• <code>monthly_os_changes</code> — int\n"
        "• <code>monthly_phone_model_changes</code> — int\n"
        "• <code>last_phone_model_categorical</code> — string\n"
        "• <code>last_os_categorical</code> — string\n"
        "• <code>logins_last_7_days</code> — int\n"
        "• <code>logins_last_30_days</code> — int\n"
        "• <code>login_frequency_7d</code> — float\n"
        "• <code>login_frequency_30d</code> — float\n"
        "• <code>freq_change_7d_vs_mean</code> — float\n"
        "• <code>logins_7d_over_30d_ratio</code> — float\n"
        "• <code>avg_login_interval_30d</code> — float\n"
        "• <code>std_login_interval_30d</code> — float\n"
        "• <code>var_login_interval_30d</code> — float\n"
        "• <code>ewm_login_interval_7d</code> — float\n"
        "• <code>burstiness_login_interval</code> — float\n"
        "• <code>fano_factor_login_interval</code> — float\n"
        "• <code>zscore_avg_login_interval_7d</code> — float\n\n"
    )



    await message.answer(
        "Отправьте два CSV-файла.\n"
        "Можно по одному, можно сразу оба одним сообщением." 
    )

    await message.answer(requirements1, parse_mode="HTML")
    await message.answer(requirements2, parse_mode="HTML")


# ==================== СОХРАНЕНИЕ ФАЙЛА ====================
async def save_file(document, user_id: int, num: int) -> str:
    file = await bot.get_file(document.file_id)
    ts = int(time.time())
    fname = f"{user_id}_{num}_{ts}.csv"
    path = os.path.join(UPLOAD_DIR, fname)
    await bot.download_file(file.file_path, path)
    return path


# ==================== ХЕНДЛЕР ПОЛУЧЕНИЯ CSV ====================
@dp.message(CsvState.waiting_for_files, F.document)
async def process_csv(message: Message, state: FSMContext, album: List[Message] | None = None):

    if album is None:
        album = [message]

    # Собираем все CSV документы
    csv_docs = [
        msg.document for msg in album
        if msg.document and msg.document.file_name.lower().endswith(".csv")
    ]

    if not csv_docs:
        return await message.answer("Отправьте только CSV файлы.")

    user_id = message.from_user.id
    data = await state.get_data()

    # ==== СОБРАНО ДВА СРАЗУ ====
    if len(csv_docs) >= 2:
        path1 = await save_file(csv_docs[0], user_id, 1)
        path2 = await save_file(csv_docs[1], user_id, 2)

        await message.answer("Получены два CSV файла. Отправляю на backend...")

        result = await send_to_backend(path1, path2)
        await send_csv_file(message, result)

        for p in (path1, path2):
            try: os.remove(p)
            except: pass

        await state.clear()
        return

    # ==== ПЕРВЫЙ И ВТОРОЙ ПО ОДНОМУ ====
    file1 = data.get("file1")

    if not file1:
        path1 = await save_file(csv_docs[0], user_id, 1)
        await state.update_data(file1=path1)
        return await message.answer("Первый файл получен. Отправьте второй CSV.")

    else:
        path2 = await save_file(csv_docs[0], user_id, 2)
        await message.answer("Второй файл получен. Отправляю на backend...")

        result = await send_to_backend(file1, path2)
        await send_csv_file(message, result)

        for p in (file1, path2):
            try: os.remove(p)
            except: pass

        await state.clear()


# ==================== ОТПРАВКА НА БЭКЕНД ====================
async def send_to_backend(file1: str, file2: str) -> str:
    try:
        async with aiohttp.ClientSession() as session:
            with open(file1, "rb") as f1, open(file2, "rb") as f2:
                form = aiohttp.FormData()
                form.add_field("file1", f1, filename="file1.csv")
                form.add_field("file2", f2, filename="file2.csv")

                async with session.post(BACKEND_URL, data=form) as resp:
                    return await resp.text()
    except Exception as e:
        return f"Ошибка: {e}"
    
    
async def send_csv_file(message: Message, text_result: str):
    import json
    import csv
    import os
    import time
    from aiogram.types import FSInputFile

    try:
        parsed = json.loads(text_result)
        rows = parsed.get("predictions", [])
        metrics = parsed.get("metrics")
        pretty = format_metrics(metrics)

        file_name = f"result_{int(time.time())}.csv"

        with open(file_name, "w", encoding="utf-8", newline="") as f:
            if rows:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
            else:
                f.write("empty")

        # создаём InputFile
        file_to_send = FSInputFile(file_name)

        await message.answer_document(
            document=file_to_send,
            caption="Готово! Результат во вложении (CSV)."
        )
        
        await message.answer(pretty, parse_mode="Markdown")

    except Exception as e:
        await message.answer(f"Ошибка при сборке CSV: {e}")

    finally:
        if os.path.exists(file_name):
            os.remove(file_name)

def format_metrics(metrics: dict) -> str:
    fraud = metrics.get("fraud", {})
    nonfraud = metrics.get("nonfraud", {})

    text = (
        "📊 *Итоговые метрики модели*\n\n"
        "🔴 *Мошенничество (fraud)*:\n"
        f"• Точность (precision): {fraud.get('precision'):.4f}\n"
        f"• Полнота (recall): {fraud.get('recall'):.4f}\n"
        f"• F1: {fraud.get('f1-score'):.4f}\n"
        f"• Кол-во примеров: {int(fraud.get('support', 0))}\n\n"
        
        "🟢 *Не мошенничество (nonfraud)*:\n"
        f"• Точность (precision): {nonfraud.get('precision'):.4f}\n"
        f"• Полнота (recall): {nonfraud.get('recall'):.4f}\n"
        f"• F1: {nonfraud.get('f1-score'):.4f}\n"
        f"• Кол-во примеров: {int(nonfraud.get('support', 0))}\n"
    )

    return text   

# ==================== ЗАПУСК ====================
async def main():
    print("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
