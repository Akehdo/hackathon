import os
import time
import asyncio
import aiohttp
from typing import Any, Dict, Awaitable, Callable, Union, List

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message

from states import CsvState

BOT_TOKEN, BACKEND_URL = "", ""

if os.path.exists("bot/config.py"):
    from bot.config import BOT_TOKEN, BACKEND_URL
else:  
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    BACKEND_URL = os.getenv("BACKEND_URL")




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
    await message.answer(
        "Отправьте два CSV-файла.\n"
        "Можно по одному, можно сразу оба одним сообщением."
    )


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
        await message.answer(result)

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
        await message.answer(result)

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


# ==================== ЗАПУСК ====================
async def main():
    print("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
