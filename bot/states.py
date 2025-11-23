from aiogram.fsm.state import StatesGroup, State

class CsvState(StatesGroup):
    waiting_for_files = State()
