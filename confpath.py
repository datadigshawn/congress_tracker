"""共用路徑常數，供 online / offline 子目錄內的程式 import。"""
import os

PROJECT_ROOT    = os.path.dirname(os.path.abspath(__file__))
TICKER_INFO_DB  = os.path.join(PROJECT_ROOT, "ticker_info.db")
DATA_DB         = os.path.join(PROJECT_ROOT, "data.db")
