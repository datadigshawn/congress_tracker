"""
跳轉用：Streamlit Cloud 的 main file 指向此檔，實際邏輯在 offline/app.py。
"""
import os, sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TARGET = os.path.join(_HERE, "offline", "app.py")

# 確保根目錄在 path
sys.path.insert(0, _HERE)

with open(_TARGET, encoding="utf-8") as _f:
    exec(compile(_f.read(), _TARGET, "exec"), {"__file__": _TARGET, "__name__": "__main__"})
