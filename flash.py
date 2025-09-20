# flash.py
import time

import streamlit as st

# соответствие типа -> иконка для toast
_ICON = {
    "success": "✅",
    "info": "ℹ️",
    "warning": "⚠️",
    "error": "❌",
}


def flash(msg: str, type: str = "success", ttl: float = 3.0):
    """
    Сохраняет сообщение во временном состоянии. Показ делается ОДИН раз,
    toast сам исчезает через TTL, без автоперезапусков страницы.
    """
    st.session_state["__flash__"] = {
        "msg": str(msg),
        "type": type if type in _ICON else "info",
        "exp": time.time() + ttl,
        "shown": False,
    }


def render_flash():
    """
    Если есть сообщение — показывает ЕГО ОДИН РАЗ через st.toast и больше не повторяет.
    Никаких автоперезапусков. При следующем действии пользователя запись будет удалена
    по TTL (или вручную можно очистить st.session_state["__flash__"]).
    """
    data = st.session_state.get("__flash__")
    if not data:
        return

    now = time.time()

    # TTL истёк — очистить и выйти
    if now > data.get("exp", 0):
        st.session_state.pop("__flash__", None)
        return

    # Уже показывали — просто ждём истечения TTL (toast сам исчезнет)
    if data.get("shown"):
        # по желанию можно в этом месте сразу удалить, чтобы точно не висело:
        # st.session_state.pop("__flash__", None)
        return

    # Показать toast один раз
    icon = _ICON.get(data.get("type", "info"), "ℹ️")
    st.toast(data["msg"], icon=icon)

    data["shown"] = True
    st.session_state["__flash__"] = data
