# flash.py
import time

import streamlit as st


def flash(msg: str, type: str = "success", ttl: float = 3.0):
    st.session_state["__flash__"] = {"msg": msg, "type": type, "exp": time.time() + ttl}


def render_flash():
    data = st.session_state.get("__flash__")
    if not data:
        return
    if time.time() > data.get("exp", 0):
        st.session_state.pop("__flash__", None)
        return
    t = data["type"]
    if t == "success":
        st.success(data["msg"])
    elif t == "warning":
        st.warning(data["msg"])
    elif t == "error":
        st.error(data["msg"])
    else:
        st.info(data["msg"])
