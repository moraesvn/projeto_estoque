"""Página de Configurações (Streamlit)

Permite cadastrar e remover Operadores e Marketplaces.
Use `render()` a partir do `app.py` para exibir esta página.
"""
from __future__ import annotations

import streamlit as st
from db import (
    init_db,
    add_operator,
    remove_operator,
    list_operators,
    add_marketplace,
    remove_marketplace,
    list_marketplaces,
)


# -----------------------------
# Helpers
# -----------------------------

def _normalize_name(name: str) -> str:
    return " ".join(name.split()).strip()


def _ensure_db_once() -> None:
    if "_db_initialized" not in st.session_state:
        init_db()
        st.session_state["_db_initialized"] = True


# -----------------------------
# Render
# -----------------------------

def render() -> None:
    _ensure_db_once()

    st.title("⚙️ Configurações")
    st.caption("Cadastre operadores e marketplaces. Remova itens quando necessário.")

    tab_ops, tab_mkts = st.tabs(["Operadores", "Marketplaces"]) 

    with tab_ops:
        st.subheader("Operadores")
        with st.form("form_add_operator", clear_on_submit=True):
            name = st.text_input("Nome do operador", placeholder="Ex.: João Silva")
            submitted = st.form_submit_button("➕ Adicionar operador", use_container_width=True)
        if submitted:
            clean = _normalize_name(name)
            if not clean:
                st.error("Informe um nome válido.")
            else:
                add_operator(clean)
                st.success(f"Operador '{clean}' cadastrado/confirmado.")

        st.divider()
        st.markdown("**Lista de operadores**")
        ops = list_operators(active_only=False)
        if not ops:
            st.info("Nenhum operador cadastrado.")
        else:
            for row in ops:
                c1, c2, c3 = st.columns([6, 2, 2])
                with c1:
                    st.write(f"👤 {row['name']}")
                with c2:
                    st.write("Ativo" if row["active"] else "Inativo")
                with c3:
                    if st.button("Remover", key=f"del_op_{row['id']}", type="secondary"):
                        remove_operator(int(row["id"]))
                        st.rerun()

    with tab_mkts:
        st.subheader("Marketplaces")
        with st.form("form_add_marketplace", clear_on_submit=True):
            name = st.text_input("Nome do marketplace", placeholder="Ex.: Shopee")
            submitted = st.form_submit_button("➕ Adicionar marketplace", use_container_width=True)
        if submitted:
            clean = _normalize_name(name)
            if not clean:
                st.error("Informe um nome válido.")
            else:
                add_marketplace(clean)
                st.success(f"Marketplace '{clean}' cadastrado/confirmado.")

        st.divider()
        st.markdown("**Lista de marketplaces**")
        mkts = list_marketplaces(active_only=False)
        if not mkts:
            st.info("Nenhum marketplace cadastrado.")
        else:
            for row in mkts:
                c1, c2, c3 = st.columns([6, 2, 2])
                with c1:
                    st.write(f"🏬 {row['name']}")
                with c2:
                    st.write("Ativo" if row.get("active", 1) else "Inativo")
                with c3:
                    if st.button("Remover", key=f"del_mkt_{row['id']}", type="secondary"):
                        remove_marketplace(int(row["id"]))
                        st.rerun()


# Execução direta (útil para desenvolvimento isolado)
if __name__ == "__main__":
    render()
