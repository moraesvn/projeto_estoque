"""Página de Configurações (Streamlit)

Permite cadastrar e remover Operadores e Marketplaces.
Use `render()` a partir do `app.py` para exibir esta página.
"""
from __future__ import annotations

import streamlit as st
from db import (
    delete_session,
    init_db,
    add_operator,
    remove_operator,
    list_operators,
    add_marketplace,
    remove_marketplace,
    list_marketplaces,
    get_conn,
    delete_session
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

def data_editor_sessions_delete():
    """Lista sessões em st.data_editor com checkbox e permite excluir as selecionadas."""
    import pandas as pd
    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT s.id, s.date, s.num_orders, s.created_at,
                   o.name AS operator_name, m.name AS marketplace_name
              FROM sessions s
              JOIN operators o ON o.id = s.operator_id
              JOIN marketplaces m ON m.id = s.marketplace_id
             ORDER BY datetime(s.created_at) DESC, s.id DESC;
        """).fetchall()

    df = pd.DataFrame(rows, columns=rows[0].keys()) if rows else pd.DataFrame(
        columns=["id","date","num_orders","created_at","operator_name","marketplace_name"]
    )
    if df.empty:
        st.info("Sem sessões para excluir.")
        return

    df.insert(0, "Selecionar", False)
    edited = st.data_editor(
        df, use_container_width=True, hide_index=True, num_rows="fixed",
        disabled=["id","date","num_orders","created_at","operator_name","marketplace_name"]
    )
    selecionados = edited.loc[edited["Selecionar"] == True, "id"].astype(int).tolist()

    c1, c2 = st.columns([1,3])
    with c1:
        if st.button(f"🗑️ Excluir {len(selecionados)} selecionado(s)", disabled=(len(selecionados) == 0)):
            st.session_state["_confirm_delete_ids"] = selecionados
            st.warning(f"Confirma excluir {len(selecionados)} sessão(ões)? Ação irreversível.")
    with c2:
        if st.session_state.get("_confirm_delete_ids"):
            if st.button("✅ Confirmar exclusão"):
                for sid in st.session_state["_confirm_delete_ids"]:
                    delete_session(int(sid))
                st.success(f"{len(st.session_state['_confirm_delete_ids'])} sessão(ões) excluída(s).")
                st.session_state.pop("_confirm_delete_ids", None)
                st.rerun()




# -----------------------------
# Render
# -----------------------------

def render() -> None:
    _ensure_db_once()

    st.title("⚙️ Configurações")
    st.caption("Cadastre operadores e marketplaces. Defina a meta e gerencie lotes se necessário.")

    tab_ops, tab_mkts, tab_cfg = st.tabs(["Operadores", "Marketplaces", "Configurações"]) 

    with tab_ops:
        st.subheader("Operadores")
        with st.form("form_add_operator", clear_on_submit=True):
            name = st.text_input("Nome do operador", placeholder="Ex.: Fernando")
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
                # Operadores – botão
                with c3:
                    if row["active"]:
                        if st.button("Inativar", key=f"del_op_{row['id']}", type="secondary"):
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
                    st.write("Ativo" if row["active"] else "Inativo")
                # Marketplaces – botão
                with c3:
                    if row["active"]:
                        if st.button("Inativar", key=f"del_mkt_{row['id']}", type="secondary"):
                            remove_marketplace(int(row["id"]))
                            st.rerun()

    with tab_cfg:
        st.subheader("Excluir lotes")
        data_editor_sessions_delete()


# Execução direta (útil para desenvolvimento isolado)
if __name__ == "__main__":
    render()
