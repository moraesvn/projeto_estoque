"""Streamlit App – Expedição (Registro, KPIs, Configurações)

Estrutura de páginas:
- Registro: selecionar Marketplace, Operador, quantidade de pedidos; iniciar/encerrar etapas e registrar tempos.
- KPIs: filtros por mês ou intervalo; select boxes de Marketplace e Operador; métricas de tempo médio por pedido e gráfico por dia.
- Configurações: cadastro/remoção de Operadores e Marketplaces (usa config.render).
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, datetime

from db import (
    init_db,
    list_operators,
    list_marketplaces,
    get_or_create_session,
    update_session_orders,
    get_stage_times,
    start_stage,
    end_stage,
    fetch_daily_stage_durations,
    fetch_stage_totals_and_orders,
    STAGES,
)
import config as config_page

# -----------------------------
# Setup
# -----------------------------

def _ensure_db_once():
    if "_db_initialized" not in st.session_state:
        init_db()
        st.session_state["_db_initialized"] = True


def _fmt_hhmmss(seconds: float | int | None) -> str:
    if seconds is None:
        return "—"
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _parse_iso(ts: str | None) -> str:
    if not ts:
        return "—"
    # Show HH:MM:SS and date
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return str(ts)


# -----------------------------
# Page: Registro
# -----------------------------

def page_registro():
    st.title("📋 Registro de Tarefas")
    st.caption("Selecione o marketplace, operador e inclua a quantidade de pedidos. Inicie e finalize cada etapa.")

    # Seletor superior
    ops = list_operators()
    mkts = list_marketplaces()

    op_names = [o["name"] for o in ops]
    mkt_names = [m["name"] for m in mkts]

    c1, c2, c3 = st.columns([3, 3, 2])
    with c1:
        op_name = st.selectbox("Operador", op_names, index=0 if op_names else None, placeholder="Selecione o operador")
    with c2:
        mkt_name = st.selectbox("Marketplace", mkt_names, index=0 if mkt_names else None, placeholder="Selecione o marketplace")
    with c3:
        num_orders = st.number_input("Qtd. pedidos", min_value=0, step=1, value=0)

    if not op_names or not mkt_names:
        st.warning("Cadastre operadores e marketplaces na página Configurações.")
        return

    # Mapear para IDs
    op_id = next(o["id"] for o in ops if o["name"] == op_name)
    mkt_id = next(m["id"] for m in mkts if m["name"] == mkt_name)

    # Criar/obter sessão do dia
    today = date.today()
    if ("session_key" not in st.session_state) or (
        st.session_state.get("_op_id") != op_id
        or st.session_state.get("_mkt_id") != mkt_id
        or st.session_state.get("_date") != today.isoformat()
    ):
        session_id = get_or_create_session(op_id, mkt_id, today, num_orders)
        st.session_state["session_key"] = session_id
        st.session_state["_op_id"] = op_id
        st.session_state["_mkt_id"] = mkt_id
        st.session_state["_date"] = today.isoformat()
    else:
        session_id = st.session_state["session_key"]
        # Atualiza pedidos quando usuário alterar
        update_session_orders(session_id, int(num_orders))

    st.info(f"Sessão {session_id} — Data: {today.strftime('%d/%m/%Y')} — Operador: {op_name} — Marketplace: {mkt_name}")

    # Tabela de etapas com botões
    times = get_stage_times(session_id)

    for stage in STAGES:
        st.subheader(stage)
        c1, c2, c3, c4 = st.columns([2, 2, 3, 3])
        with c1:
            if st.button("▶️ Iniciar", key=f"start_{stage}"):
                start_stage(session_id, stage)
                st.rerun()
        with c2:
            if st.button("⏹️ Encerrar", key=f"end_{stage}"):
                end_stage(session_id, stage)
                st.rerun()
        with c3:
            st.write(f"Início: **{_parse_iso(times.get(stage, {}).get('start_time'))}**")
        with c4:
            st.write(f"Fim: **{_parse_iso(times.get(stage, {}).get('end_time'))}**")
        st.divider()


# -----------------------------
# Page: KPIs
# -----------------------------

def page_kpis():
    st.title("📈 KPIs Logística")

    # Filtros
    st.sidebar.header("Filtros")
    mode = st.sidebar.radio("Período", ["Mês", "Intervalo"], horizontal=True)

    if mode == "Mês":
        # Seleção de mês/ano
        ref = st.sidebar.date_input("Mês de referência", value=date.today().replace(day=1))
        start = ref.replace(day=1)
        # Próximo mês - 1 dia
        if start.month == 12:
            end = date(start.year + 1, 1, 1)
        else:
            end = date(start.year, start.month + 1, 1)
        end = end.replace(day=1)
        start_str, end_str = start.isoformat(), (end.isoformat())
        # Usaremos BETWEEN inclusive; para mensal, considerar até véspera do próximo mês na consulta (ajuste abaixo)
        end_inc = (pd.to_datetime(end_str) - pd.Timedelta(days=1)).date().isoformat()
    else:
        start_ref = st.sidebar.date_input("Início", value=date.today().replace(day=1))
        end_ref = st.sidebar.date_input("Fim", value=date.today())
        if start_ref > end_ref:
            st.sidebar.error("Data inicial não pode ser maior que a final.")
        start_str, end_inc = start_ref.isoformat(), end_ref.isoformat()

    # Select boxes
    ops = list_operators()
    mkts = list_marketplaces()
    op_map = {"Todos": None}
    op_map.update({row["name"]: row["id"] for row in ops})
    mkt_map = {"Todos": None}
    mkt_map.update({row["name"]: row["id"] for row in mkts})

    op_choice = st.selectbox("Operador", list(op_map.keys()), index=0)
    mkt_choice = st.selectbox("Marketplace", list(mkt_map.keys()), index=0)

    operator_id = op_map[op_choice]
    marketplace_id = mkt_map[mkt_choice]

    # Métricas de tempo médio por pedido
    totals = fetch_stage_totals_and_orders(start_str, end_inc, operator_id, marketplace_id)

    st.subheader("Tempo médio por pedido (por etapa)")
    c = st.container()
    cols = c.columns(3)
    for i, stage in enumerate(STAGES):
        avg = totals.get(stage, {}).get("avg_seconds_per_order", 0.0)
        cols[i].metric(stage, _fmt_hhmmss(avg))

    st.divider()

    # Gráfico de barras por dia
    st.subheader("Tempo total por dia e etapa")
    rows = fetch_daily_stage_durations(start_str, end_inc, operator_id, marketplace_id)

    if not rows:
        st.info("Sem dados para o período/filters.")
        return

    df = pd.DataFrame(rows, columns=["day", "stage", "duration_seconds"])  # rows é sqlite3.Row
    # Garantir ordenação dos estágios
    df["stage"] = pd.Categorical(df["stage"], categories=list(STAGES), ordered=True)

    # Converter para minutos
    df["duration_min"] = (df["duration_seconds"].astype(float) / 60.0).round(2)

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("day:T", title="Dia"),
            y=alt.Y("sum(duration_min):Q", title="Minutos"),
            color=alt.Color("stage:N", title="Etapa"),
            tooltip=["day:T", "stage:N", alt.Tooltip("sum(duration_min):Q", title="Minutos")],
        )
        .properties(height=360)
    )
    st.altair_chart(chart, use_container_width=True)


# -----------------------------
# Page: Configurações
# -----------------------------

def page_config():
    config_page.render()


# -----------------------------
# Router
# -----------------------------

def main():
    _ensure_db_once()

    st.set_page_config(page_title="Logística", page_icon="📦", layout="wide")

    with st.sidebar:
        st.title("📦 Logística")
        page = st.radio("Navegação", ("Registro", "KPIs", "Configurações"))
    

    if page == "Registro":
        page_registro()
    elif page == "KPIs":
        page_kpis()
    else:
        page_config()


if __name__ == "__main__":
    main()
