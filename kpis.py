"""KPIs – página separada (Streamlit)

Estrutura modular para manter o código organizado em blocos pequenos.
Use `render()` a partir do `app.py`.
"""
from __future__ import annotations

from datetime import date
import pandas as pd
import streamlit as st
import altair as alt

from db import (
    list_operators,
    list_marketplaces,
    fetch_daily_stage_durations,
    fetch_stage_totals_and_orders,
    fetch_end_to_end_totals,
    STAGES,
)

# -----------------------------
# Helpers
# -----------------------------

def _fmt_hhmmss(seconds: float | int | None) -> str:
    if not seconds:
        return "00:00:00"
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


# -----------------------------
# Filtros (sidebar)
# -----------------------------

def sidebar_period_filters():
    """Renderiza filtros de período (Mês | Intervalo) e retorna (start_iso, end_iso)."""
    st.sidebar.header("Filtros")
    mode = st.sidebar.radio("Período", ["Mês", "Intervalo"], horizontal=True)

    if mode == "Mês":
        meses = [
            "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
        ]
        mes = st.sidebar.selectbox("Mês", meses, index=date.today().month - 1)
        ano = st.sidebar.number_input("Ano", min_value=2000, max_value=2100, value=date.today().year, step=1)

        start = date(ano, meses.index(mes) + 1, 1)
        if start.month == 12:
            end = date(start.year + 1, 1, 1)
        else:
            end = date(start.year, start.month + 1, 1)
        # intervalo inclusivo: [start, end-1]
        end_inc = (pd.to_datetime(end) - pd.Timedelta(days=1)).date()
        return start.isoformat(), end_inc.isoformat()

    # Intervalo de datas (dd/mm/yyyy mostrado na UI; internamente usamos ISO)
    start_ref = st.sidebar.date_input("Início", value=date.today().replace(day=1), format="DD/MM/YYYY")
    end_ref = st.sidebar.date_input("Fim", value=date.today(), format="DD/MM/YYYY")
    if start_ref > end_ref:
        st.sidebar.error("Data inicial não pode ser maior que a final.")
    return start_ref.isoformat(), end_ref.isoformat()


def sidebar_entity_filters():
    """Renderiza filtros de Operador, Marketplace e Etapa.
    Retorna (operator_id|None, marketplace_id|None, stage|None).
    """
    ops = list_operators()
    mkts = list_marketplaces()

    op_choices = ["Todos"] + [o["name"] for o in ops]
    mkt_choices = ["Todos"] + [m["name"] for m in mkts]
    stage_choices = ["Todas"] + list(STAGES)

    c1, c2, c3 = st.columns(3)
    with c1:
        op_choice = st.selectbox("Operador", op_choices, index=0)
    with c2:
        mkt_choice = st.selectbox("Marketplace", mkt_choices, index=0)
    with c3:
        stage_choice = st.selectbox("Etapa", stage_choices, index=0)

    op_map = {row["name"]: row["id"] for row in ops}
    mkt_map = {row["name"]: row["id"] for row in mkts}

    operator_id = op_map.get(op_choice) if op_choice != "Todos" else None
    marketplace_id = mkt_map.get(mkt_choice) if mkt_choice != "Todos" else None
    stage = None if stage_choice == "Todas" else stage_choice

    return operator_id, marketplace_id, stage


# -----------------------------
# Data access (pequenos blocos)
# -----------------------------

def load_totals(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None):
    return fetch_stage_totals_and_orders(start_iso, end_iso, operator_id, marketplace_id)


def load_daily(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None):
    rows = fetch_daily_stage_durations(start_iso, end_iso, operator_id, marketplace_id)
    return pd.DataFrame(rows, columns=["day", "stage", "duration_seconds"]) if rows else pd.DataFrame(columns=["day","stage","duration_seconds"]) 

#Calcula média de pedidos por hora
def card_orders_per_hour(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None, stage: str | None):
    
    if stage is None:
        total_seconds, total_orders = fetch_end_to_end_totals(start_iso, end_iso, operator_id, marketplace_id)
    else:
        totals = fetch_stage_totals_and_orders(start_iso, end_iso, operator_id, marketplace_id)
        total_seconds = float(totals.get(stage, {}).get("total_seconds", 0.0))
        total_orders = float(totals.get(stage, {}).get("total_orders", 0.0))

    pedidos_por_hora = (total_orders / (total_seconds / 3600)) if total_seconds > 0 else 0.0
    st.metric("**:blue[Média de pedidos por hora]**", f"{pedidos_por_hora:.0f} pedidos/h", border=True)




#CARD PARA MOSTRAR MEDIA DE TEMPO GASTO POR DIA COM PEDIDOS (PODE FILTRAR)
def card_avg_daily_total_time(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None, stage: str | None):
    from db import fetch_daily_end_to_end
    rows = fetch_daily_end_to_end(start_iso, end_iso, operator_id, marketplace_id, stage)

    if not rows:
        st.metric("**:blue[Horas utilizadas por dia]**", "00:00:00", border=True); return

    secs = [int(r["total_seconds"] or 0) for r in rows if (r["total_seconds"] or 0) > 0]
    if not secs:
        st.metric("**:blue[Horas utilizadas por dia", "00:00:00]**", border=True); return

    avg_sec = int(round(sum(secs) / len(secs)))
    h, rem = divmod(avg_sec, 3600); m, s = divmod(rem, 60)
    st.metric("**:blue[Horas utilizadas por dia]**", f"{h:02d}:{m:02d}:{s:02d}", border=True)


def card_avg_time_per_order(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None, stage: str | None):

    if stage is None:
        total_seconds, total_orders = fetch_end_to_end_totals(start_iso, end_iso, operator_id, marketplace_id)
    else:
        totals = fetch_stage_totals_and_orders(start_iso, end_iso, operator_id, marketplace_id)
        total_seconds = float(totals.get(stage, {}).get("total_seconds", 0.0))
        total_orders = float(totals.get(stage, {}).get("total_orders", 0.0))

    if not total_orders or total_seconds <= 0:
        st.metric("**:blue[Tempo médio por pedido]**", "00:00:00", border=True)
        return

    avg_sec = int(round(total_seconds / total_orders))
    h, rem = divmod(avg_sec, 3600); m, s = divmod(rem, 60)
    st.metric("**:blue[Tempo médio por pedido]**", f"{h:02d}:{m:02d}:{s:02d}", border=True)




#TABELA COMPLETA SESSIONS DO BANCO DE DADOS
def table_all_sessions():
    """Mostra todas as sessões (com nomes de operador e marketplace)."""
    from db import get_conn
    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT s.*,
                   o.name AS operator_name,
                   m.name AS marketplace_name
              FROM sessions s
              JOIN operators o   ON o.id = s.operator_id
              JOIN marketplaces m ON m.id = s.marketplace_id
             ORDER BY datetime(s.created_at) DESC, s.id DESC;
        """).fetchall()
    df = pd.DataFrame(rows, columns=rows[0].keys()) if rows else pd.DataFrame()
    st.subheader("Todas as sessões")
    st.dataframe(df, use_container_width=True)

#TABELA COMPLETA STAGE_EVENTS DO BANCO DE DADOS
def table_all_stage_events():
    """Mostra todos os registros da tabela stage_events com dados da sessão."""
    from db import get_conn
    with get_conn(readonly=True) as conn:
        rows = conn.execute("""
            SELECT e.*,
                   s.date AS session_date,
                   s.num_orders,
                   o.name AS operator_name,
                   m.name AS marketplace_name
              FROM stage_events e
              JOIN sessions s     ON s.id = e.session_id
              JOIN operators o    ON o.id = s.operator_id
              JOIN marketplaces m ON m.id = s.marketplace_id
             ORDER BY e.session_id DESC, e.id ASC;
        """).fetchall()
    df = pd.DataFrame(rows, columns=rows[0].keys()) if rows else pd.DataFrame()
    st.subheader("Todas as etapas")
    st.dataframe(df, use_container_width=True)


def chart_orders_vs_time(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None, stage: str | None):
    from db import fetch_daily_end_to_end

    # ---- dados de tempo gasto por dia ----
    rows_time = fetch_daily_end_to_end(start_iso, end_iso, operator_id, marketplace_id, stage)
    df_time = pd.DataFrame(rows_time, columns=["day", "total_seconds"]) if rows_time else pd.DataFrame(columns=["day","total_seconds"])
    if not df_time.empty:
        df_time["total_hours"] = df_time["total_seconds"] / 3600

    # ---- dados de pedidos/hora por dia ----
    # usamos stage_totals para pegar pedidos e tempo
    from db import fetch_end_to_end_totals, fetch_stage_totals_and_orders
    per_day = []
    current = pd.to_datetime(start_iso)
    end_dt = pd.to_datetime(end_iso)
    while current <= end_dt:
        d = current.date().isoformat()
        if stage is None:
            secs, orders = fetch_end_to_end_totals(d, d, operator_id, marketplace_id)
        else:
            totals = fetch_stage_totals_and_orders(d, d, operator_id, marketplace_id)
            secs = totals.get(stage, {}).get("total_seconds", 0)
            orders = totals.get(stage, {}).get("total_orders", 0)
        per_hour = (orders / (secs/3600)) if secs and orders else 0
        per_day.append({"day": d, "orders_per_hour": per_hour})
        current += pd.Timedelta(days=1)
    df_orders = pd.DataFrame(per_day)

    if df_time.empty and df_orders.empty:
        st.info("Sem dados para o período.")
        return

    # após montar df_time
    df_time = pd.DataFrame(rows_time, columns=["day", "total_seconds"]) if rows_time else pd.DataFrame(columns=["day","total_seconds"])
    # garanta a coluna, mesmo se vazio
    df_time["total_hours"] = (df_time["total_seconds"].astype(float) / 3600.0).fillna(0.0)

    # ---- unir dados ----
    df = pd.merge(df_orders, df_time[["day","total_hours"]], on="day", how="outer").fillna(0)

    # ---- gráfico ----
    base = alt.Chart(df).encode(x="day:T")

    line1 = base.mark_line(point=True, color="steelblue").encode(
        y=alt.Y("orders_per_hour:Q", axis=alt.Axis(title="Pedidos por hora", titleColor="steelblue"))
    )
    line2 = base.mark_line(point=True, color="green").encode(
        y=alt.Y("total_hours:Q", axis=alt.Axis(title="Tempo gasto (h)", titleColor="green"))
    )

    st.subheader("Produtividade x Tempo por dia")
    st.altair_chart(alt.layer(line1, line2).resolve_scale(y="independent").interactive(), use_container_width=True)


# -----------------------------
# Render principal
# -----------------------------


def render():
    st.title("📈 KPIs Logística")
    start_iso, end_iso = sidebar_period_filters()
    operator_id, marketplace_id, stage = sidebar_entity_filters()

    st.divider()

    
    st.subheader(" :green[📊 Métricas]")


    # Dentro do render(), onde quiser mostrar os cards:
    col1, col2, col3 = st.columns(3)
    with col1:
        card_orders_per_hour(start_iso, end_iso, operator_id, marketplace_id, stage)

    with col2:
        card_avg_daily_total_time(start_iso, end_iso, operator_id, marketplace_id, stage)    
    with col3:
        card_avg_time_per_order(start_iso, end_iso, operator_id, marketplace_id, stage)


    st.divider()

    chart_orders_vs_time(start_iso, end_iso, operator_id, marketplace_id, stage)


    table_all_sessions()   
    table_all_stage_events()
       

