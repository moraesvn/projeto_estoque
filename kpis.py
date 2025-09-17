"""KPIs – página separada (Streamlit)

Estrutura modular para manter o código organizado em blocos pequenos.
Use `render()` a partir do `app.py`.
"""
from __future__ import annotations

from datetime import date, datetime
import pandas as pd
import streamlit as st
import altair as alt

from db import (
    list_operators,
    list_marketplaces,
    fetch_daily_stage_durations,
    fetch_stage_totals_and_orders,
    fetch_end_to_end_totals,
    get_setting,
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

#filtrar operadores por valores distintos
def _distinct_packers():
    from db import get_conn
    with get_conn(readonly=True) as conn:
        rows = conn.execute("SELECT DISTINCT packers_count FROM sessions ORDER BY packers_count;").fetchall()
    return [int(r["packers_count"]) for r in rows] if rows else []

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
    """Operador (com 'Todos'), Empacotadores (sem 'Todos'), Etapa (com 'Todas').
    Retorna (operator_id|None, packers_count:int, stage|None).
    """
    ops = list_operators()
    op_choices = ["Todos"] + [o["name"] for o in ops]

    # apenas os valores já usados em algum lote
    packers_list = _distinct_packers()  # ex.: [1, 2, 3]
    if not packers_list:
        st.warning("Nenhum lote criado ainda. Definindo Empacotadores como 1.")
        packers_list = [1]

    # default: 2 se existir, senão 1, senão o primeiro da lista
    if 2 in packers_list:
        default_idx_pack = packers_list.index(2)
    elif 1 in packers_list:
        default_idx_pack = packers_list.index(1)
    else:
        default_idx_pack = 0

    stage_choices = ["Todas"] + list(STAGES)

    c1, c2, c3 = st.columns(3)
    with c1:
        op_choice = st.selectbox("Operador", op_choices, index=0)
    with c2:
        packers_choice = st.selectbox("Empacotadores", packers_list, index=default_idx_pack)
    with c3:
        stage_choice = st.selectbox("Etapa", stage_choices, index=0)

    op_map = {row["name"]: row["id"] for row in ops}
    operator_id = op_map.get(op_choice) if op_choice != "Todos" else None
    packers_count = int(packers_choice)  # sempre um número válido
    stage = None if stage_choice == "Todas" else stage_choice
    return operator_id, packers_count, stage



#FUNCAO PARA RECEBER A UNIR DOS INTERVALOR POR DIA. 
def _union_active_seconds_by_day(rows) -> pd.DataFrame:
    """
    Recebe rows com colunas: day (YYYY-MM-DD), start_ts ('YYYY-MM-DD HH:MM:SS'), end_ts (idem).
    Retorna DataFrame: columns=['day','active_seconds'] com a soma da UNIÃO de intervalos por dia.
    """
    by_day = {}
    for r in rows:
        day = r["day"]
        s = r["start_ts"]; e = r["end_ts"]
        if not (s and e):
            continue
        by_day.setdefault(day, []).append((
            datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(e, "%Y-%m-%d %H:%M:%S"),
        ))

    out = []
    for day, intervals in by_day.items():
        if not intervals:
            out.append({"day": day, "active_seconds": 0})
            continue
        # ordenar por início
        intervals.sort(key=lambda t: t[0])
        merged = []
        cur_s, cur_e = intervals[0]
        for s, e in intervals[1:]:
            if s <= cur_e:  # sobreposição
                if e > cur_e:
                    cur_e = e
            else:
                merged.append((cur_s, cur_e))
                cur_s, cur_e = s, e
        merged.append((cur_s, cur_e))
        # somar duração dos mergeados
        total = sum(int((e - s).total_seconds()) for s, e in merged)
        out.append({"day": day, "active_seconds": total})

    return pd.DataFrame(out, columns=["day", "active_seconds"]).sort_values("day")

def compute_orders_per_hour_union(start_iso: str, end_iso: str, operator_id: int | None, packers_count: int | None, stage: str | None):
    from db import fetch_session_intervals, get_conn

    rows = fetch_session_intervals(start_iso, end_iso, operator_id, packers_count, stage)
    df_union = _union_active_seconds_by_day(rows)
    active_seconds = int(df_union["active_seconds"].sum()) if not df_union.empty else 0

    params = [start_iso, end_iso]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?"); params.append(operator_id)
    if packers_count:
        flt.append("s.packers_count = ?"); params.append(packers_count)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""

    with get_conn(readonly=True) as conn:
        row = conn.execute(
            f"SELECT COALESCE(SUM(s.num_orders), 0) AS total_orders FROM sessions s WHERE s.date BETWEEN ? AND ? {where_extra};",
            params,
        ).fetchone()
    total_orders = int(row["total_orders"] or 0)

    pedidos_por_hora = (total_orders / (active_seconds / 3600.0)) if active_seconds > 0 else 0.0
    return pedidos_por_hora, active_seconds, total_orders

# -----------------------------
# Data access 
# -----------------------------

def load_totals(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None):
    return fetch_stage_totals_and_orders(start_iso, end_iso, operator_id, marketplace_id)


def load_daily(start_iso: str, end_iso: str, operator_id: int | None, marketplace_id: int | None):
    rows = fetch_daily_stage_durations(start_iso, end_iso, operator_id, marketplace_id)
    return pd.DataFrame(rows, columns=["day", "stage", "duration_seconds"]) if rows else pd.DataFrame(columns=["day","stage","duration_seconds"]) 


#Calcula média de pedidos por hora
def card_orders_per_hour(start_iso: str, end_iso: str, operator_id: int | None, packers_count: int | None, stage: str | None):
    from db import get_setting
    pph, active_seconds, total_orders = compute_orders_per_hour_union(start_iso, end_iso, operator_id, packers_count, stage)


    # meta inteira e delta em texto humano
    target = int(float(get_setting("orders_per_hour_target", "0") or 0))
    diff = pph - target
    if diff > 0:
        delta_text = f"{diff:.0f} pedidos acima da meta ({target})"
    elif diff < 0:
        delta_text = f"{abs(diff):.0f} pedidos abaixo da meta ({target})"
    else:
        delta_text = f"Estamos no alvo ({target})"
    with st.container(border=True):
        st.metric("**:blue[Média de pedidos por hora]**", f"{pph:.0f} pedidos/h", delta=delta_text)






#CARD PARA MOSTRAR MEDIA DE TEMPO GASTO POR DIA COM PEDIDOS (PODE FILTRAR)
def card_avg_daily_total_time(start_iso: str, end_iso: str, operator_id: int | None, packers_count: int | None, stage: str | None):
    from db import fetch_session_intervals
    rows = fetch_session_intervals(start_iso, end_iso, operator_id, packers_count, stage)
    df_union = _union_active_seconds_by_day(rows)

    if df_union.empty:
        st.metric("**:green[Média de tempo utilizado por dia]**", "00:00:00", border=True)
        return

    # média entre dias com dados (tempo ativo, sem dupla contagem)
    avg_sec = int(round(df_union["active_seconds"].mean()))
    h, rem = divmod(avg_sec, 3600)
    m, s = divmod(rem, 60)

    st.metric("**:green[Média de tempo utilizado por dia]**", f"{h:02d}:{m:02d}:{s:02d}", border=True)



def card_avg_time_per_order(start_iso: str, end_iso: str, operator_id: int | None, packers_count: int | None, stage: str | None):
    pph, active_seconds, total_orders = compute_orders_per_hour_union(start_iso, end_iso, operator_id, packers_count, stage)

    if total_orders <= 0 or active_seconds <= 0:
        st.metric("**:yellow[Tempo médio por pedido]**", "00:00:00", border=True)
        return

    avg_sec = int(round(active_seconds / total_orders))
    h, rem = divmod(avg_sec, 3600); m, s = divmod(rem, 60)
    st.metric("**:yellow[Tempo médio por pedido]**", f"{h:02d}:{m:02d}:{s:02d}", border=True)





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
    st.dataframe(df, width="stretch")

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
    st.dataframe(df, width="stretch")


def chart_orders_vs_time(start_iso: str, end_iso: str, operator_id: int | None, packers_count: int | None, stage: str | None):
    from db import fetch_daily_end_to_end, get_conn, get_setting
    import altair as alt
    import math

    # ---- tempo ativo (união) por dia ----
    rows_time = fetch_daily_end_to_end(start_iso, end_iso, operator_id, packers_count, stage)
    df_time = pd.DataFrame(rows_time, columns=["day", "total_seconds"]) if rows_time else pd.DataFrame(columns=["day","total_seconds"])
    df_time["total_hours"] = (df_time["total_seconds"].astype(float) / 3600.0).fillna(0.0)

    # ---- pedidos por dia ----
    params = [start_iso, end_iso]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?"); params.append(operator_id)
    if packers_count:
        flt.append("s.packers_count = ?"); params.append(packers_count)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""
    with get_conn(readonly=True) as conn:
        rows_orders = conn.execute(
            f"""
            SELECT s.date AS day, COALESCE(SUM(s.num_orders),0) AS total_orders
              FROM sessions s
             WHERE s.date BETWEEN ? AND ? {where_extra}
             GROUP BY s.date
             ORDER BY s.date;
            """,
            params,
        ).fetchall()
    df_orders = pd.DataFrame(rows_orders, columns=["day","total_orders"]) if rows_orders else pd.DataFrame(columns=["day","total_orders"])

    # ---- consolidar + pedidos/h por dia ----
    df = pd.merge(df_orders, df_time[["day","total_hours"]], on="day", how="outer").fillna(0)
    df["orders_per_hour"] = df.apply(lambda r: (r["total_orders"] / r["total_hours"]) if r["total_hours"] > 0 else 0.0, axis=1)

    if df.empty:
        st.info("Sem dados para o período."); 
        return

    # ---- ticks & domínios ----
    # eixo esquerdo (pedidos/h) de 30 em 30
    max_pph = max(float(df["orders_per_hour"].max() or 0.0), float(get_setting("orders_per_hour_target", "0") or 0))
    max_pph = math.ceil(max_pph / 30.0) * 30  # arredonda para múltiplo de 30
    pph_ticks = list(range(0, int(max_pph) + 30, 30)) or [0, 30, 60]

    # eixo direito (horas) de 0.5 em 0.5
    max_hours = float(df["total_hours"].max() or 0.0)
    max_hours = math.ceil(max_hours * 2) / 2.0  # próximo múltiplo de 0.5
    if max_hours == 0:
        max_hours = 0.5
    hour_ticks = [i / 2 for i in range(0, int(max_hours * 2) + 1)]  # 0, 0.5, 1.0, ...

    # meta (linha horizontal no eixo esquerdo)
    target = int(float(get_setting("orders_per_hour_target", "0") or 0))
    rule_df = df[["day"]].copy()
    rule_df["target"] = target

    base = alt.Chart(df).encode(x=alt.X("day:T", title="Dia"))

    line_pph = base.mark_line(point=True, color="steelblue").encode(
        y=alt.Y(
            "orders_per_hour:Q",
            title="Pedidos por hora",
            scale=alt.Scale(domain=[0, max_pph]),
            axis=alt.Axis(titleColor="steelblue", values=pph_ticks),
        )
    )

    line_hours = base.mark_line(point=True, color="green").encode(
        y=alt.Y(
            "total_hours:Q",
            title="Tempo ativo (h)",
            scale=alt.Scale(domain=[0, max_hours]),
            axis=alt.Axis(titleColor="green", values=hour_ticks),
        )
    )

    rule = alt.Chart(rule_df).mark_rule(color="red", strokeDash=[4,4]).encode(
        x="day:T",
        y=alt.Y("target:Q", scale=alt.Scale(domain=[0, max_pph]), axis=None)
    )

    st.subheader("Produtividade x Tempo por dia")
    chart = alt.layer(line_pph, line_hours, rule).resolve_scale(y="independent").interactive()
    st.altair_chart(chart, use_container_width=True)


# -----------------------------
# Render principal
# -----------------------------


def render():
    st.title("📈 KPIs Logística")
    start_iso, end_iso = sidebar_period_filters()
    operator_id, packers_count, stage = sidebar_entity_filters()

    st.divider()

    
    st.subheader(" :red[📊 Métricas]")


    # Dentro do render():
    col1, col2, col3 = st.columns(3)
    with col1:
        card_orders_per_hour(start_iso, end_iso, operator_id, packers_count, stage)
    with col2:
        card_avg_daily_total_time(start_iso, end_iso, operator_id, packers_count, stage)    
    with col3:
        card_avg_time_per_order(start_iso, end_iso, operator_id, packers_count, stage)


    st.divider()

    chart_orders_vs_time(start_iso, end_iso, operator_id, packers_count, stage)


    table_all_sessions()   
    table_all_stage_events()
       

