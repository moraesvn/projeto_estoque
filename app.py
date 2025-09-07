"""Streamlit App – Expedição (Registro, KPIs, Configurações)

Estrutura de páginas:
- Registro: selecionar Marketplace, Operador, quantidade de pedidos; iniciar/encerrar etapas e registrar tempos.
- KPIs: filtros por mês ou intervalo; select boxes de Marketplace e Operador; métricas de tempo médio por pedido e gráfico por dia.
- Configurações: cadastro/remoção de Operadores e Marketplaces (usa config.render).
"""
from __future__ import annotations

import kpis as kpis_page
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, datetime

from db import (
    init_db,
    list_operators,
    list_marketplaces,
    create_session,
    list_sessions_today,
    update_session_orders,
    get_stage_times,
    start_stage,
    end_stage,
    fetch_daily_stage_durations,
    fetch_stage_totals_and_orders,
    list_sessions_today_with_status,
    get_session,
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
    st.title("📋 Registrar tempo de execução")
    st.caption("Selecione marketplace, operador e quantidade pedidos. Inicie e finalize cada etapa para registrar o tempo.")

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

    # ----- Controle de lote (sessão) -----
    today = date.today()

    # Botão para criar um novo lote
    c_new, c_pick = st.columns([1, 2])
    with c_new:
        if st.button("➕ Iniciar novo lote"):
            if num_orders <= 0:
                st.error("Informe a quantidade de pedidos antes de iniciar um novo lote.")
            else:
                new_id = create_session(op_id, mkt_id, today, int(num_orders))
                st.session_state["session_key"] = new_id    


    st.divider()

        # Seletor de lote ativo (apenas lotes não finalizados)
    sessions = list_sessions_today_with_status(op_id, mkt_id)

    def _is_done(r):
        return int(r["completed_stages"] or 0) >= int(r["total_stages"] or 0)

    # apenas não finalizados para o selectbox
    active_sessions = [r for r in sessions if not _is_done(r)]

    options, labels = [], {}
    for r in active_sessions:
        sid = int(r["id"])
        hhmm = r["created_at"].split(" ")[1] if r["created_at"] else "--:--:--"
        labels[sid] = f"{sid} • {r['marketplace']} • {r['num_orders']} pedidos • {hhmm}"
        options.append(sid)


    col_left, col_right = st.columns([1, 1])

    with col_left:
        if options:
            # tenta manter a seleção anterior, se ainda estiver ativa
            default_idx = 0
            if "session_key" in st.session_state and st.session_state["session_key"] in options:
                default_idx = options.index(st.session_state["session_key"])
            picked = st.selectbox("**Lotes ativos hoje do Marketplace selecionado**", options, index=default_idx, format_func=lambda x: labels[x])
            st.session_state["session_key"] = picked
        else:
            st.info("Nenhum lote ativo hoje para este Marketplace/Operador.")

    with col_right:
        # Lista auxiliar com TODOS os lotes do dia (finalizados em verde)
        st.markdown("**Todos os lotes do dia**")
        for r in sessions:
            sid = int(r["id"])
            hhmm = r["created_at"].split(" ")[1] if r["created_at"] else "--:--:--"
            base = f"{sid} • {r['marketplace']} • {r['num_orders']} pedidos • {hhmm}"
            st.markdown(f":green[{base} (lote finalizado)]" if _is_done(r) else base)



    # Garantir session_id selecionado
    session_id = st.session_state.get("session_key")
    if not session_id:
        return

    st.divider()


    # Tabela de etapas com botões
    times = get_stage_times(session_id)

    def _stage_status(times: dict, stage: str) -> tuple[str, str]:
        t = times.get(stage, {}) if times else {}
        start = t.get("start_time")
        end = t.get("end_time")
        if start and end:
            return "Concluído", "green"
        if start and not end:
            return "Em andamento", "blue"
        return "Pendente", "gray"

    #TEMPO AO CONCLUIR

    

    def _elapsed_label(times: dict, stage: str) -> str:
        """Retorna o tempo total da etapa se concluída, senão vazio."""
        t = times.get(stage, {}) if times else {}
        start, end = t.get("start_time"), t.get("end_time")
        if not (start and end):
            return ""
        fmt = "%Y-%m-%d %H:%M:%S"
        start_dt = datetime.strptime(start, fmt)
        end_dt = datetime.strptime(end, fmt)
        delta = end_dt - start_dt
        total_sec = int(delta.total_seconds())
        h = total_sec // 3600
        m = (total_sec % 3600) // 60
        s = total_sec % 60
        return f"{h:02d}:{m:02d}:{s:02d}"



    # Buscar dados reais do lote selecionad

    sess = get_session(int(session_id))
    num_orders_hdr = sess["num_orders"] if sess else 0
    hora_criacao = sess["created_at"].split(" ")[1] if (sess and sess["created_at"]) else "--:--:--"

    st.markdown(f"##### :red[Lote {session_id} • {mkt_name} • {num_orders_hdr} pedidos • criado às {hora_criacao}]")


    # Definir os pares para exibir lado a lado
    pairs = [("Separação", "Conferência"), ("Embalagem", "Contagem de pacotes")]

    for left_stage, right_stage in pairs:
        col1, col2 = st.columns(2)

        for stage, col in [(left_stage, col1), (right_stage, col2)]:
            with col:
                status_label, status_color = _stage_status(times, stage)
                st.markdown(f"### {stage} · :{status_color}[{status_label}]")


                # Linha do Início
                c1, c2 = st.columns([1, 3])
                with c1:
                    if st.button("▶️ Iniciar", key=f"start_{stage}"):
                        start_stage(session_id, stage)
                        st.rerun()
                with c2:
                    st.write(f"Início: **{_parse_iso(times.get(stage, {}).get('start_time'))}**")
                    

                
                # Linha do Fim
                c3, c4 = st.columns([1, 3])
                with c3:
                    if st.button("⏹️ Encerrar", key=f"end_{stage}"):
                        end_stage(session_id, stage)
                        st.rerun()
                with c4:
                    st.write(f"Fim: **{_parse_iso(times.get(stage, {}).get('end_time'))}**")
                    # Tempo total
                    # Cronômetro / duração
                    elapsed_txt = _elapsed_label(times, stage)
                    if elapsed_txt:
                        st.write(f"⏱️ Tempo total: {elapsed_txt}")



        st.divider()
        


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
        st.image("logo.png")
        st.title("📦 Logística")
        page = st.radio("", ("Registro", "KPIs", "Configurações"), label_visibility="collapsed")
    

    if page == "Registro":
        page_registro()
    elif page == "KPIs":
        kpis_page.render()
    else:
        page_config()


if __name__ == "__main__":
    main()
