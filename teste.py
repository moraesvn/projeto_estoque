
#excluido

'''
# Criar/obter sessão do dia
    today = date.today()
    if ("session_key" not in st.session_state) or (
        st.session_state.get("_op_id") != op_id
        or st.session_state.get("_mkt_id") != mkt_id
        or st.session_state.get("_date") != today.isoformat()
    ):
        session_id = create_session(op_id, mkt_id, today, num_orders)
        st.session_state["session_key"] = session_id
        st.session_state["_op_id"] = op_id
        st.session_state["_mkt_id"] = mkt_id
        st.session_state["_date"] = today.isoformat()
    else:
        session_id = st.session_state["session_key"]
        # Atualiza pedidos quando usuário alterar
        update_session_orders(session_id, int(num_orders))

    st.info(f"Sessão {session_id} — Data: {today.strftime('%d/%m/%Y')} — Operador: {op_name} — Marketplace: {mkt_name}")
'''



#excluido lista de lotes de hoje
'''
 sessions = list_sessions_today(op_id, mkt_id)
    options = []
    labels = {}
    for r in sessions:
        sid = int(r["id"])
        # label amigável: #id - pedidos X - criado HH:MM
        hhmm = r["created_at"].split(" ")[1] if r["created_at"] else "--:--:--"
        label = f"{sid} • {r['marketplace']} • {r['num_orders']} pedidos • {hhmm}"
        options.append(sid)
        labels[sid] = label

    with c_pick:
        if options:
            default_idx = 0
            if "session_key" in st.session_state and st.session_state["session_key"] in options:
                default_idx = options.index(st.session_state["session_key"])
            picked = st.selectbox("Lotes de hoje", options, index=default_idx, format_func=lambda x: labels[x])
            st.session_state["session_key"] = picked
        else:
            st.info("Nenhum lote hoje. Clique em “Iniciar novo lote”.")


'''

'''
LAYOUT EXCLUIDO LINHAS DAS ETAPAS 

'''


