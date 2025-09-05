########################################
# README.md — guia rápido (SP)
########################################
"""
# Packing Timer — SP (Streamlit)

Mede tempo por etapa do processo (Separação, Conferência, Embalagem) via botões **Iniciar**/**Terminar** por operador/estação. Datas e horas são automáticas pelo `datetime`.

## Rodar local
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## Como usar
1. No sidebar, preencha **Estação** e **Operador** (site fixo SP).
2. Na página **Registro**, clique **Iniciar** ao começar a etapa; **Terminar** ao finalizar.
   - Se iniciar outra etapa com uma aberta, a anterior é encerrada automaticamente (flag `auto_closed=1`).
3. A aba **Dashboard** mostra KPIs e o gráfico de tempo médio por etapa.

## Banco de dados
- Arquivo `metrics.db` (SQLite) na raiz do projeto.
- Tabela `sessions`: `etapa`, `site='SP'`, `estacao`, `operador`, `start_ts`, `end_ts`, `auto_closed`.

## Próximos passos
- Exportar CSV/Excel.
- PIN de operador.
- Regras de outliers (ex.: descartar > 12h).
"""
