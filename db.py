"""SQLite data access layer for the Streamlit expedição app.

Tables
------
- operators(id, name, active, created_at)
- marketplaces(id, name, active, created_at)
- sessions(id, date, operator_id, marketplace_id, num_orders, created_at, updated_at)
- stage_events(id, session_id, stage, start_time, end_time)
  * UNIQUE(session_id, stage)

Notes
-----
- All timestamps are stored as local time in ISO format ("YYYY-MM-DD HH:MM:SS").
- Dates are stored as ISO date strings ("YYYY-MM-DD").
- Stages are constrained to: "Separação", "Conferência", "Embalagem", "Contagem de pacotes".
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, date
from typing import Dict, Iterable, List, Optional, Tuple

DB_PATH = "expedicao.db"

STAGES = ("Separação", "Conferência", "Embalagem", "Contagem de pacotes")


# -----------------------------
# Utilities
# -----------------------------

def iso_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def iso_date(d: Optional[date] = None) -> str:
    return (d or date.today()).isoformat()


@contextmanager
def get_conn(readonly: bool = False):
    """Context manager for SQLite connection with FK and Row factory enabled.

    Parameters
    ----------
    readonly: bool
        When True, open the database in read-only mode.
    """
    uri = f"file:{DB_PATH}?mode={'ro' if readonly else 'rwc'}"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        yield conn
        if not readonly:
            conn.commit()
    finally:
        conn.close()


# -----------------------------
# Schema management
# -----------------------------

def init_db() -> None:
    """Create tables and indexes if they do not exist."""
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS operators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS marketplaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                operator_id INTEGER NOT NULL,
                marketplace_id INTEGER NOT NULL,
                num_orders INTEGER NOT NULL CHECK (num_orders >= 0),   
                packers_count INTEGER NOT NULL DEFAULT 2,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                updated_at TEXT,
                FOREIGN KEY(operator_id) REFERENCES operators(id),
                FOREIGN KEY(marketplace_id) REFERENCES marketplaces(id)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                stage TEXT NOT NULL CHECK (stage IN ('Separação','Conferência','Embalagem', "Contagem de pacotes")),
                start_time TEXT,
                end_time TEXT,
                FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                UNIQUE(session_id, stage)
            );
            """
        )

        cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
        )


        # Helpful indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_date ON sessions(date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_operator ON sessions(operator_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_marketplace ON sessions(marketplace_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stage_events_session ON stage_events(session_id);")


# -----------------------------
# Operators
# -----------------------------

def add_operator(name: str) -> int:
    with get_conn() as conn:
        cur = conn.execute("INSERT OR IGNORE INTO operators(name) VALUES (?);", (name.strip(),))
        if cur.rowcount == 0:
            # Already exists; fetch id
            row = conn.execute("SELECT id FROM operators WHERE name = ?;", (name.strip(),)).fetchone()
            return int(row[0])
        return int(cur.lastrowid)


def remove_operator(identifier: int | str) -> None:
    """Inativa o operador (soft delete) ao invés de remover definitivamente."""
    with get_conn() as conn:
        if isinstance(identifier, int):
            conn.execute("UPDATE operators SET active = 0 WHERE id = ?;", (identifier,))
        else:
            conn.execute("UPDATE operators SET active = 0 WHERE name = ?;", (identifier,))



def list_operators(active_only: bool = True) -> List[sqlite3.Row]:
    with get_conn(readonly=True) as conn:
        if active_only:
            return conn.execute("SELECT id, name FROM operators WHERE active = 1 ORDER BY name;").fetchall()
        return conn.execute("SELECT id, name, active FROM operators ORDER BY name;").fetchall()


# -----------------------------
# Marketplaces
# -----------------------------

def add_marketplace(name: str) -> int:
    with get_conn() as conn:
        cur = conn.execute("INSERT OR IGNORE INTO marketplaces(name) VALUES (?);", (name.strip(),))
        if cur.rowcount == 0:
            row = conn.execute("SELECT id FROM marketplaces WHERE name = ?;", (name.strip(),)).fetchone()
            return int(row[0])
        return int(cur.lastrowid)


def remove_marketplace(identifier: int | str) -> None:
    """Inativa o marketplace (soft delete) ao invés de remover definitivamente."""
    with get_conn() as conn:
        if isinstance(identifier, int):
            conn.execute("UPDATE marketplaces SET active = 0 WHERE id = ?;", (identifier,))
        else:
            conn.execute("UPDATE marketplaces SET active = 0 WHERE name = ?;", (identifier,))



def list_marketplaces(active_only: bool = True) -> List[sqlite3.Row]:
    with get_conn(readonly=True) as conn:
        if active_only:
            return conn.execute("SELECT id, name FROM marketplaces WHERE active = 1 ORDER BY name;").fetchall()
        return conn.execute("SELECT id, name, active FROM marketplaces ORDER BY name;").fetchall()


# -----------------------------
# Sessions (registro do dia)
# -----------------------------

def create_session(
    operator_id: int,
    marketplace_id: int,
    session_date: Optional[date | str] = None,
    num_orders: int = 0,
    packers_count: int = 2,
) -> int:
    """Cria SEMPRE uma nova sessão (lote) e pré-cria as etapas."""
    if num_orders <= 0:
        raise ValueError("Quantidade de pedidos deve ser maior que zero para criar um novo lote.")
    if packers_count < 1:
        raise ValueError("Número de empacotadores deve ser pelo menos 1.")

    d = session_date if isinstance(session_date, str) else iso_date(session_date)
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO sessions(date, operator_id, marketplace_id, num_orders, packers_count, updated_at)
            VALUES (?,?,?,?,?,?);
            """,
            (d, operator_id, marketplace_id, int(num_orders), int(packers_count), iso_now()),
        )
        session_id = int(cur.lastrowid)
        for stage in STAGES:
            conn.execute("INSERT INTO stage_events(session_id, stage) VALUES (?,?);", (session_id, stage))
        return session_id



def list_sessions_today(operator_id: int, marketplace_id: int):
    """Lista sessões (lotes) do dia atual para o par operador+marketplace, mais recentes primeiro."""
    today = iso_date()
    with get_conn(readonly=True) as conn:
        return conn.execute(
            """
            SELECT s.id,
                   s.date,
                   s.num_orders,
                   s.created_at,
                   m.name AS marketplace
              FROM sessions s
              JOIN marketplaces m ON m.id = s.marketplace_id
             WHERE s.date = ?
               AND s.operator_id = ?
               AND s.marketplace_id = ?
             ORDER BY datetime(s.created_at) DESC, s.id DESC;
            """,
            (today, operator_id, marketplace_id),
        ).fetchall()



def update_session_orders(session_id: int, num_orders: int) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE sessions SET num_orders = ?, updated_at = ? WHERE id = ?;",
            (int(num_orders), iso_now(), session_id),
        )


def get_session(session_id: int) -> Optional[sqlite3.Row]:
    with get_conn(readonly=True) as conn:
        return conn.execute("SELECT * FROM sessions WHERE id = ?;", (session_id,)).fetchone()

def list_sessions_today_with_status(operator_id: int, marketplace_id: int):
    """Lista lotes de hoje para o par operador+marketplace com flag de finalizado."""
    today = iso_date()
    with get_conn(readonly=True) as conn:
        return conn.execute(
            """
            SELECT s.id,
                   s.date,
                   s.num_orders,
                   s.created_at,
                   m.name AS marketplace,
                   SUM(CASE WHEN e.start_time IS NOT NULL AND e.end_time IS NOT NULL THEN 1 ELSE 0 END) AS completed_stages,
                   COUNT(e.id) AS total_stages
              FROM sessions s
              JOIN marketplaces m ON m.id = s.marketplace_id
              JOIN stage_events e ON e.session_id = s.id
             WHERE s.date = ?
               AND s.operator_id = ?
               AND s.marketplace_id = ?
             GROUP BY s.id
             ORDER BY datetime(s.created_at) DESC, s.id DESC;
            """,
            (today, operator_id, marketplace_id),
        ).fetchall()



# -----------------------------
# Stage control
# -----------------------------

def _validate_stage(stage: str) -> str:
    if stage not in STAGES:
        raise ValueError(f"Invalid stage: {stage}. Must be one of {STAGES}")
    return stage


def start_stage(session_id: int, stage: str, when: Optional[datetime] = None) -> str:
    """Mark start time for a stage. If the row exists and is completed, it will restart the stage (overwriting).

    Returns the ISO timestamp used.
    """
    _validate_stage(stage)
    ts = (when or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        # Ensure row exists
        conn.execute(
            "INSERT OR IGNORE INTO stage_events(session_id, stage) VALUES (?,?);",
            (session_id, stage),
        )
        conn.execute(
            "UPDATE stage_events SET start_time = ?, end_time = NULL WHERE session_id = ? AND stage = ?;",
            (ts, session_id, stage),
        )
        conn.execute("UPDATE sessions SET updated_at = ? WHERE id = ?;", (iso_now(), session_id))
    return ts


def end_stage(session_id: int, stage: str, when: Optional[datetime] = None) -> str:
    """Mark end time for a stage. If start_time is missing, it will set start=end=now (zero duration).

    Returns the ISO timestamp used for end_time.
    """
    _validate_stage(stage)
    ts = (when or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        # Guarantee row
        conn.execute(
            "INSERT OR IGNORE INTO stage_events(session_id, stage) VALUES (?,?);",
            (session_id, stage),
        )
        # If no start_time, set it equal to end_time to avoid negative/NULL duration
        conn.execute(
            """
            UPDATE stage_events
               SET start_time = COALESCE(start_time, ?),
                   end_time   = ?
             WHERE session_id = ? AND stage = ?;
            """,
            (ts, ts, session_id, stage),
        )
        conn.execute("UPDATE sessions SET updated_at = ? WHERE id = ?;", (iso_now(), session_id))
    return ts


def get_stage_times(session_id: int) -> Dict[str, Dict[str, Optional[str]]]:
    """Return mapping: stage -> {start_time, end_time} as ISO strings (or None)."""
    with get_conn(readonly=True) as conn:
        rows = conn.execute(
            "SELECT stage, start_time, end_time FROM stage_events WHERE session_id = ?;",
            (session_id,),
        ).fetchall()
    result: Dict[str, Dict[str, Optional[str]]] = {}
    for r in rows:
        result[r["stage"]] = {"start_time": r["start_time"], "end_time": r["end_time"]}
    return result


def get_stage_durations_seconds(session_id: int) -> Dict[str, Optional[int]]:
    """Duration in seconds per stage (None if incomplete)."""
    with get_conn(readonly=True) as conn:
        rows = conn.execute(
            """
            SELECT stage,
                   CASE WHEN start_time IS NOT NULL AND end_time IS NOT NULL
                        THEN CAST(strftime('%s', end_time) AS INTEGER) - CAST(strftime('%s', start_time) AS INTEGER)
                        ELSE NULL END AS duration_sec
              FROM stage_events
             WHERE session_id = ?;
            """,
            (session_id,),
        ).fetchall()
    return {r["stage"]: (None if r["duration_sec"] is None else int(r["duration_sec"])) for r in rows}


# -----------------------------
# Analytics helpers for KPIs
# -----------------------------

#LOGICA PARA UNIR INTERVALOS DO DIA E FAZER AS METRICAS. 
def fetch_session_intervals(
    start: str,
    end: str,
    operator_id: int | None = None,
    packers_count: int | None = None,   # <- trocado de marketplace_id para packers_count
    stage: str | None = None,
):
    params = [start, end]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?"); params.append(operator_id)
    if packers_count:
        flt.append("s.packers_count = ?"); params.append(packers_count)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""
    # ... (restante da função permanece igual, usando where_extra)


    if stage is None:
        sql = f"""
            WITH per_session AS (
                SELECT s.id AS session_id,
                       s.date AS day,
                       MIN(e.start_time) AS start_ts,
                       MAX(e.end_time)   AS end_ts
                  FROM sessions s
                  JOIN stage_events e ON e.session_id = s.id
                 WHERE s.date BETWEEN ? AND ? {where_extra}
                 GROUP BY s.id, s.date
            )
            SELECT session_id, day, start_ts, end_ts
              FROM per_session
             WHERE start_ts IS NOT NULL AND end_ts IS NOT NULL
             ORDER BY day ASC, session_id ASC;
        """
        with get_conn(readonly=True) as conn:
            return conn.execute(sql, params).fetchall()
    else:
        sql = f"""
            WITH per_session AS (
                SELECT s.id AS session_id,
                       s.date AS day,
                       MIN(CASE WHEN e.stage = ? THEN e.start_time END) AS start_ts,
                       MAX(CASE WHEN e.stage = ? THEN e.end_time   END) AS end_ts
                  FROM sessions s
                  JOIN stage_events e ON e.session_id = s.id
                 WHERE s.date BETWEEN ? AND ? {where_extra}
                 GROUP BY s.id, s.date
            )
            SELECT session_id, day, start_ts, end_ts
              FROM per_session
             WHERE start_ts IS NOT NULL AND end_ts IS NOT NULL
             ORDER BY day ASC, session_id ASC;
        """
        with get_conn(readonly=True) as conn:
            return conn.execute(sql, [stage, stage] + params).fetchall()





def fetch_end_to_end_totals(start: str, end: str, operator_id: int | None = None, marketplace_id: int | None = None):
    """
    Retorna (total_seconds_end2end, total_orders) no intervalo [start, end],
    considerando cada sessão com start=min(start_time) e end=max(end_time) entre as etapas.
    Ignora sessões sem start OU end.
    """
    params = [start, end]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?")
        params.append(operator_id)
    if marketplace_id:
        flt.append("s.marketplace_id = ?")
        params.append(marketplace_id)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""

    sql = f"""
        WITH agg AS (
            SELECT s.id AS session_id,
                   s.num_orders AS num_orders,
                   MIN(e.start_time) AS start_any,
                   MAX(e.end_time)   AS end_any
              FROM sessions s
              JOIN stage_events e ON e.session_id = s.id
             WHERE s.date BETWEEN ? AND ? {where_extra}
             GROUP BY s.id
        )
        SELECT
            SUM(CASE WHEN start_any IS NOT NULL AND end_any IS NOT NULL
                     THEN CAST(strftime('%s', end_any) AS INTEGER) - CAST(strftime('%s', start_any) AS INTEGER)
                     ELSE 0 END) AS total_seconds,
            SUM(num_orders) AS total_orders
        FROM agg;
    """
    with get_conn(readonly=True) as conn:
        row = conn.execute(sql, params).fetchone()
        total_seconds = int(row["total_seconds"] or 0)
        total_orders = int(row["total_orders"] or 0)
        return total_seconds, total_orders


def fetch_daily_stage_durations(
    start: str, end: str, operator_id: Optional[int] = None, marketplace_id: Optional[int] = None
) -> List[sqlite3.Row]:
    """Aggregate duration (seconds) per day and stage within [start, end]."""
    params: List[object] = [start, end]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?")
        params.append(operator_id)
    if marketplace_id:
        flt.append("s.marketplace_id = ?")
        params.append(marketplace_id)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""

    sql = f"""
        SELECT s.date AS day,
               e.stage AS stage,
               SUM(CASE WHEN e.start_time IS NOT NULL AND e.end_time IS NOT NULL
                        THEN CAST(strftime('%s', e.end_time) AS INTEGER) - CAST(strftime('%s', e.start_time) AS INTEGER)
                        ELSE 0 END) AS duration_seconds
          FROM sessions s
          JOIN stage_events e ON e.session_id = s.id
         WHERE s.date BETWEEN ? AND ? {where_extra}
         GROUP BY s.date, e.stage
         ORDER BY s.date ASC;
    """
    with get_conn(readonly=True) as conn:
        return conn.execute(sql, params).fetchall()


def fetch_stage_totals_and_orders(
    start: str, end: str, operator_id: Optional[int] = None, marketplace_id: Optional[int] = None
) -> Dict[str, Dict[str, float]]:
    """Return totals and averages per stage within [start, end].

    Output format:
    {
      'Separação': { 'total_seconds': 1234.0, 'total_orders': 100.0, 'avg_seconds_per_order': 12.34 },
      ...
    }
    """
    params: List[object] = [start, end]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?")
        params.append(operator_id)
    if marketplace_id:
        flt.append("s.marketplace_id = ?")
        params.append(marketplace_id)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""

    sql = f"""
        SELECT e.stage AS stage,
               SUM(CASE WHEN e.start_time IS NOT NULL AND e.end_time IS NOT NULL
                        THEN CAST(strftime('%s', e.end_time) AS REAL) - CAST(strftime('%s', e.start_time) AS REAL)
                        ELSE 0 END) AS total_seconds,
               SUM(s.num_orders) AS total_orders
          FROM sessions s
          JOIN stage_events e ON e.session_id = s.id
         WHERE s.date BETWEEN ? AND ? {where_extra}
         GROUP BY e.stage;
    """
    out: Dict[str, Dict[str, float]] = {stage: {"total_seconds": 0.0, "total_orders": 0.0, "avg_seconds_per_order": 0.0} for stage in STAGES}
    with get_conn(readonly=True) as conn:
        for row in conn.execute(sql, params):
            stage = row["stage"]
            total_seconds = float(row["total_seconds"] or 0.0)
            total_orders = float(row["total_orders"] or 0.0)
            avg = (total_seconds / total_orders) if total_orders > 0 else 0.0
            out[stage] = {
                "total_seconds": total_seconds,
                "total_orders": total_orders,
                "avg_seconds_per_order": avg,
            }
    return out


#FUNCOES PARA USAR NA META
def set_setting(key: str, value: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
            (key, value),
        )

def get_setting(key: str, default: str | None = None) -> str | None:
    with get_conn(readonly=True) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?;", (key,)).fetchone()
        return row["value"] if row else default









#FUNCAO PARA USAR NA MEDIA DE TEMPO GASTO POR DIA
def fetch_daily_end_to_end(start: str, end: str, operator_id: int | None = None, packers_count: int | None = None, stage: str | None = None):
    params = [start, end]
    flt = []
    if operator_id:
        flt.append("s.operator_id = ?"); params.append(operator_id)
    if packers_count:
        flt.append("s.packers_count = ?"); params.append(packers_count)
    where_extra = (" AND " + " AND ".join(flt)) if flt else ""

    if stage is None:
        sql = f"""
            WITH per_session AS (
                SELECT s.id, s.date AS day,
                       MIN(e.start_time) AS start_any,
                       MAX(e.end_time)   AS end_any
                  FROM sessions s
                  JOIN stage_events e ON e.session_id = s.id
                 WHERE s.date BETWEEN ? AND ? {where_extra}
                 GROUP BY s.id, s.date
            )
            SELECT day,
                   SUM(CASE WHEN start_any IS NOT NULL AND end_any IS NOT NULL
                            THEN CAST(strftime('%s', end_any) AS INTEGER) - CAST(strftime('%s', start_any) AS INTEGER)
                            ELSE 0 END) AS total_seconds
              FROM per_session
             GROUP BY day
             ORDER BY day ASC;
        """
        with get_conn(readonly=True) as conn:
            return conn.execute(sql, params).fetchall()
    else:
        sql = f"""
            WITH per_session AS (
                SELECT s.id, s.date AS day,
                       MIN(CASE WHEN e.stage = ? THEN e.start_time END) AS stg_start,
                       MAX(CASE WHEN e.stage = ? THEN e.end_time   END) AS stg_end
                  FROM sessions s
                  JOIN stage_events e ON e.session_id = s.id
                 WHERE s.date BETWEEN ? AND ? {where_extra}
                 GROUP BY s.id, s.date
            )
            SELECT day,
                   SUM(CASE WHEN stg_start IS NOT NULL AND stg_end IS NOT NULL
                            THEN CAST(strftime('%s', stg_end) AS INTEGER) - CAST(strftime('%s', stg_start) AS INTEGER)
                            ELSE 0 END) AS total_seconds
              FROM per_session
             GROUP BY day
             ORDER BY day ASC;
        """
        with get_conn(readonly=True) as conn:
            return conn.execute(sql, [stage, stage] + params).fetchall()

#FUNCAO PARA DELETAR UM REGISTRO DO BD
def delete_session(session_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE id = ?;", (int(session_id),))



if __name__ == "__main__":
    # Quick manual bootstrap
    init_db()
    print("DB initialized at:", DB_PATH)
