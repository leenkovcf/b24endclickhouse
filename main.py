import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional

import clickhouse_connect
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONFIG_FILE  = "config.json"
HISTORY_FILE = "history.json"
HISTORY_LIMIT = 1000
app = FastAPI(title="b24endclickhouse")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

scheduler = BackgroundScheduler(timezone="Europe/Moscow")
export_status = {"running": False, "rows": 0, "error": None, "last_run": None}
connection_status = {"bitrix": None, "clickhouse": None}

# ---------------------------------------------------------------------------
# Entity catalogue
# ---------------------------------------------------------------------------
ENTITIES = {
    "CRM": [
        {"code": "crm_lead",               "name": "Лиды",                             "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_lead_uf",            "name": "Пользовательские поля лидов",      "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_lead_status_history","name": "История статусов лидов",           "date_fields": []},
        {"code": "crm_lead_product_row",   "name": "Товары в лидах",                   "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_deal",               "name": "Сделки",                           "date_fields": ["CLOSEDATE", "DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_deal_uf",            "name": "Пользовательские поля сделок",     "date_fields": ["CLOSEDATE", "DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_deal_stage_history", "name": "История статусов сделок",          "date_fields": []},
        {"code": "crm_deal_product_row",   "name": "Товары в сделках",                 "date_fields": ["DEAL_CLOSEDATE", "DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_company",            "name": "Компании",                         "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_company_uf",         "name": "Пользовательские поля компаний",   "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_contact",            "name": "Контакты",                         "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_contact_uf",         "name": "Пользовательские поля контактов",  "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
    ],
    "Товары из каталога": [
        {"code": "crm_product",               "name": "Товары",                       "date_fields": []},
        {"code": "crm_product_property",      "name": "Свойства товаров",             "date_fields": []},
        {"code": "crm_product_property_value","name": "Значения свойств товаров",     "date_fields": []},
    ],
    "Дела, стадии и связи": [
        {"code": "crm_activity",          "name": "Дела в элементах CRM",            "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_entity_stage",      "name": "Стадии элементов CRM",            "date_fields": []},
        {"code": "crm_activity_relation", "name": "Связи дел с элементами CRM",      "date_fields": []},
        {"code": "crm_entity_relation",   "name": "Связи между элементами CRM",      "date_fields": []},
    ],
    "Задачи и проекты": [
        {"code": "task",              "name": "Задачи",                           "date_fields": ["CREATED_DATE", "CHANGED_DATE", "DEADLINE", "CLOSED_DATE"]},
        {"code": "task_uf",           "name": "Пользовательские поля задач",      "date_fields": []},
        {"code": "task_elapsed_item", "name": "Время работы над задачей",         "date_fields": []},
        {"code": "task_stage",        "name": "Стадии задач",                     "date_fields": []},
        {"code": "task_result",       "name": "Эффективность задач",              "date_fields": []},
        {"code": "task_flow",         "name": "Потоки задач",                     "date_fields": []},
        {"code": "socialnetwork_group","name": "Проекты",                         "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
    ],
    "Пользователи, структура и звонки": [
        {"code": "user",             "name": "Пользователи",       "date_fields": []},
        {"code": "org_structure",    "name": "Структура компании",  "date_fields": []},
        {"code": "org_department",   "name": "Иерархия отделов",    "date_fields": []},
        {"code": "telephony_call",   "name": "Звонки",              "date_fields": ["CALL_START_DATE"]},
    ],
    "Бизнес-процессы": [
        {"code": "bizproc_task",           "name": "Задания бизнес-процессов",    "date_fields": []},
        {"code": "bizproc_workflow_state", "name": "Запущенные бизнес-процессы", "date_fields": []},
    ],
    "CoPilot": [
        {"code": "ai_call_script_result", "name": "Оценки разговоров по скриптам продаж", "date_fields": []},
        {"code": "ai_call_script",        "name": "Скрипты продаж AI",                    "date_fields": []},
    ],
    "Подпись и КЭДО": [
        {"code": "sign_document", "name": "Подпись",                                           "date_fields": ["DATE_CREATE"]},
        {"code": "hr_timesheet",  "name": "Кадровый электронный документооборот (КЭДО)",       "date_fields": []},
    ],
    "Складской учёт": [
        {"code": "catalog_store",                 "name": "Список складов",                          "date_fields": []},
        {"code": "catalog_store_product",         "name": "Остатки товаров на складах",              "date_fields": []},
        {"code": "catalog_store_document",        "name": "Складские документы",                     "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "catalog_store_document_element","name": "Список товаров в складских документах",   "date_fields": []},
        {"code": "sale_shipment",                 "name": "Документы реализации",                    "date_fields": ["DATE_INSERT", "DATE_UPDATE"]},
        {"code": "sale_shipment_item",            "name": "Состав документа реализации",             "date_fields": []},
    ],
}

DATE_FIELD_LABELS = {
    "DATE_CREATE":    "Дата создания",
    "DATE_MODIFY":    "Дата изменения",
    "CLOSEDATE":      "Дата закрытия",
    "DEAL_CLOSEDATE": "Дата закрытия сделки",
    "CREATED_DATE":   "Дата создания",
    "CHANGED_DATE":   "Дата изменения",
    "DEADLINE":       "Дедлайн",
    "CLOSED_DATE":    "Дата закрытия",
    "CALL_START_DATE":"Дата звонка",
    "DATE_INSERT":    "Дата создания",
    "DATE_UPDATE":    "Дата обновления",
}

SCHEDULE_LABELS = {
    "daily":   "Ежедневно",
    "weekly":  "Еженедельно (пн)",
    "monthly": "Ежемесячно (1-го)",
}

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        # Non-destructive in-memory migration. Disk is NOT touched until explicit save.
        cfg.setdefault("ozon", {})
        if not isinstance(cfg["ozon"].get("accounts"), list):
            legacy = cfg.get("ozon", {})
            if legacy.get("client_id") or legacy.get("api_key"):
                cfg["ozon"] = {"accounts": [{
                    "name":           legacy.get("name") or "Магазин",
                    "client_id":      legacy.get("client_id", ""),
                    "api_key":        legacy.get("api_key", ""),
                    "perf_client_id": legacy.get("perf_client_id", ""),
                    "perf_secret":    legacy.get("perf_secret", ""),
                }]}
            else:
                cfg["ozon"] = {"accounts": []}
        cfg.setdefault("saved_configs", [])
        return cfg
    return {
        "bitrix":     {"portal": "", "bi_key": ""},
        "ozon":       {"accounts": []},
        "clickhouse": {"host": "", "port": 8443, "database": "default", "username": "admin", "password": ""},
        "schedule":   {"enabled": False, "frequency": "daily", "time_msk": "00:01", "configs": [], "days_back": 1},
        "saved_configs": [],
    }

def save_config(config: dict) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

# ---------------------------------------------------------------------------
# Export history
# ---------------------------------------------------------------------------
def _entity_display_name(code: str) -> str:
    if not code:
        return ""
    for items in ENTITIES.values():
        for e in items:
            if e["code"] == code:
                return e["name"]
    # OZON_ENTITIES is defined later in the file; access via globals() to avoid
    # forward-reference issues at import time.
    ozon_cat = globals().get("OZON_ENTITIES")
    if isinstance(ozon_cat, dict):
        for items in ozon_cat.values():
            for e in items:
                if e["code"] == code:
                    return e["name"]
    try:
        for ce in load_config().get("custom_entities", []):
            if ce.get("code") == code:
                return ce.get("name") or code
    except Exception:
        pass
    return code

def _load_history() -> list:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_history(items: list) -> None:
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(items[:HISTORY_LIMIT], f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("history save failed: %s", e)

def _record_history(entry: dict) -> None:
    items = _load_history()
    next_id = (items[0]["id"] + 1) if items and isinstance(items[0].get("id"), int) else 1
    entry = dict(entry)
    entry["id"] = next_id
    entry.setdefault("provider", "bitrix")
    entry.setdefault("entity_name", _entity_display_name(entry.get("entity", "")))
    items.insert(0, entry)
    _save_history(items)

# ===========================================================================
# ============================  BITRIX24 PROVIDER  ==========================
# ===========================================================================
# Everything below until the OZON PROVIDER marker is Bitrix-specific.
# When adding multi-provider features, prefer to add new endpoints/functions
# alongside without touching the existing ones (backward compatibility).
# ---------------------------------------------------------------------------
# Bitrix24 helpers
# ---------------------------------------------------------------------------
def _normalize_portal(portal: str) -> str:
    portal = portal.strip().rstrip("/")
    if not portal.startswith("http"):
        portal = f"https://{portal}"
    return portal

def fetch_from_bitrix(portal: str, bi_key: str, table: str,
                      date_field: Optional[str] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      dimensions_filters: Optional[list] = None,
                      fields: Optional[list] = None,
                      limit: Optional[int] = None) -> list:
    url = f"{_normalize_portal(portal)}/bitrix/tools/biconnector/pbi.php"
    payload: dict = {"key": bi_key}
    if date_field and start_date and end_date:
        payload["dateRange"]    = {"startDate": start_date, "endDate": end_date}
        payload["configParams"] = {"timeFilterColumn": date_field}
    if dimensions_filters:
        payload["dimensionsFilters"] = [[f] for f in dimensions_filters]
    if fields:
        payload["fields"] = [{"name": f} for f in fields]
    if limit:
        payload["limit"] = limit
    logger.info("BI request table=%s filters=%s fields=%s",
                table, json.dumps(payload.get("dimensionsFilters")), fields)
    resp = requests.post(url, params={"table": table}, json=payload, timeout=300)
    resp.raise_for_status()
    raw = resp.json()
    if isinstance(raw, dict):
        logger.error("BI connector error: %s", raw)
        msg = raw.get("errorDescription") or raw.get("error_description") or raw.get("error") or str(raw)
        raise Exception(f"BI connector ошибка: {msg}")
    return raw

_UNRECOGNIZED_COLUMN_RE = re.compile(r"Unrecognized column '([^']+)'", re.IGNORECASE)

def fetch_from_bitrix_safe(portal: str, bi_key: str, table: str,
                           date_field: Optional[str], start_date: Optional[str],
                           end_date: Optional[str], dimensions_filters,
                           fields, limit: Optional[int] = None,
                           max_retries: int = 30):
    """
    Like fetch_from_bitrix, but if BI returns 'Unrecognized column X' it removes
    X from fields and retries (up to max_retries). Returns (raw, removed_fields).
    """
    cur_fields = list(fields) if fields else None
    removed: list = []
    for _ in range(max_retries):
        try:
            raw = fetch_from_bitrix(portal, bi_key, table, date_field,
                                    start_date, end_date,
                                    dimensions_filters, cur_fields, limit)
            return raw, removed
        except Exception as exc:
            m = _UNRECOGNIZED_COLUMN_RE.search(str(exc))
            if not m or not cur_fields:
                raise
            bad = m.group(1)
            if bad not in cur_fields:
                raise
            cur_fields.remove(bad)
            removed.append(bad)
            logger.warning("Removed missing column %s from %s and retrying", bad, table)
    raise Exception(f"Не удалось загрузить {table}: BI не принимает поля даже после удаления {removed}")

def _rest(webhook: str, method: str, params: dict = None) -> dict:
    """Call Bitrix24 REST API via webhook."""
    url = f"{webhook.rstrip('/')}/{method}"
    resp = requests.get(url, params=params or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()

def _rest_post(webhook: str, method: str, data: dict = None) -> dict:
    """Call Bitrix24 REST API via POST (used for batch)."""
    url = f"{webhook.rstrip('/')}/{method}"
    resp = requests.post(url, json=data or {}, timeout=60)
    resp.raise_for_status()
    return resp.json()

def _uf_labels_via_batch(webhook: str, list_method: str) -> dict:
    """Fetch UF field labels using batch API: list to get IDs, then get each label."""
    get_method = list_method.replace(".list", ".get")

    # Step 1: collect all (ID, FIELD_NAME) pairs with pagination
    start = 0
    all_fields: list = []
    while True:
        data  = _rest(webhook, list_method, {"start": start})
        batch = data.get("result", [])
        all_fields.extend([(uf["ID"], uf.get("FIELD_NAME", "")) for uf in batch])
        next_start = data.get("next")
        if not next_start or not batch:
            break
        start = next_start

    if not all_fields:
        return {}

    # Step 2: batch-fetch labels (max 50 per Bitrix24 batch request)
    labels: dict = {}
    for i in range(0, len(all_fields), 50):
        chunk    = all_fields[i:i + 50]
        commands = {f"f{j}": f"{get_method}?id={fid}" for j, (fid, _) in enumerate(chunk)}
        try:
            resp    = _rest_post(webhook, "batch", {"halt": 0, "cmd": commands})
            results = resp.get("result", {}).get("result", {})
            for j, (_, fname) in enumerate(chunk):
                field_data = results.get(f"f{j}") or {}
                label = _label_from_uf(field_data)
                if fname and label and label != fname:
                    labels[fname] = label
        except Exception as be:
            logger.warning("batch uf labels chunk %d: %s", i, be)

    return labels

# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------
def get_ch_client(config: dict):
    ch   = config["clickhouse"]
    port = int(ch.get("port", 8443))
    return clickhouse_connect.get_client(
        host=ch["host"].strip(),
        port=port,
        database=ch.get("database", "default"),
        username=ch.get("username", "default"),
        password=ch.get("password", ""),
        secure=port in (8443, 9440),
        verify=False,
    )

_DATE_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DT_PAT   = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")

def _infer_type(values: list) -> str:
    non_null = [v for v in values if v is not None and str(v).strip() != ""]
    if not non_null:
        return "Nullable(String)"
    sample = [str(v) for v in non_null[:30]]
    if all(_DT_PAT.match(s) for s in sample):
        return "Nullable(DateTime)"
    if all(_DATE_PAT.match(s) for s in sample):
        return "Nullable(Date)"
    try:
        [int(s) for s in sample]
        return "Nullable(Int64)"
    except ValueError:
        pass
    try:
        [float(s) for s in sample]
        return "Nullable(Float64)"
    except ValueError:
        pass
    return "Nullable(String)"

def _convert(value, ch_type: str):
    if value is None or str(value).strip() == "":
        return None
    try:
        if "DateTime" in ch_type:
            return datetime.fromisoformat(str(value).replace("T", " ")[:19])
        if "Date" in ch_type:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        if "Int64" in ch_type:
            return int(value)
        if "Float64" in ch_type:
            return float(value)
        return str(value)
    except Exception:
        return None if "String" not in ch_type else str(value)

def _table_col_types(client, table_name: str) -> dict:
    """Read actual column types from an existing ClickHouse table."""
    try:
        rows = client.query(f"DESCRIBE TABLE `{table_name}`").result_rows
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}

# Сущности, у которых API не поддерживает фильтр по датам и мы тянем
# полный снимок. Чтобы не плодить дубли при повторных выгрузках —
# перед INSERT удаляем строки по id из текущей выборки (upsert).
_SNAPSHOT_DEDUPE_BY: dict = {
    "ozon_returns_fbs": "id",
    "ozon_returns_fbo": "id",
    "ozon_product":     "product_id",
}

# Принудительные типы колонок для отдельных сущностей.
# Если таблица уже создана с другим типом — выполним ALTER MODIFY COLUMN.
_COLUMN_TYPE_OVERRIDES: dict = {
    "ozon_perf_statistics": {"date": "Nullable(Date)"},
}

def push_to_clickhouse(config: dict, table_name: str, raw: list) -> int:
    if len(raw) < 2:
        return 0

    headers  = raw[0]
    rows     = raw[1:]
    sample_n = min(200, len(rows))
    inferred = [
        _infer_type([rows[j][i] if i < len(rows[j]) else None for j in range(sample_n)])
        for i in range(len(headers))
    ]

    safe   = re.sub(r"[^\w]", "_", table_name)
    client = get_ch_client(config)
    cols_sql = ",\n  ".join(f"`{h}` {t}" for h, t in zip(headers, inferred))
    client.command(f"""
        CREATE TABLE IF NOT EXISTS `{safe}` (
          {cols_sql}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
    """)

    # Auto-add columns that exist in incoming data but not yet in the table.
    # Happens when a saved config has a Bitrix field that was added after the
    # ClickHouse table was first created.
    existing = _table_col_types(client, safe)
    for h, t in zip(headers, inferred):
        if h not in existing:
            try:
                client.command(f"ALTER TABLE `{safe}` ADD COLUMN IF NOT EXISTS `{h}` {t}")
                existing[h] = t
                logger.info("Added column %s (%s) to %s", h, t, safe)
            except Exception as alter_exc:
                logger.warning("Failed to ALTER %s ADD %s: %s", safe, h, alter_exc)

    # Принудительные типы по сущности — апдейтим существующие колонки,
    # если в таблице сохранился неправильный тип с прошлых выгрузок.
    overrides = _COLUMN_TYPE_OVERRIDES.get(table_name) or {}
    for col, desired in overrides.items():
        cur = existing.get(col)
        if cur and cur != desired:
            try:
                client.command(f"ALTER TABLE `{safe}` MODIFY COLUMN `{col}` {desired}")
                existing[col] = desired
                logger.info("Modified column %s on %s: %s -> %s", col, safe, cur, desired)
            except Exception as alter_exc:
                logger.warning("Failed to ALTER %s MODIFY %s: %s", safe, col, alter_exc)

    # Use the actual table schema (not freshly inferred) so type conversions
    # stay consistent across days when column values change character (e.g.
    # CRM_PRODUCT_ID going from single int → comma-separated string).
    col_types = [existing.get(h, inferred[i]) for i, h in enumerate(headers)]

    converted = []
    for r in rows:
        padded = list(r) + [None] * (len(headers) - len(r))
        converted.append([_convert(v, t) for v, t in zip(padded, col_types)])

    # Upsert для snapshot-сущностей: DELETE по id-колонке, потом INSERT.
    dedupe_col = _SNAPSHOT_DEDUPE_BY.get(table_name)
    if dedupe_col and dedupe_col in headers:
        idx = headers.index(dedupe_col)
        ids = [r[idx] for r in converted if r[idx] is not None and r[idx] != ""]
        if ids:
            # квотируем как строки — универсально для Int/String id
            id_list = ",".join("'" + str(v).replace("'", "''") + "'" for v in ids)
            try:
                client.command(
                    f"ALTER TABLE `{safe}` DELETE "
                    f"WHERE toString(`{dedupe_col}`) IN ({id_list}) "
                    f"SETTINGS mutations_sync = 2"
                )
                logger.info("Dedup %s: deleted by %s (%d ids)", safe, dedupe_col, len(ids))
            except Exception as del_exc:
                logger.warning("Dedup DELETE failed on %s: %s", safe, del_exc)

    client.insert(safe, converted, column_names=list(headers))
    return len(rows)

# ---------------------------------------------------------------------------
# Background export
# ---------------------------------------------------------------------------
def _do_export(data: dict, source: str = "manual_form",
               config_name: Optional[str] = None,
               provider: str = "bitrix") -> None:
    global export_status
    export_status = {"running": True, "rows": 0, "error": None, "last_run": None}
    config  = load_config()
    started = datetime.now()
    rows    = 0
    error   = None
    try:
        if provider == "ozon":
            account_name = data.get("ozon_account") or ""
            account = _ozon_account_by_name(config, account_name)
            if not account:
                raise Exception(f"Аккаунт Ozon '{account_name}' не найден")
            raw = _ozon_fetch_dispatch(account, data["entity"],
                                       data.get("start_date") or "",
                                       data.get("end_date") or "",
                                       data.get("fields") or None)
        else:
            raw, _removed = fetch_from_bitrix_safe(
                config["bitrix"]["portal"],
                config["bitrix"]["bi_key"],
                data["entity"],
                data.get("date_field")         or None,
                data.get("start_date")         or None,
                data.get("end_date")           or None,
                data.get("dimensions_filters") or None,
                data.get("fields")             or None,
            )
        rows = push_to_clickhouse(config, data["entity"], raw)
        export_status = {"running": False, "rows": rows, "error": None,
                         "last_run": datetime.now().strftime("%d.%m.%Y %H:%M")}
        logger.info("Export %s — %d rows", data["entity"], rows)
    except Exception as exc:
        error = str(exc)
        export_status = {"running": False, "rows": 0, "error": error,
                         "last_run": datetime.now().strftime("%d.%m.%Y %H:%M")}
        logger.error("Export failed: %s", exc)
    finally:
        finished = datetime.now()
        _record_history({
            "started_at":         started.isoformat(timespec="seconds"),
            "finished_at":        finished.isoformat(timespec="seconds"),
            "duration_sec":       int((finished - started).total_seconds()),
            "provider":           provider,
            "source":             source,
            "config_name":        config_name,
            "entity":             data.get("entity", ""),
            "date_field":         data.get("date_field") or "",
            "start_date":         data.get("start_date") or "",
            "end_date":           data.get("end_date") or "",
            "dimensions_filters": data.get("dimensions_filters") or [],
            "fields":             data.get("fields") or [],
            "rows":               rows,
            "status":             "error" if error else "ok",
            "error":              error,
        })

def _run_scheduled() -> None:
    config = load_config()
    sch    = config.get("schedule", {})
    days   = int(sch.get("days_back", 1))
    start  = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    end    = datetime.now().strftime("%Y-%m-%d")

    config_names = sch.get("configs") or []
    saved_map    = {c["name"]: c for c in config.get("saved_configs", [])
                    if isinstance(c, dict) and c.get("name")}

    if config_names:
        for name in config_names:
            sc = saved_map.get(name)
            if not sc:
                logger.warning("Scheduled config %s not found, skipping", name)
                continue
            logger.info("Scheduled run: %s (entity=%s)", name, sc.get("entity"))
            _do_export({
                "entity":             sc.get("entity", ""),
                "date_field":         sc.get("date_field") or "DATE_CREATE",
                "start_date":         start,
                "end_date":           end,
                "dimensions_filters": sc.get("dimensions_filters") or None,
                "fields":             sc.get("fields") or None,
                "ozon_account":       sc.get("ozon_account") or "",
            }, source="schedule", config_name=name,
               provider=sc.get("provider") or "bitrix")
        return

    # Backward compatibility: old single-entity schedule
    if sch.get("entity"):
        _do_export({
            "entity":             sch.get("entity", ""),
            "date_field":         sch.get("date_field", "DATE_CREATE"),
            "start_date":         start,
            "end_date":           end,
            "dimensions_filters": sch.get("dimensions_filters") or None,
            "fields":             sch.get("fields") or None,
        }, source="schedule", provider="bitrix")

def _apply_schedule(config: dict) -> None:
    if scheduler.get_job("export_job"):
        scheduler.remove_job("export_job")
    sch = config.get("schedule", {})
    if not sch.get("enabled"):
        return
    time_str = sch.get("time_msk", "00:01")
    try:
        h, m = [int(x) for x in time_str.split(":")]
    except Exception:
        h, m = 0, 1
    freq = sch.get("frequency", "daily")
    triggers = {
        "daily":   CronTrigger(hour=h, minute=m),
        "weekly":  CronTrigger(day_of_week="mon", hour=h, minute=m),
        "monthly": CronTrigger(day=1, hour=h, minute=m),
    }
    trigger = triggers.get(freq)
    if trigger:
        scheduler.add_job(_run_scheduled, trigger, id="export_job", replace_existing=True)
        logger.info("Schedule set: %s at %s MSK", freq, time_str)

# ---------------------------------------------------------------------------
# FastAPI routes
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/entities")
async def api_entities():
    return ENTITIES

@app.get("/api/date-field-labels")
async def api_date_labels():
    return DATE_FIELD_LABELS

@app.get("/api/settings")
async def api_get_settings():
    return load_config()

@app.post("/api/settings")
async def api_save_settings(data: dict):
    config = load_config()
    if "bitrix" in data:
        config["bitrix"].update(data["bitrix"])
    # Note: Ozon accounts are managed via /api/ozon/accounts (multi-account UI).
    # We intentionally don't accept ozon credentials through generic settings save.
    if "clickhouse" in data:
        config["clickhouse"].update(data["clickhouse"])
    if "custom_entities" in data:
        config["custom_entities"] = data["custom_entities"]
    save_config(config)
    return {"status": "ok"}

@app.get("/api/smart-processes")
async def api_smart_processes():
    config = load_config()
    webhook = config["bitrix"].get("rest_webhook", "").strip().rstrip("/")
    if not webhook:
        raise HTTPException(400, "Укажите REST API Webhook в настройках подключения")
    try:
        url  = f"{webhook}/crm.type.list"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        types = data.get("result", {}).get("types", [])
        result = []
        for t in types:
            eid  = t.get("entityTypeId")
            name = t.get("title", f"СП #{eid}")
            if eid:
                result.append({"code": f"crm_dynamic_items_{eid}", "name": name})
                result.append({"code": f"crm_dynamic_items_{eid}_product_row", "name": f"Товары в СП: {name}"})
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, str(exc))

@app.post("/api/test-connection")
async def api_test_connection(data: dict):
    global connection_status
    config = load_config()
    result: dict = {}

    if data.get("test_bitrix", True):
        portal = config["bitrix"].get("portal", "").strip()
        bi_key = config["bitrix"].get("bi_key", "").strip()
        if not portal or not bi_key:
            result["bitrix"] = False
            result["bitrix_error"] = "Заполните адрес портала и BI-ключ в настройках"
        else:
            try:
                today = datetime.now().strftime("%Y-%m-%d")
                fetch_from_bitrix(portal, bi_key, "crm_lead", "DATE_CREATE", today, today)
                result["bitrix"] = True
            except Exception as exc:
                result["bitrix"] = False
                result["bitrix_error"] = str(exc)

    if data.get("test_clickhouse", True):
        host = config["clickhouse"].get("host", "").strip()
        if not host:
            result["clickhouse"] = False
            result["clickhouse_error"] = "Заполните хост ClickHouse в настройках"
        else:
            try:
                get_ch_client(config).ping()
                result["clickhouse"] = True
            except Exception as exc:
                result["clickhouse"] = False
                result["clickhouse_error"] = str(exc)

    connection_status.update({k: v for k, v in result.items() if isinstance(v, bool)})
    return result

@app.get("/api/connection-status")
async def api_connection_status():
    return connection_status

@app.get("/api/export-status")
async def api_export_status():
    return export_status

@app.post("/api/export")
async def api_export(data: dict, background_tasks: BackgroundTasks):
    if export_status["running"]:
        raise HTTPException(400, "Выгрузка уже выполняется")
    background_tasks.add_task(_do_export, data)
    return {"status": "started"}

@app.get("/api/entity-fields")
async def api_entity_fields(entity: str):
    """Fetch available field names for an entity via BI connector (limit=1 for speed)."""
    config = load_config()
    portal = config["bitrix"].get("portal", "").strip()
    bi_key = config["bitrix"].get("bi_key", "").strip()
    if not portal or not bi_key:
        raise HTTPException(400, "Настройте подключение к Bitrix24")
    try:
        raw = await asyncio.to_thread(fetch_from_bitrix, portal, bi_key, entity,
                                      None, None, None, None, None, 1)
        if raw and isinstance(raw[0], list):
            return {"fields": raw[0]}
        return {"fields": []}
    except Exception as exc:
        raise HTTPException(500, str(exc))

# Entities derived from crm_deal that share its funnel/stage structure
_DEAL_VARIANTS = {"crm_deal", "crm_deal_uf", "crm_deal_stage_history", "crm_deal_product_row"}

# These BI tables don't expose CATEGORY_ID/STAGE_ID as filter dimensions →
# must pivot via crm_deal first to get matching DEAL_IDs
_UF_DEAL_VARIANTS = {"crm_deal_uf", "crm_deal_stage_history"}

def _needs_deal_id_pivot(entity: str, filters) -> bool:
    if entity not in _UF_DEAL_VARIANTS or not filters:
        return False
    return any(f.get("fieldName") in ("CATEGORY_ID", "STAGE_ID") for f in filters)

@app.get("/api/crm-funnels")
async def api_crm_funnels(entity: str = "crm_deal"):
    """Return pipeline categories for deals or smart processes."""
    config = load_config()
    webhook = config["bitrix"].get("rest_webhook", "").strip().rstrip("/")
    if not webhook:
        return []
    try:
        if entity in _DEAL_VARIANTS:
            data = _rest(webhook, "crm.category.list", {"entityTypeId": 2})
            cats = data.get("result", {}).get("categories", [])
        elif entity.startswith("crm_dynamic_items_"):
            eid  = entity.replace("crm_dynamic_items_", "").split("_")[0]
            data = _rest(webhook, "crm.category.list", {"entityTypeId": eid})
            cats = data.get("result", {}).get("categories", [])
        else:
            return []
        return [{"id": str(c["id"]), "name": c["name"]} for c in cats]
    except Exception as exc:
        logger.warning("crm-funnels: %s", exc)
        return []

_ENTITY_FIELDS_REST: dict = {
    "crm_deal":                "crm.deal.fields",
    "crm_deal_uf":             "crm.deal.fields",
    "crm_deal_stage_history":  "crm.deal.fields",
    "crm_deal_product_row":    "crm.deal.fields",
    "crm_lead":                "crm.lead.fields",
    "crm_lead_uf":             "crm.lead.fields",
    "crm_lead_status_history": "crm.lead.fields",
    "crm_lead_product_row":    "crm.lead.fields",
    "crm_contact":             "crm.contact.fields",
    "crm_contact_uf":          "crm.contact.fields",
    "crm_company":             "crm.company.fields",
    "crm_company_uf":          "crm.company.fields",
}

# Entities that have UF fields: map to the userfield.list REST method
_UF_METHODS: dict = {
    "crm_deal_uf":    "crm.deal.userfield.list",
    "crm_lead_uf":    "crm.lead.userfield.list",
    "crm_contact_uf": "crm.contact.userfield.list",
    "crm_company_uf": "crm.company.userfield.list",
}

def _label_from_uf(uf: dict) -> str:
    """Extract the best available Russian label from a userfield record."""
    for key in ("EDIT_FORM_LABEL", "LIST_COLUMN_LABEL", "LIST_FILTER_LABEL"):
        lbl = uf.get(key)
        if isinstance(lbl, str) and lbl.strip():
            return lbl.strip()
        if isinstance(lbl, dict):
            # {"ru": "Название"} or {"ru": null, "en": "Name"}
            text = lbl.get("ru") or lbl.get("en") or next((v for v in lbl.values() if v), "")
            if text and str(text).strip():
                return str(text).strip()
    return ""

@app.get("/api/field-labels")
async def api_field_labels(entity: str):
    """Return {FIELD_CODE: Russian_title} fetched from Bitrix24 REST API."""
    config  = load_config()
    webhook = config["bitrix"].get("rest_webhook", "").strip().rstrip("/")
    if not webhook:
        return {}
    try:
        labels: dict = {}

        if entity in _ENTITY_FIELDS_REST:
            # Standard field titles from crm.<entity>.fields
            data   = _rest(webhook, _ENTITY_FIELDS_REST[entity])
            result = data.get("result", {})
            for k, v in result.items():
                if isinstance(v, dict) and v.get("title") and v["title"] != k:
                    labels[k] = v["title"]

            # For _uf entities: batch-fetch proper labels via userfield.get per field
            if entity in _UF_METHODS:
                try:
                    uf_labels = await asyncio.to_thread(
                        _uf_labels_via_batch, webhook, _UF_METHODS[entity]
                    )
                    labels.update(uf_labels)
                except Exception as uf_exc:
                    logger.warning("batch uf labels for %s: %s", entity, uf_exc)

        elif entity.startswith("crm_dynamic_items_") and not entity.endswith("_product_row"):
            eid  = entity.replace("crm_dynamic_items_", "").split("_")[0]
            data = _rest(webhook, "crm.item.fields", {"entityTypeId": eid})
            raw  = data.get("result", {})
            fields = raw.get("fields", raw) if isinstance(raw, dict) else {}
            for k, v in fields.items():
                if not isinstance(v, dict) or not v.get("title"):
                    continue
                title = v["title"]
                upper = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", k).upper()
                if title != k:
                    labels[k] = title
                if upper != k and title != upper:
                    labels[upper] = title

        return labels
    except Exception as exc:
        logger.warning("field-labels: %s", exc)
        return {}

@app.get("/api/debug-userfields")
async def api_debug_userfields(entity: str = "crm_deal_uf"):
    """Return raw Bitrix24 response for userfield.list — for diagnostics only."""
    config  = load_config()
    webhook = config["bitrix"].get("rest_webhook", "").strip().rstrip("/")
    if not webhook:
        return {"error": "webhook not configured"}
    method = _UF_METHODS.get(entity)
    if not method:
        return {"error": f"no userfield method for {entity}"}
    try:
        data   = _rest(webhook, method, {"start": 0})
        result = data.get("result", [])
        sample = result[:3] if result else []

        # Also check crm.deal.fields for UF titles
        base_method = _ENTITY_FIELDS_REST.get(entity, "")
        uf_from_fields: dict = {}
        if base_method:
            fd = _rest(webhook, base_method)
            uf_from_fields = {
                k: v.get("title")
                for k, v in fd.get("result", {}).items()
                if k.startswith("UF_") and isinstance(v, dict)
            }

        # Try userfield.get for first field to see full structure
        first_get = {}
        if result:
            first_id = result[0].get("ID")
            try:
                first_get = _rest(webhook, method.replace(".list", ".get"), {"id": first_id})
            except Exception:
                pass

        return {
            "userfield_list": {
                "method": method,
                "total": data.get("total"),
                "first_3_labels": [
                    {
                        "FIELD_NAME": uf.get("FIELD_NAME"),
                        "EDIT_FORM_LABEL": uf.get("EDIT_FORM_LABEL"),
                        "LIST_COLUMN_LABEL": uf.get("LIST_COLUMN_LABEL"),
                    }
                    for uf in sample
                ],
            },
            "crm_entity_fields_uf_titles": dict(list(uf_from_fields.items())[:10]),
            "userfield_get_first": first_get,
        }
    except Exception as exc:
        return {"error": str(exc)}

@app.get("/api/crm-stages")
async def api_crm_stages(entity: str, category_ids: str = ""):
    """Return stages for selected funnels (or lead statuses)."""
    config = load_config()
    webhook = config["bitrix"].get("rest_webhook", "").strip().rstrip("/")
    if not webhook:
        return []
    ids = [x.strip() for x in category_ids.split(",") if x.strip()] or ["0"]
    stages: list = []
    seen:   set  = set()
    try:
        if entity in _DEAL_VARIANTS:
            for cat_id in ids:
                data = _rest(webhook, "crm.dealcategory.stage.list", {"id": cat_id})
                for s in data.get("result", []):
                    if s["STATUS_ID"] not in seen:
                        stages.append({"id": s["STATUS_ID"], "name": s["NAME"], "category_id": cat_id})
                        seen.add(s["STATUS_ID"])
        elif entity == "crm_lead":
            data = _rest(webhook, "crm.status.list", {"filter[ENTITY_ID]": "STATUS"})
            stages = [{"id": s["STATUS_ID"], "name": s["NAME"]}
                      for s in data.get("result", [])]
        elif entity.startswith("crm_dynamic_items_") and not entity.endswith("_product_row"):
            eid  = entity.replace("crm_dynamic_items_", "").split("_")[0]
            data = _rest(webhook, "crm.item.stage.list", {"entityTypeId": eid})
            raw  = data.get("result", {}).get("stages", [])
            stages = [{"id": str(s.get("statusId", s.get("id", ""))),
                       "name": s.get("name", ""),
                       "category_id": str(s.get("categoryId", ""))}
                      for s in raw]
    except Exception as exc:
        logger.warning("crm-stages: %s", exc)
    return stages

def fmtday(iso: str) -> str:
    """'2026-01-08' → '08.01.2026'"""
    y, m, d = iso.split("-")
    return f"{d}.{m}.{y}"

async def _export_event_iter(config: dict, entity: str, date_field: str,
                             start_date: str, end_date: str,
                             dimensions_filters, fields):
    """
    Async generator yielding event dicts (NOT SSE-formatted strings).
    Reused by both /api/export-stream (single) and /api/manual-export-stream (multi).
    Final event is always {'status':'done','total':N} or {'status':'error','error':str}.
    """
    portal   = config["bitrix"]["portal"]
    bi_key   = config["bitrix"]["bi_key"]
    do_daily = bool(date_field and start_date and end_date)

    if dimensions_filters:
        parts = []
        for f in dimensions_filters:
            vals = f.get("values", [])
            v_str = ", ".join(str(v) for v in vals[:5])
            if len(vals) > 5:
                v_str += f"... (+{len(vals)-5})"
            parts.append(f"{f.get('fieldName')} IN [{v_str}]")
        yield {"status": "info", "message": "Фильтры: " + " | ".join(parts)}
    if fields:
        fields_preview = ", ".join(fields[:8]) + ("..." if len(fields) > 8 else "")
        yield {"status": "info", "message": f"Поля ({len(fields)}): {fields_preview}"}

    # ── Товары в сделках по дате закрытия: двухшаговый pivot ──────────
    if entity == "crm_deal_product_row" and date_field == "DEAL_CLOSEDATE" and do_daily:
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt  = datetime.strptime(end_date,   "%Y-%m-%d")
        total   = 0
        while current <= end_dt:
            day = current.strftime("%Y-%m-%d")
            try:
                _ids_task = asyncio.ensure_future(
                    asyncio.to_thread(fetch_from_bitrix, portal, bi_key, "crm_deal",
                                      "CLOSEDATE", day, day, dimensions_filters, ["ID"]))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_ids_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield {"status": "info", "message": f"Получение сделок за {fmtday(day)}..."}
                deal_raw = _ids_task.result()

                deal_ids = []
                if len(deal_raw) > 1:
                    hdr    = deal_raw[0]
                    id_col = hdr.index("ID") if "ID" in hdr else 0
                    deal_ids = [str(r[id_col]) for r in deal_raw[1:]
                                if id_col < len(r) and r[id_col] is not None]

                if not deal_ids:
                    yield {"date": day, "rows": 0, "total": total, "status": "ok"}
                    current += timedelta(days=1)
                    continue

                id_filter = {"fieldName": "DEAL_ID", "values": deal_ids,
                             "type": "INCLUDE", "operator": "IN_LIST"}
                _task = asyncio.ensure_future(
                    asyncio.to_thread(fetch_from_bitrix_safe, portal, bi_key, entity,
                                      None, None, None, [id_filter], fields))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield {"status": "info", "message": f"Загрузка товаров за {fmtday(day)}..."}
                raw, removed = _task.result()
                if removed:
                    yield {"status": "info",
                           "message": f"⚠ Пропущены отсутствующие поля: {', '.join(removed)}"}

                if len(raw) > 1:
                    pr_hdr = raw[0]
                    did_col = pr_hdr.index("DEAL_ID") if "DEAL_ID" in pr_hdr else None
                    if did_col is not None:
                        found_ids = {str(r[did_col]) for r in raw[1:]
                                     if did_col < len(r) and r[did_col] is not None}
                        no_products = len(set(deal_ids) - found_ids)
                        if no_products:
                            yield {"status": "info",
                                   "message": f"{fmtday(day)}: сделок {len(deal_ids)}, без товаров {no_products}"}

                _ch_task = asyncio.ensure_future(
                    asyncio.to_thread(push_to_clickhouse, config, entity, raw))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_ch_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield {"status": "info", "message": f"Запись в ClickHouse за {fmtday(day)}..."}
                rows  = _ch_task.result()
                total += rows
                yield {"date": day, "rows": rows, "total": total, "status": "ok"}
            except Exception as exc:
                yield {"date": day, "rows": 0, "total": total, "status": "error", "error": str(exc)}
            current += timedelta(days=1)
        yield {"status": "done", "total": total}
        return

    # ── crm_deal_uf / stage_history + фильтр CATEGORY_ID/STAGE_ID ─────
    if _needs_deal_id_pivot(entity, dimensions_filters) and do_daily:
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt  = datetime.strptime(end_date,   "%Y-%m-%d")
        total   = 0
        while current <= end_dt:
            day = current.strftime("%Y-%m-%d")
            try:
                _ids_task = asyncio.ensure_future(
                    asyncio.to_thread(fetch_from_bitrix, portal, bi_key, "crm_deal",
                                      date_field, day, day, dimensions_filters, ["ID"]))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_ids_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield {"status": "info", "message": f"ID сделок за {fmtday(day)}..."}
                deal_raw = _ids_task.result()

                deal_ids = []
                if len(deal_raw) > 1:
                    hdr    = deal_raw[0]
                    id_col = hdr.index("ID") if "ID" in hdr else 0
                    deal_ids = [str(r[id_col]) for r in deal_raw[1:]
                                if id_col < len(r) and r[id_col] is not None]

                if not deal_ids:
                    yield {"date": day, "rows": 0, "total": total, "status": "ok"}
                    current += timedelta(days=1)
                    continue

                id_filter = {"fieldName": "DEAL_ID", "values": deal_ids,
                             "type": "INCLUDE", "operator": "IN_LIST"}
                _task = asyncio.ensure_future(
                    asyncio.to_thread(fetch_from_bitrix_safe, portal, bi_key, entity,
                                      None, None, None, [id_filter], fields))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield {"status": "info", "message": f"Загрузка {entity} за {fmtday(day)}..."}
                raw, removed = _task.result()
                if removed:
                    yield {"status": "info",
                           "message": f"⚠ Пропущены отсутствующие поля: {', '.join(removed)}"}

                _ch_task = asyncio.ensure_future(
                    asyncio.to_thread(push_to_clickhouse, config, entity, raw))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_ch_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield {"status": "info", "message": f"Запись в ClickHouse за {fmtday(day)}..."}
                rows  = _ch_task.result()
                total += rows
                yield {"date": day, "rows": rows, "total": total, "status": "ok"}
            except Exception as exc:
                yield {"date": day, "rows": 0, "total": total, "status": "error", "error": str(exc)}
            current += timedelta(days=1)
        yield {"status": "done", "total": total}
        return

    if not do_daily:
        try:
            yield {"status": "info", "message": "Запрос данных из Bitrix24..."}
            _task = asyncio.ensure_future(
                asyncio.to_thread(fetch_from_bitrix_safe, portal, bi_key, entity,
                                  None, None, None, dimensions_filters, fields))
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(_task), timeout=20.0)
                    break
                except asyncio.TimeoutError:
                    yield {"status": "info", "message": "Ожидание ответа Bitrix24..."}
            raw, removed = _task.result()
            if removed:
                yield {"status": "info",
                       "message": f"⚠ Пропущены отсутствующие поля: {', '.join(removed)}"}
            bi_n = len(raw) - 1 if isinstance(raw, list) and raw else 0
            yield {"status": "info", "message": f"BI connector вернул: {bi_n} строк"}
            _ch_task = asyncio.ensure_future(
                asyncio.to_thread(push_to_clickhouse, config, entity, raw))
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(_ch_task), timeout=20.0)
                    break
                except asyncio.TimeoutError:
                    yield {"status": "info", "message": "Запись в ClickHouse..."}
            rows = _ch_task.result()
            yield {"status": "done", "rows": rows, "total": rows}
        except Exception as exc:
            yield {"status": "error", "error": str(exc)}
        return

    current  = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")
    total    = 0
    while current <= end_dt:
        day = current.strftime("%Y-%m-%d")
        try:
            _task = asyncio.ensure_future(
                asyncio.to_thread(fetch_from_bitrix_safe, portal, bi_key, entity,
                                  date_field, day, day, dimensions_filters, fields))
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(_task), timeout=20.0)
                    break
                except asyncio.TimeoutError:
                    yield {"status": "info", "message": f"Ожидание ответа за {fmtday(day)}..."}
            raw, removed = _task.result()
            if removed:
                yield {"status": "info",
                       "message": f"⚠ {fmtday(day)}: пропущены отсутствующие поля: {', '.join(removed)}"}
            _ch_task = asyncio.ensure_future(
                asyncio.to_thread(push_to_clickhouse, config, entity, raw))
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(_ch_task), timeout=20.0)
                    break
                except asyncio.TimeoutError:
                    yield {"status": "info", "message": f"Запись в ClickHouse за {fmtday(day)}..."}
            rows = _ch_task.result()
            total += rows
            yield {"date": day, "rows": rows, "total": total, "status": "ok"}
        except Exception as exc:
            yield {"date": day, "rows": 0, "total": total, "status": "error", "error": str(exc)}
        current += timedelta(days=1)

    yield {"status": "done", "total": total}


def _sse(ev: dict) -> str:
    return f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"


async def _iter_with_history(inner, *, source: str, entity: str,
                             date_field: str, start_date: str, end_date: str,
                             dimensions_filters, fields,
                             config_name: Optional[str] = None,
                             provider: str = "bitrix"):
    """Pass-through async generator that records a history entry on completion."""
    started = datetime.now()
    rows    = 0
    error   = None
    try:
        async for ev in inner:
            st = ev.get("status")
            if st == "done":
                rows = ev.get("total", 0)
            elif st == "error" and not ev.get("date"):
                # Top-level error (not per-day). Per-day errors are kept inline.
                error = ev.get("error", "?")
            yield ev
    except Exception as exc:
        error = str(exc)
        raise
    finally:
        finished = datetime.now()
        _record_history({
            "started_at":         started.isoformat(timespec="seconds"),
            "finished_at":        finished.isoformat(timespec="seconds"),
            "duration_sec":       int((finished - started).total_seconds()),
            "provider":           provider,
            "source":             source,
            "config_name":        config_name,
            "entity":             entity or "",
            "date_field":         date_field or "",
            "start_date":         start_date or "",
            "end_date":           end_date or "",
            "dimensions_filters": dimensions_filters or [],
            "fields":             fields or [],
            "rows":               rows,
            "status":             "error" if error else "ok",
            "error":              error,
        })


@app.post("/api/export-stream")
async def api_export_stream(data: dict):
    config = load_config()
    entity             = data.get("entity", "")
    date_field         = data.get("date_field", "")
    start_date         = data.get("start_date", "")
    end_date           = data.get("end_date", "")
    dimensions_filters = data.get("dimensions_filters") or None
    fields             = data.get("fields") or None
    provider           = data.get("provider") or "bitrix"

    async def gen():
        inner = _export_event_iter(config, entity, date_field,
                                   start_date, end_date,
                                   dimensions_filters, fields)
        async for ev in _iter_with_history(
            inner,
            source="manual_form", entity=entity, date_field=date_field,
            start_date=start_date, end_date=end_date,
            dimensions_filters=dimensions_filters, fields=fields,
            provider=provider,
        ):
            yield _sse(ev)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/manual-export-stream")
async def api_manual_export_stream(data: dict):
    """
    Run several jobs sequentially, streaming combined events.
    Body: {jobs: [{name, entity, date_field, dimensions_filters?, fields?}],
           start_date, end_date}
    Adds 'job_idx' / 'job_name' / 'entity' to each event so UI can route them.
    Emits 'job_start', 'job_done' between jobs and final 'all_done'.
    """
    config = load_config()
    jobs       = data.get("jobs", []) or []
    start_date = data.get("start_date", "")
    end_date   = data.get("end_date", "")

    async def gen():
        grand_total = 0
        for idx, job in enumerate(jobs):
            name   = job.get("name") or f"#{idx+1}"
            entity = job.get("entity", "")
            df     = job.get("date_field", "") or ""
            dims   = job.get("dimensions_filters") or None
            flds   = job.get("fields") or None

            yield _sse({"status": "job_start", "job_idx": idx,
                        "job_name": name, "entity": entity,
                        "total_jobs": len(jobs)})
            job_total = 0
            job_provider = job.get("provider") or "bitrix"
            ozon_account = job.get("ozon_account") or ""
            try:
                if job_provider == "ozon":
                    inner = _ozon_export_event_iter(config, ozon_account, entity,
                                                    df, start_date, end_date, flds)
                else:
                    inner = _export_event_iter(config, entity, df,
                                               start_date, end_date,
                                               dims, flds)
                async for ev in _iter_with_history(
                    inner,
                    source="manual_batch", entity=entity, date_field=df,
                    start_date=start_date, end_date=end_date,
                    dimensions_filters=dims, fields=flds,
                    config_name=name, provider=job_provider,
                ):
                    ev["job_idx"]  = idx
                    ev["job_name"] = name
                    if ev.get("status") == "done":
                        job_total = ev.get("total", 0)
                        grand_total += job_total
                        yield _sse({"status": "job_done", "job_idx": idx,
                                    "job_name": name, "rows": job_total})
                    else:
                        yield _sse(ev)
            except Exception as exc:
                yield _sse({"status": "job_done", "job_idx": idx,
                            "job_name": name, "rows": 0, "error": str(exc)})
        yield _sse({"status": "all_done", "total": grand_total})

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

@app.get("/api/schedule")
async def api_get_schedule():
    return load_config().get("schedule", {})

@app.post("/api/schedule")
async def api_save_schedule(data: dict):
    config = load_config()
    config["schedule"] = data
    save_config(config)
    _apply_schedule(config)
    return {"status": "ok"}

@app.delete("/api/schedule")
async def api_stop_schedule():
    config = load_config()
    config["schedule"]["enabled"] = False
    save_config(config)
    if scheduler.get_job("export_job"):
        scheduler.remove_job("export_job")
    return {"status": "ok"}

# ---------------------------------------------------------------------------
# Reconciliation: compare Bitrix vs ClickHouse, find changes, apply updates
# ---------------------------------------------------------------------------
def _norm_for_compare(v) -> str:
    """Normalize a value to string for cross-type comparison (BI returns strings,
    ClickHouse returns native types). None / empty → empty string."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    s = str(v).strip()
    # Bitrix often returns "2026-01-01T10:00:00" or "2026-01-01 10:00:00.000"
    # → strip subseconds and unify separator.
    if len(s) >= 19 and s[10] in ("T", " ") and s[4] == "-" and s[7] == "-":
        return s[:10] + " " + s[11:19]
    return s

def _compare_with_clickhouse(config: dict, table_name: str, raw: list) -> dict:
    """
    raw: BI-style [headers, row, row, ...] (must contain ID column).
    Returns {
      'headers': [...],
      'id_col': str,
      'new':       [{'id': ..., 'row': [...]}, ...],
      'changed':   [{'id': ..., 'row': [...], 'diff': {field: {'old':..,'new':..}}}, ...],
      'unchanged_count': N,
      'total_bitrix': N,
      'warning': str | None,
    }
    """
    if not raw or len(raw) < 1:
        return {"headers": [], "id_col": "", "new": [], "changed": [],
                "unchanged_count": 0, "total_bitrix": 0,
                "warning": "BI вернул пустой ответ"}

    headers = list(raw[0])
    if "ID" not in headers:
        return {"headers": headers, "id_col": "", "new": [], "changed": [],
                "unchanged_count": 0, "total_bitrix": max(0, len(raw) - 1),
                "warning": "Сущность не имеет колонки ID — сверка невозможна"}

    id_idx = headers.index("ID")
    rows   = raw[1:]
    if not rows:
        return {"headers": headers, "id_col": "ID", "new": [], "changed": [],
                "unchanged_count": 0, "total_bitrix": 0, "warning": None}

    # Build dict id → row for incoming Bitrix data.
    bitrix_by_id: dict = {}
    for r in rows:
        if id_idx >= len(r) or r[id_idx] is None or str(r[id_idx]).strip() == "":
            continue
        bitrix_by_id[str(r[id_idx])] = list(r) + [None] * (len(headers) - len(r))

    safe = re.sub(r"[^\w]", "_", table_name)
    client = get_ch_client(config)

    # Read existing ClickHouse columns; only compare overlapping ones.
    ch_cols = list(_table_col_types(client, safe).keys())
    if not ch_cols:
        # Table doesn't exist yet — everything is new.
        new_records = [{"id": i, "row": r} for i, r in bitrix_by_id.items()]
        return {"headers": headers, "id_col": "ID", "new": new_records,
                "changed": [], "unchanged_count": 0,
                "total_bitrix": len(bitrix_by_id),
                "warning": "Таблица в ClickHouse ещё не создана — все строки будут добавлены"}

    common_cols = [h for h in headers if h in ch_cols]
    if "ID" not in common_cols:
        common_cols = ["ID"] + common_cols

    # Fetch matching CH rows. Chunked to avoid huge IN clauses.
    ids_list  = list(bitrix_by_id.keys())
    ch_by_id: dict = {}
    cols_sql = ", ".join(f"`{c}`" for c in common_cols)
    for i in range(0, len(ids_list), 1000):
        chunk = ids_list[i:i+1000]
        # Quote each id; IDs in CH are usually Int but in Bitrix come as strings.
        ids_quoted = ", ".join(f"'{x}'" for x in chunk)
        try:
            rs = client.query(
                f"SELECT {cols_sql} FROM `{safe}` "
                f"WHERE toString(`ID`) IN ({ids_quoted})"
            )
            for ch_row in rs.result_rows:
                row_dict = dict(zip(common_cols, ch_row))
                ch_by_id[str(row_dict.get("ID", ""))] = row_dict
        except Exception as e:
            logger.warning("Reconciliation SELECT chunk failed: %s", e)

    new_records: list     = []
    changed_records: list = []
    unchanged             = 0

    for rid, brow in bitrix_by_id.items():
        ch_row = ch_by_id.get(rid)
        if ch_row is None:
            new_records.append({"id": rid, "row": brow})
            continue
        diff: dict = {}
        for col in common_cols:
            if col == "ID":
                continue
            b_val = brow[headers.index(col)] if col in headers else None
            c_val = ch_row.get(col)
            if _norm_for_compare(b_val) != _norm_for_compare(c_val):
                diff[col] = {
                    "old": _norm_for_compare(c_val),
                    "new": _norm_for_compare(b_val),
                }
        if diff:
            changed_records.append({"id": rid, "row": brow, "diff": diff})
        else:
            unchanged += 1

    return {
        "headers":         headers,
        "id_col":          "ID",
        "new":             new_records,
        "changed":         changed_records,
        "unchanged_count": unchanged,
        "total_bitrix":    len(bitrix_by_id),
        "warning":         None,
    }

def _apply_reconciliation(config: dict, table_name: str, headers: list,
                          rows: list, delete_ids: list) -> dict:
    """DELETE rows by ID then INSERT replacement rows. Returns {deleted, inserted}."""
    safe   = re.sub(r"[^\w]", "_", table_name)
    client = get_ch_client(config)

    # Make sure table has all needed columns (auto-ALTER).
    if rows:
        sample_n = min(200, len(rows))
        inferred = [
            _infer_type([rows[j][i] if i < len(rows[j]) else None for j in range(sample_n)])
            for i in range(len(headers))
        ]
        existing = _table_col_types(client, safe)
        if not existing:
            cols_sql = ",\n  ".join(f"`{h}` {t}" for h, t in zip(headers, inferred))
            client.command(f"""
                CREATE TABLE IF NOT EXISTS `{safe}` (
                  {cols_sql}
                ) ENGINE = MergeTree()
                ORDER BY tuple()
            """)
            existing = _table_col_types(client, safe)
        for h, t in zip(headers, inferred):
            if h not in existing:
                try:
                    client.command(f"ALTER TABLE `{safe}` ADD COLUMN IF NOT EXISTS `{h}` {t}")
                    existing[h] = t
                except Exception as e:
                    logger.warning("ALTER ADD %s.%s: %s", safe, h, e)
        col_types = [existing.get(h, inferred[i]) for i, h in enumerate(headers)]
    else:
        col_types = []

    deleted = 0
    if delete_ids:
        # Synchronous mutation so the subsequent INSERT doesn't see leftovers.
        for i in range(0, len(delete_ids), 1000):
            chunk = delete_ids[i:i+1000]
            ids_quoted = ", ".join(f"'{x}'" for x in chunk)
            try:
                client.command(
                    f"ALTER TABLE `{safe}` DELETE WHERE toString(`ID`) IN ({ids_quoted}) "
                    f"SETTINGS mutations_sync = 2"
                )
                deleted += len(chunk)
            except Exception as e:
                logger.error("DELETE chunk failed: %s", e)
                raise

    inserted = 0
    if rows:
        converted = []
        for r in rows:
            padded = list(r) + [None] * (len(headers) - len(r))
            converted.append([_convert(v, t) for v, t in zip(padded, col_types)])
        client.insert(safe, converted, column_names=list(headers))
        inserted = len(rows)

    return {"deleted": deleted, "inserted": inserted}


@app.post("/api/check-updates-stream")
async def api_check_updates_stream(data: dict):
    """
    Body: {configs: [name1, ...], start_date: 'YYYY-MM-DD', end_date: 'YYYY-MM-DD'}
    For each saved config: pulls Bitrix data filtered by DATE_MODIFY (or analogous
    field) over the period, compares with ClickHouse, streams a result event.
    """
    config       = load_config()
    saved_map    = {c["name"]: c for c in config.get("saved_configs", [])
                    if isinstance(c, dict) and c.get("name")}
    config_names = data.get("configs", []) or []
    start_date   = data.get("start_date", "")
    end_date     = data.get("end_date", "")
    portal       = config["bitrix"]["portal"]
    bi_key       = config["bitrix"]["bi_key"]

    # Fields that mark "modified" for various entities. First match wins.
    MODIFY_FIELD_CANDIDATES = ["DATE_MODIFY", "CHANGED_DATE", "DATE_UPDATE"]

    def _pick_modify_field(entity_code: str) -> Optional[str]:
        ent = None
        for items in ENTITIES.values():
            for e in items:
                if e["code"] == entity_code:
                    ent = e; break
            if ent: break
        if not ent:
            return MODIFY_FIELD_CANDIDATES[0]
        for cand in MODIFY_FIELD_CANDIDATES:
            if cand in ent.get("date_fields", []):
                return cand
        return None

    async def gen():
        for idx, name in enumerate(config_names):
            sc = saved_map.get(name)
            if not sc:
                yield _sse({"status": "config_done", "config_idx": idx, "config_name": name,
                            "error": "Конфигурация не найдена"})
                continue

            entity = sc.get("entity", "")
            mod_field = _pick_modify_field(entity)
            yield _sse({"status": "config_start", "config_idx": idx, "config_name": name,
                        "entity": entity, "modify_field": mod_field,
                        "total_configs": len(config_names)})

            if not mod_field:
                yield _sse({"status": "config_done", "config_idx": idx, "config_name": name,
                            "error": "У сущности нет поля даты изменения — сверка невозможна"})
                continue

            try:
                # Fetch Bitrix data for the modify period with same filters/fields.
                # Use safe wrapper to skip removed fields.
                _task = asyncio.ensure_future(asyncio.to_thread(
                    fetch_from_bitrix_safe, portal, bi_key, entity,
                    mod_field, start_date, end_date,
                    sc.get("dimensions_filters") or None,
                    sc.get("fields") or None
                ))
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(_task), timeout=20.0)
                        break
                    except asyncio.TimeoutError:
                        yield _sse({"status": "info", "config_idx": idx,
                                    "message": f"Ожидание данных от Bitrix24 ({name})..."})
                raw, removed = _task.result()
                if removed:
                    yield _sse({"status": "info", "config_idx": idx,
                                "message": f"⚠ Пропущены отсутствующие поля: {', '.join(removed)}"})

                yield _sse({"status": "info", "config_idx": idx,
                            "message": f"Сравнение с ClickHouse..."})
                result = await asyncio.to_thread(_compare_with_clickhouse, config, entity, raw)
                yield _sse({"status": "config_done", "config_idx": idx,
                            "config_name": name, "entity": entity,
                            "result": result})
            except Exception as exc:
                yield _sse({"status": "config_done", "config_idx": idx,
                            "config_name": name, "error": str(exc)})

        yield _sse({"status": "all_done"})

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/apply-updates")
async def api_apply_updates(data: dict):
    """
    Body: {entity, headers: [...], rows: [[...], [...]], delete_ids: [...]}
    DELETE old rows by ID then INSERT new rows. Returns {deleted, inserted}.
    """
    entity     = data.get("entity", "")
    headers    = data.get("headers", []) or []
    rows       = data.get("rows", []) or []
    delete_ids = data.get("delete_ids", []) or []
    if not entity:
        raise HTTPException(400, "Не указана сущность")
    config = load_config()
    started = datetime.now()
    try:
        result = await asyncio.to_thread(
            _apply_reconciliation, config, entity, headers, rows, delete_ids
        )
        finished = datetime.now()
        _record_history({
            "started_at":         started.isoformat(timespec="seconds"),
            "finished_at":        finished.isoformat(timespec="seconds"),
            "duration_sec":       int((finished - started).total_seconds()),
            "provider":           data.get("provider") or "bitrix",
            "source":             "reconciliation",
            "config_name":        data.get("config_name"),
            "entity":             entity,
            "date_field":         "",
            "start_date":         "",
            "end_date":           "",
            "dimensions_filters": [],
            "fields":             headers,
            "rows":               result.get("inserted", 0),
            "deleted":            result.get("deleted", 0),
            "status":             "ok",
            "error":              None,
        })
        return {"status": "ok", **result}
    except Exception as exc:
        finished = datetime.now()
        _record_history({
            "started_at":         started.isoformat(timespec="seconds"),
            "finished_at":        finished.isoformat(timespec="seconds"),
            "duration_sec":       int((finished - started).total_seconds()),
            "provider":           data.get("provider") or "bitrix",
            "source":             "reconciliation",
            "config_name":        data.get("config_name"),
            "entity":             entity,
            "date_field":         "",
            "start_date":         "",
            "end_date":           "",
            "dimensions_filters": [],
            "fields":             headers,
            "rows":               0,
            "status":             "error",
            "error":              str(exc),
        })
        raise HTTPException(500, str(exc))

# ---------------------------------------------------------------------------
# Saved configurations (named export presets)
# ---------------------------------------------------------------------------
def _normalize_saved_config(data: dict, name: str) -> dict:
    provider = (data.get("provider") or "bitrix").strip() or "bitrix"
    return {
        "name":               name,
        "provider":           provider,
        "ozon_account":       (data.get("ozon_account") or "") if provider == "ozon" else "",
        "entity":             data.get("entity", ""),
        "date_field":         data.get("date_field", "") or "",
        "dimensions_filters": data.get("dimensions_filters") or [],
        "fields":             data.get("fields") or [],
    }

@app.get("/api/saved-configs")
async def api_get_saved_configs():
    items = load_config().get("saved_configs", [])
    # Backfill provider="bitrix" for legacy entries that pre-date provider support.
    for it in items:
        if isinstance(it, dict) and not it.get("provider"):
            it["provider"] = "bitrix"
    return items

@app.post("/api/saved-configs")
async def api_create_saved_config(data: dict):
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Укажите название конфигурации")
    if not (data.get("entity") or "").strip():
        raise HTTPException(400, "Не выбрана сущность")
    config  = load_config()
    configs = config.get("saved_configs", [])
    if any(c.get("name") == name for c in configs):
        raise HTTPException(409, f"Конфигурация с названием «{name}» уже существует")
    configs.append(_normalize_saved_config(data, name))
    config["saved_configs"] = configs
    save_config(config)
    return {"status": "ok"}

@app.put("/api/saved-configs/{name}")
async def api_update_saved_config(name: str, data: dict):
    config  = load_config()
    configs = config.get("saved_configs", [])
    for i, c in enumerate(configs):
        if c.get("name") == name:
            configs[i] = _normalize_saved_config(data, name)
            config["saved_configs"] = configs
            save_config(config)
            return {"status": "ok"}
    raise HTTPException(404, f"Конфигурация «{name}» не найдена")

@app.post("/api/saved-configs/{name}/rename")
async def api_rename_saved_config(name: str, data: dict):
    new_name = (data.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(400, "Укажите новое название")
    if new_name == name:
        return {"status": "ok"}
    config  = load_config()
    configs = config.get("saved_configs", [])
    if any(c.get("name") == new_name for c in configs):
        raise HTTPException(409, f"Конфигурация с названием «{new_name}» уже существует")
    found = False
    for c in configs:
        if c.get("name") == name:
            c["name"] = new_name
            found = True
            break
    if not found:
        raise HTTPException(404, f"Конфигурация «{name}» не найдена")
    config["saved_configs"] = configs
    sch = config.get("schedule", {})
    if isinstance(sch.get("configs"), list):
        sch["configs"] = [new_name if n == name else n for n in sch["configs"]]
        config["schedule"] = sch
    save_config(config)
    _apply_schedule(config)
    return {"status": "ok"}

@app.get("/api/history")
async def api_get_history(limit: int = 200):
    items = _load_history()
    if limit and limit > 0:
        items = items[:limit]
    # Enrich with display names server-side so the client doesn't need ENTITIES.
    for it in items:
        it["entity_name"] = _entity_display_name(it.get("entity", ""))
        it.setdefault("provider", "bitrix")
    return items

@app.delete("/api/history")
async def api_clear_history():
    _save_history([])
    return {"status": "ok"}

@app.delete("/api/history/{entry_id}")
async def api_delete_history_entry(entry_id: int, also_data: bool = False):
    """
    Delete a single history entry by id.
    If also_data=True — also delete the matching slice from ClickHouse using
    the entry's table (=entity), date_field and start_date/end_date.
    """
    items = _load_history()
    target = next((it for it in items if it.get("id") == entry_id), None)
    if not target:
        raise HTTPException(404, "Запись истории не найдена")

    deleted_rows = 0
    data_warn: Optional[str] = None
    if also_data:
        entity     = (target.get("entity") or "").strip()
        date_field = (target.get("date_field") or "").strip()
        start_date = (target.get("start_date") or "").strip()
        end_date   = (target.get("end_date") or "").strip()
        if not entity or not date_field or not start_date or not end_date:
            data_warn = "Недостаточно данных в записи (нет сущности/даты/периода) — удалена только запись истории."
        else:
            try:
                deleted_rows = _delete_clickhouse_period(entity, date_field, start_date, end_date)
            except Exception as exc:
                data_warn = f"Не удалось удалить из ClickHouse: {exc}"

    items = [it for it in items if it.get("id") != entry_id]
    _save_history(items)
    return {"status": "ok", "deleted_rows": deleted_rows, "warning": data_warn}

def _delete_clickhouse_period(entity: str, date_field: str,
                              start_date: str, end_date: str) -> int:
    """ALTER TABLE ... DELETE WHERE date_field BETWEEN start AND end. Returns row count before delete."""
    config = load_config()
    client = get_ch_client(config)
    safe = entity.replace("`", "")
    df_safe = date_field.replace("`", "")
    # Count first so we can report deleted rows (synchronous mutation does not return count).
    try:
        cnt_res = client.query(
            f"SELECT count() FROM `{safe}` "
            f"WHERE toDate(`{df_safe}`) BETWEEN toDate('{start_date}') AND toDate('{end_date}')"
        )
        cnt = int(cnt_res.result_rows[0][0]) if cnt_res.result_rows else 0
    except Exception as exc:
        raise RuntimeError(f"COUNT failed: {exc}")
    client.command(
        f"ALTER TABLE `{safe}` DELETE "
        f"WHERE toDate(`{df_safe}`) BETWEEN toDate('{start_date}') AND toDate('{end_date}') "
        f"SETTINGS mutations_sync = 2"
    )
    return cnt

@app.post("/api/delete-data")
async def api_delete_data(data: dict):
    """
    Body: {entity, date_field, start_date, end_date}
    Удаляет строки из ClickHouse за указанный период по выбранному полю даты.
    """
    entity     = (data.get("entity") or "").strip()
    date_field = (data.get("date_field") or "").strip()
    start_date = (data.get("start_date") or "").strip()
    end_date   = (data.get("end_date") or "").strip()
    if not entity:     raise HTTPException(400, "Не указана сущность")
    if not date_field: raise HTTPException(400, "Не указано поле даты")
    if not start_date or not end_date:
        raise HTTPException(400, "Не указан период")
    try:
        deleted = _delete_clickhouse_period(entity, date_field, start_date, end_date)
    except Exception as exc:
        raise HTTPException(500, f"Ошибка удаления: {exc}")
    return {"status": "ok", "deleted_rows": deleted}

@app.delete("/api/saved-configs/{name}")
async def api_delete_saved_config(name: str):
    config  = load_config()
    configs = config.get("saved_configs", [])
    new_list = [c for c in configs if c.get("name") != name]
    if len(new_list) == len(configs):
        raise HTTPException(404, f"Конфигурация «{name}» не найдена")
    config["saved_configs"] = new_list
    sch = config.get("schedule", {})
    if isinstance(sch.get("configs"), list):
        sch["configs"] = [n for n in sch["configs"] if n != name]
        config["schedule"] = sch
    save_config(config)
    _apply_schedule(config)
    return {"status": "ok"}

def _check_connection_on_startup() -> None:
    """Auto-verify saved credentials so the UI shows correct status immediately."""
    global connection_status
    config = load_config()
    portal = config["bitrix"].get("portal", "").strip()
    bi_key = config["bitrix"].get("bi_key", "").strip()
    host   = config["clickhouse"].get("host", "").strip()

    if not portal or not bi_key or not host:
        return  # nothing configured yet, leave status as None

    logger.info("Auto-checking saved connection settings...")
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        fetch_from_bitrix(portal, bi_key, "crm_lead", "DATE_CREATE", today, today)
        connection_status["bitrix"] = True
        logger.info("Bitrix24 connection: OK")
    except Exception as exc:
        connection_status["bitrix"] = False
        logger.warning("Bitrix24 connection failed: %s", exc)

    try:
        get_ch_client(config).ping()
        connection_status["clickhouse"] = True
        logger.info("ClickHouse connection: OK")
    except Exception as exc:
        connection_status["clickhouse"] = False
        logger.warning("ClickHouse connection failed: %s", exc)

@app.on_event("startup")
def on_startup():
    scheduler.start()
    config = load_config()
    _apply_schedule(config)
    _check_connection_on_startup()

@app.on_event("shutdown")
def on_shutdown():
    scheduler.shutdown(wait=False)

# ===========================================================================
# ===============================  OZON PROVIDER  ===========================
# ===========================================================================
OZON_API_BASE  = "https://api-seller.ozon.ru"
OZON_PERF_BASE = "https://api-performance.ozon.ru"

OZON_ENTITIES = {
    "Каталог": [
        {"code": "ozon_product", "name": "Товары",            "date_fields": []},
        {"code": "ozon_stock",   "name": "Остатки на складах", "date_fields": []},
    ],
    "Заказы и возвраты": [
        {"code": "ozon_posting_fbs", "name": "Заказы FBS",         "date_fields": ["in_process_at"]},
        {"code": "ozon_posting_fbo", "name": "Заказы FBO",         "date_fields": ["in_process_at"]},
        {"code": "ozon_returns_fbs", "name": "Возвраты FBS",       "date_fields": []},
        {"code": "ozon_returns_fbo", "name": "Возвраты FBO",       "date_fields": []},
    ],
    "Финансы и аналитика": [
        {"code": "ozon_finance_transaction", "name": "Финансовые транзакции",  "date_fields": ["operation_date"]},
        {"code": "ozon_analytics_data",      "name": "Аналитика товаров",       "date_fields": ["date"]},
        {"code": "ozon_analytics_stocks",    "name": "Аналитика остатков",      "date_fields": []},
    ],
    "Реклама (Performance API)": [
        {"code": "ozon_perf_campaigns",  "name": "Рекламные кампании",     "date_fields": []},
        {"code": "ozon_perf_statistics", "name": "Статистика кампаний",    "date_fields": ["date"]},
    ],
}

OZON_DATE_LABELS = {
    "in_process_at":  "Дата заказа",
    "operation_date": "Дата операции",
    "date":           "Дата",
}

# Available fields per entity (matches headers produced by each fetcher).
# When empty / not selected — all fields are returned.
OZON_ENTITY_FIELDS: dict = {
    "ozon_product": [
        "product_id", "offer_id", "name", "barcode", "category_id",
        "marketing_price", "min_price", "old_price", "price",
        "vat", "is_discounted", "is_kgt", "archived",
        "created_at", "primary_image", "images_count", "weight",
    ],
    "ozon_stock": [
        "product_id", "offer_id", "sku", "warehouse_name", "present", "reserved",
    ],
    "ozon_posting_fbs": [
        "posting_number", "order_id", "order_number", "status", "substatus",
        "in_process_at", "shipment_date", "delivery_method_name", "warehouse_name",
        "tracking_number", "is_express",
        "product_sku", "product_offer_id", "product_name",
        "product_price", "product_quantity", "product_currency_code",
        "delivery_price", "commission_amount", "payout",
    ],
    "ozon_posting_fbo": [
        "posting_number", "order_id", "order_number", "status", "substatus",
        "in_process_at", "shipment_date", "delivery_method_name", "warehouse_name",
        "tracking_number", "is_express",
        "product_sku", "product_offer_id", "product_name",
        "product_price", "product_quantity", "product_currency_code",
        "delivery_price", "commission_amount", "payout",
    ],
    "ozon_returns_fbs": [
        "id", "posting_number", "order_id", "sku", "offer_id", "name",
        "quantity", "price", "currency_code", "status_name", "return_reason_name",
        "schema", "place_name", "moved_to_place_moment", "returned_to_ozon_moment",
    ],
    "ozon_returns_fbo": [
        "id", "posting_number", "order_id", "sku", "offer_id", "name",
        "quantity", "price", "currency_code", "status_name", "return_reason_name",
        "schema", "place_name", "moved_to_place_moment", "returned_to_ozon_moment",
    ],
    "ozon_finance_transaction": [
        "operation_id", "operation_type", "operation_type_name", "operation_date",
        "type", "posting_number", "delivery_schema",
        "amount", "accruals_for_sale", "sale_commission",
        "delivery_charge", "return_delivery_charge",
        "services_count", "services_total", "services_summary",
    ],
    "ozon_analytics_data": ["date", "sku"] + OZON_ANALYTICS_DEFAULT_METRICS if False else [],
    # ^ filled below since OZON_ANALYTICS_DEFAULT_METRICS is defined later
    "ozon_analytics_stocks": [
        "sku", "offer_id", "name",
        "cluster_name", "warehouse_name",
        "ads", "idc", "turnover_grade",
        "available_stock_count", "valid_stock_count",
        "transit_stock_count", "expiring_stock_count",
        "requested_stock_count", "stock_defect_stock_count",
        "return_from_customer_stock_count", "return_to_seller_stock_count",
        "waiting_docs_stock_count", "other_stock_count",
    ],
    "ozon_perf_campaigns": [
        "id", "title", "state", "advObjectType", "fromDate", "toDate",
        "dailyBudget", "budget", "createdAt", "updatedAt",
    ],
    "ozon_perf_statistics": [
        "date", "campaign_id", "campaign_title", "views", "clicks",
        "moneySpent", "ordersMoney", "orders",
    ],
}

OZON_FIELD_LABELS: dict = {
    # Product
    "product_id":       "ID товара",
    "offer_id":         "Артикул продавца",
    "name":             "Название",
    "barcode":          "Штрихкод",
    "category_id":      "ID категории",
    "marketing_price":  "Маркетинговая цена",
    "min_price":        "Минимальная цена",
    "old_price":        "Старая цена",
    "price":            "Цена",
    "vat":              "НДС",
    "is_discounted":    "Уценённый",
    "is_kgt":           "КГТ",
    "archived":         "В архиве",
    "created_at":       "Дата создания",
    "primary_image":    "Главное изображение",
    "images_count":     "Кол-во изображений",
    "weight":           "Вес",
    # Stock
    "sku":              "SKU",
    "warehouse_name":   "Склад",
    "present":          "В наличии",
    "reserved":         "Зарезервировано",
    # Posting
    "posting_number":   "Номер отправления",
    "order_id":         "ID заказа",
    "order_number":     "Номер заказа",
    "status":           "Статус",
    "substatus":        "Подстатус",
    "in_process_at":    "Дата заказа",
    "shipment_date":    "Дата отгрузки",
    "delivery_method_name": "Способ доставки",
    "tracking_number":  "Трек-номер",
    "is_express":       "Экспресс",
    "product_sku":      "SKU товара",
    "product_offer_id": "Артикул товара",
    "product_name":     "Название товара",
    "product_price":    "Цена товара",
    "product_quantity": "Количество",
    "product_currency_code": "Валюта",
    "delivery_price":   "Стоимость доставки",
    "delivery_charge":  "Стоимость доставки (нач.)",
    "commission_amount":"Комиссия",
    "payout":           "К выплате",
    # Returns
    "id":               "ID",
    "quantity":         "Количество",
    "status_name":      "Статус возврата",
    "return_reason_name": "Причина возврата",
    "accepted_from_customer_moment": "Принят от покупателя",
    "returned_to_ozon_moment":       "Возвращён на Ozon",
    # Finance
    "operation_id":     "ID операции",
    "operation_type":   "Тип операции (код)",
    "operation_type_name": "Тип операции",
    "operation_date":   "Дата операции",
    "type":             "Категория",
    "delivery_schema":  "Схема (FBO/FBS)",
    "amount":           "Сумма",
    "accruals_for_sale":"Начисления за товар",
    "sale_commission":  "Комиссия Ozon",
    "return_delivery_charge": "Возвратная доставка",
    "services_count":   "Кол-во услуг",
    "services_total":   "Сумма услуг",
    "services_summary": "Услуги (детально)",
    # Analytics
    "date":             "Дата",
    "revenue":          "Выручка",
    "ordered_units":    "Заказано шт.",
    "returns":          "Возвраты",
    "hits_view_search": "Показы в поиске",
    "hits_view_pdp":    "Показы карточки",
    "hits_view":        "Показы всего",
    "hits_tocart_search":"В корзину из поиска",
    "hits_tocart_pdp":  "В корзину с карточки",
    "hits_tocart":      "В корзину всего",
    "session_view_search":"Сессии в поиске",
    "session_view_pdp": "Сессии на карточке",
    "session_view":     "Сессии всего",
    "conv_tocart_search":"Конверсия в корзину (поиск)",
    "conv_tocart_pdp":  "Конверсия в корзину (карточка)",
    "conv_tocart":      "Конверсия в корзину",
    "delivered_units":  "Доставлено шт.",
    "cancellations":    "Отмены",
    "ads":              "Средние продажи в день",
    "idc":              "Дней до окончания",
    "cluster_name":     "Кластер",
    "turnover_grade":   "Грейд оборачиваемости",
    "available_stock_count":   "Доступно",
    "valid_stock_count":       "Годных",
    "transit_stock_count":     "В пути",
    "expiring_stock_count":    "Истекают",
    "requested_stock_count":   "Запрошено к поставке",
    "stock_defect_stock_count":"Брак",
    "return_from_customer_stock_count": "Возврат от покупателя",
    "return_to_seller_stock_count":     "Возврат продавцу",
    "waiting_docs_stock_count":"Ожидают документов",
    "other_stock_count":       "Прочее",
    # Performance campaigns
    "title":            "Название",
    "state":            "Статус",
    "advObjectType":    "Тип объекта",
    "fromDate":         "С даты",
    "toDate":           "По дату",
    "dailyBudget":      "Дневной бюджет",
    "budget":           "Бюджет",
    "createdAt":        "Создана",
    "updatedAt":        "Обновлена",
    # Performance statistics
    "campaign_id":      "ID кампании",
    "campaign_title":   "Название кампании",
    "views":            "Показы",
    "clicks":           "Клики",
    "moneySpent":       "Расход",
    "ordersMoney":      "Сумма заказов",
    "orders":           "Заказов",
}

# ── Helpers ────────────────────────────────────────────────────────────────
def _ozon_accounts_list(config: dict) -> list:
    return ((config.get("ozon") or {}).get("accounts")) or []

def _ozon_account_by_name(config: dict, name: str) -> Optional[dict]:
    for a in _ozon_accounts_list(config):
        if a.get("name") == name:
            return a
    return None

def _ozon_iso(date_str: str, end_of_day: bool = False) -> Optional[str]:
    """'2026-01-15' → '2026-01-15T00:00:00.000Z' (or 23:59:59.999Z)."""
    if not date_str:
        return None
    suffix = "T23:59:59.999Z" if end_of_day else "T00:00:00.000Z"
    return f"{date_str}{suffix}"

def _ozon_seller_request(account: dict, method: str, path: str,
                         body=None, params=None, timeout: int = 180) -> dict:
    """Call Ozon Seller API. Raises with readable message on HTTP error."""
    if not account.get("client_id") or not account.get("api_key"):
        raise Exception("Не заполнены Client-Id или Api-Key аккаунта Ozon")
    headers = {
        "Client-Id":   str(account["client_id"]).strip(),
        "Api-Key":     str(account["api_key"]).strip(),
        "Content-Type": "application/json",
    }
    url = OZON_API_BASE + path
    resp = requests.request(method, url, headers=headers, json=body,
                            params=params, timeout=timeout)
    if resp.status_code >= 400:
        try:
            err = resp.json()
            msg = err.get("message") or err.get("error") or json.dumps(err)[:300]
        except Exception:
            msg = resp.text[:300]
        raise Exception(f"Ozon API {resp.status_code}: {msg}")
    return resp.json()

# Performance API token cache: {account_name: (token, expires_at)}
_OZON_PERF_TOKENS: dict = {}

def _ozon_perf_token(account: dict) -> str:
    name = account.get("name", "")
    cached = _OZON_PERF_TOKENS.get(name)
    if cached and cached[1] > datetime.now() + timedelta(seconds=30):
        return cached[0]
    cid = (account.get("perf_client_id") or "").strip()
    sec = (account.get("perf_secret") or "").strip()
    if not cid or not sec:
        raise Exception("Performance API ключи не заполнены для этого аккаунта")
    resp = requests.post(
        f"{OZON_PERF_BASE}/api/client/token",
        json={"client_id": cid, "client_secret": sec, "grant_type": "client_credentials"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise Exception(f"Performance auth failed: HTTP {resp.status_code} — {resp.text[:200]}")
    data  = resp.json()
    token = data.get("access_token", "")
    ttl   = int(data.get("expires_in") or 1800)
    _OZON_PERF_TOKENS[name] = (token, datetime.now() + timedelta(seconds=ttl))
    return token

def _ozon_perf_request(account: dict, method: str, path: str,
                       body=None, params=None, timeout: int = 180) -> dict:
    token   = _ozon_perf_token(account)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url     = OZON_PERF_BASE + path
    resp    = requests.request(method, url, headers=headers, json=body,
                               params=params, timeout=timeout)
    if resp.status_code >= 400:
        raise Exception(f"Performance API {resp.status_code}: {resp.text[:300]}")
    return resp.json()

# ── Entity fetchers ────────────────────────────────────────────────────────
# Each returns BI-style raw: [headers, row, row, ...]. They yield progress
# events via the wrapping iterator — actual paging/chunking happens here.

def _ozon_fetch_product(account: dict) -> list:
    headers = ["product_id", "offer_id", "name", "barcode", "category_id",
               "marketing_price", "min_price", "old_price", "price",
               "vat", "is_discounted", "is_kgt", "archived",
               "created_at", "primary_image", "images_count", "weight"]
    rows: list = []
    last_id = ""
    while True:
        body = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        data = _ozon_seller_request(account, "POST", "/v3/product/list", body=body)
        result = data.get("result") or {}
        items  = result.get("items") or []
        if not items:
            break
        ids = [it.get("product_id") for it in items if it.get("product_id")]
        info = _ozon_seller_request(account, "POST", "/v3/product/info/list",
                                    body={"product_id": ids})
        info_items = (info.get("result") or {}).get("items") \
                     or (info.get("items") or [])
        info_by_id = {(it.get("id") or it.get("product_id")): it for it in info_items}
        for it in items:
            full = info_by_id.get(it.get("product_id")) or {}
            mp = full.get("marketing_price")
            if isinstance(mp, dict):
                mp = mp.get("price")
            barcodes = full.get("barcodes") or ([full.get("barcode")] if full.get("barcode") else [])
            images   = full.get("images") or []
            rows.append([
                it.get("product_id"), it.get("offer_id"), full.get("name", ""),
                ",".join(str(x) for x in barcodes if x),
                full.get("category_id") or full.get("description_category_id"),
                mp, full.get("min_price"), full.get("old_price"), full.get("price"),
                full.get("vat"), full.get("is_discounted"), full.get("is_kgt"),
                full.get("archived"), full.get("created_at"),
                full.get("primary_image", ""), len(images),
                full.get("volume_weight") or full.get("weight"),
            ])
        last_id = result.get("last_id") or ""
        if not last_id:
            break
    return [headers] + rows

def _ozon_fetch_stock(account: dict) -> list:
    headers = ["product_id", "offer_id", "sku", "warehouse_name",
               "present", "reserved"]
    rows: list = []
    cursor = ""
    while True:
        body = {"cursor": cursor, "limit": 1000, "filter": {"visibility": "ALL"}}
        data = _ozon_seller_request(account, "POST", "/v4/product/info/stocks", body=body)
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            offer  = it.get("offer_id")
            pid    = it.get("product_id")
            for s in (it.get("stocks") or []):
                rows.append([
                    pid, offer, s.get("sku"),
                    s.get("warehouse_name") or s.get("type") or "",
                    s.get("present"), s.get("reserved"),
                ])
        cursor = data.get("cursor") or ""
        if not cursor or len(items) < 1000:
            break
    return [headers] + rows

def _ozon_fetch_posting_fbs(account: dict, start: str, end: str) -> list:
    return _ozon_fetch_posting(account, start, end, fbo=False)

def _ozon_fetch_posting_fbo(account: dict, start: str, end: str) -> list:
    return _ozon_fetch_posting(account, start, end, fbo=True)

def _ozon_fetch_posting(account: dict, start: str, end: str, fbo: bool) -> list:
    """One row per (posting × product). Chunks period by 30 days for safety."""
    headers = [
        "posting_number", "order_id", "order_number", "status", "substatus",
        "in_process_at", "shipment_date", "delivery_method_name", "warehouse_name",
        "tracking_number", "is_express",
        "product_sku", "product_offer_id", "product_name",
        "product_price", "product_quantity", "product_currency_code",
        "delivery_price", "commission_amount", "payout",
    ]
    rows: list = []
    path = "/v2/posting/fbo/list" if fbo else "/v3/posting/fbs/list"
    cur  = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=29), end_dt)
        offset = 0
        limit  = 1000
        while True:
            body = {
                "dir": "ASC",
                "filter": {
                    "since": _ozon_iso(cur.strftime("%Y-%m-%d"), False),
                    "to":    _ozon_iso(chunk_end.strftime("%Y-%m-%d"), True),
                },
                "limit":  limit,
                "offset": offset,
                "with":   {"analytics_data": True, "financial_data": True},
            }
            data = _ozon_seller_request(account, "POST", path, body=body)
            result = data.get("result") or {}
            postings = result if isinstance(result, list) else (result.get("postings") or [])
            if not postings:
                break
            for p in postings:
                dm = p.get("delivery_method") or {}
                fin = p.get("financial_data") or {}
                fin_products = {fp.get("product_id"): fp for fp in (fin.get("products") or [])}
                base = [
                    p.get("posting_number"), p.get("order_id"), p.get("order_number"),
                    p.get("status"), p.get("substatus"),
                    p.get("in_process_at"), p.get("shipment_date"),
                    dm.get("name", ""), dm.get("warehouse", ""),
                    p.get("tracking_number"), p.get("is_express"),
                ]
                for prod in (p.get("products") or []):
                    fp = fin_products.get(prod.get("product_id")) or {}
                    rows.append(base + [
                        prod.get("sku"), prod.get("offer_id"), prod.get("name"),
                        prod.get("price"), prod.get("quantity"),
                        prod.get("currency_code"),
                        fp.get("delivery_price"), fp.get("commission_amount"),
                        fp.get("payout"),
                    ])
            if len(postings) < limit:
                break
            offset += limit
        cur = chunk_end + timedelta(days=1)
    return [headers] + rows

def _ozon_fetch_finance_transaction(account: dict, start: str, end: str) -> list:
    headers = [
        "operation_id", "operation_type", "operation_type_name", "operation_date",
        "type", "posting_number", "delivery_schema",
        "amount", "accruals_for_sale", "sale_commission",
        "delivery_charge", "return_delivery_charge",
        "services_count", "services_total", "services_summary",
    ]
    rows: list = []
    cur    = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=29), end_dt)
        page = 1
        page_size = 1000
        while True:
            body = {
                "filter": {
                    "date": {
                        "from": _ozon_iso(cur.strftime("%Y-%m-%d"), False),
                        "to":   _ozon_iso(chunk_end.strftime("%Y-%m-%d"), True),
                    },
                    "operation_type":  [],
                    "posting_number":  "",
                    "transaction_type": "all",
                },
                "page":      page,
                "page_size": page_size,
            }
            data = _ozon_seller_request(account, "POST",
                                        "/v3/finance/transaction/list", body=body)
            result = data.get("result") or {}
            ops    = result.get("operations") or []
            if not ops:
                break
            for op in ops:
                posting  = op.get("posting") or {}
                services = op.get("services") or []
                services_total = 0.0
                try:
                    services_total = sum(float(s.get("price") or 0) for s in services)
                except Exception:
                    pass
                services_summary = "; ".join(
                    f"{s.get('name','')}={s.get('price','')}" for s in services
                )[:1500]
                rows.append([
                    op.get("operation_id"), op.get("operation_type"),
                    op.get("operation_type_name"), op.get("operation_date"),
                    op.get("type"),
                    posting.get("posting_number"), posting.get("delivery_schema"),
                    op.get("amount"), op.get("accruals_for_sale"),
                    op.get("sale_commission"), op.get("delivery_charge"),
                    op.get("return_delivery_charge"),
                    len(services), services_total, services_summary,
                ])
            if len(ops) < page_size:
                break
            page += 1
        cur = chunk_end + timedelta(days=1)
    return [headers] + rows

def _ozon_fetch_returns_fbs(account: dict) -> list:
    return _ozon_fetch_returns(account, fbo=False)

def _ozon_fetch_returns_fbo(account: dict) -> list:
    return _ozon_fetch_returns(account, fbo=True)

def _ozon_fetch_returns(account: dict, fbo: bool) -> list:
    headers = ["id", "posting_number", "order_id", "sku", "offer_id", "name",
               "quantity", "price", "currency_code", "status_name", "return_reason_name",
               "schema", "place_name", "moved_to_place_moment", "returned_to_ozon_moment"]
    rows: list = []
    scheme = "FBO" if fbo else "FBS"
    last_id = 0
    limit  = 500
    while True:
        body = {
            "filter": {"logistic_scheme": scheme},
            "limit": limit,
            "last_id": last_id,
        }
        data = _ozon_seller_request(account, "POST", "/v1/returns/list", body=body)
        items = data.get("returns") or []
        if not items:
            break
        for r in items:
            product = r.get("product") or {}
            price_obj = product.get("price") or {}
            if isinstance(price_obj, dict):
                price_val = price_obj.get("price") or price_obj.get("amount") or ""
                currency  = price_obj.get("currency_code") or ""
            else:
                price_val = price_obj
                currency  = ""
            visual = r.get("visual") or {}
            status = visual.get("status") if isinstance(visual, dict) else {}
            status_name = ""
            if isinstance(status, dict):
                status_name = status.get("display_name") or status.get("sys_name") or ""
            place = r.get("place") or {}
            place_name = place.get("name") if isinstance(place, dict) else ""
            logistic = r.get("logistic") or {}
            moved_moment = ""
            if isinstance(logistic, dict):
                moved = logistic.get("technical_return_moment") or {}
                if isinstance(moved, dict):
                    moved_moment = moved.get("moment") or ""
            rows.append([
                r.get("id"), r.get("posting_number"), r.get("order_id"),
                product.get("sku"), product.get("offer_id"), product.get("name"),
                product.get("quantity"), price_val, currency,
                status_name,
                r.get("return_reason_name") or "",
                r.get("schema") or scheme,
                place_name,
                moved_moment,
                r.get("returned_to_ozon_moment") or "",
            ])
            rid = r.get("id")
            if isinstance(rid, int) and rid > last_id:
                last_id = rid
        if not data.get("has_next"):
            break
        if len(items) < limit:
            break
    return [headers] + rows

OZON_ANALYTICS_DEFAULT_METRICS = [
    "revenue", "ordered_units", "returns", "delivered_units", "cancellations",
    "hits_view", "hits_view_search", "hits_view_pdp",
    "hits_tocart", "hits_tocart_search", "hits_tocart_pdp",
    "session_view", "session_view_search", "conv_tocart",
]

# Now that metrics list exists, fill the analytics_data fields entry
OZON_ENTITY_FIELDS["ozon_analytics_data"] = ["date", "sku"] + OZON_ANALYTICS_DEFAULT_METRICS

def _ozon_fetch_analytics_data(account: dict, start: str, end: str) -> list:
    headers = ["date", "sku"] + OZON_ANALYTICS_DEFAULT_METRICS
    rows: list = []
    body = {
        "date_from": start, "date_to": end,
        "dimension": ["day", "sku"],
        "metrics":   OZON_ANALYTICS_DEFAULT_METRICS,
        "limit":     1000, "offset": 0,
    }
    while True:
        data = _ozon_seller_request(account, "POST", "/v1/analytics/data", body=body)
        result = (data.get("result") or {})
        items  = result.get("data") or []
        if not items:
            break
        for it in items:
            dims = it.get("dimensions") or []
            day  = dims[0].get("id") if len(dims) > 0 else ""
            sku  = dims[1].get("id") if len(dims) > 1 else ""
            metrics = it.get("metrics") or []
            rows.append([day, sku] + list(metrics) + [None] * (len(OZON_ANALYTICS_DEFAULT_METRICS) - len(metrics)))
        if len(items) < body["limit"]:
            break
        body["offset"] += body["limit"]
    return [headers] + rows

def _ozon_fetch_analytics_stocks(account: dict) -> list:
    """
    /v1/analytics/stocks requires explicit SKU list (1..100 per call).
    Step 1 — collect all SKUs via /v4/product/info/stocks.
    Step 2 — call analytics/stocks in chunks of 100 and aggregate.
    """
    headers = [
        "sku", "offer_id", "name",
        "cluster_name", "warehouse_name",
        "ads", "idc", "turnover_grade",
        "available_stock_count", "valid_stock_count",
        "transit_stock_count", "expiring_stock_count",
        "requested_stock_count", "stock_defect_stock_count",
        "return_from_customer_stock_count", "return_to_seller_stock_count",
        "waiting_docs_stock_count", "other_stock_count",
    ]
    rows: list = []

    # Collect SKUs (deduplicated) from product stocks
    sku_set: set = set()
    cursor = ""
    while True:
        body = {"cursor": cursor, "limit": 1000, "filter": {"visibility": "ALL"}}
        data = _ozon_seller_request(account, "POST", "/v4/product/info/stocks", body=body)
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            for s in (it.get("stocks") or []):
                sku = s.get("sku")
                if sku:
                    sku_set.add(str(sku))
        cursor = data.get("cursor") or ""
        if not cursor or len(items) < 1000:
            break
    all_skus = list(sku_set)
    if not all_skus:
        return [headers]

    for i in range(0, len(all_skus), 100):
        chunk = all_skus[i:i + 100]
        try:
            data = _ozon_seller_request(
                account, "POST", "/v1/analytics/stocks",
                body={"skus": chunk}
            )
        except Exception as exc:
            logger.warning("analytics/stocks chunk %d failed: %s", i, exc)
            continue
        items = data.get("items") or []
        for it in items:
            rows.append([
                it.get("sku"), it.get("offer_id"), it.get("name"),
                it.get("cluster_name") or "",
                it.get("warehouse_name") or "",
                it.get("ads"), it.get("idc"), it.get("turnover_grade"),
                it.get("available_stock_count"), it.get("valid_stock_count"),
                it.get("transit_stock_count"), it.get("expiring_stock_count"),
                it.get("requested_stock_count"),
                it.get("stock_defect_stock_count"),
                it.get("return_from_customer_stock_count"),
                it.get("return_to_seller_stock_count"),
                it.get("waiting_docs_stock_count"),
                it.get("other_stock_count"),
            ])
    return [headers] + rows

def _ozon_fetch_perf_campaigns(account: dict) -> list:
    headers = ["id", "title", "state", "advObjectType", "fromDate", "toDate",
               "dailyBudget", "budget", "createdAt", "updatedAt"]
    rows: list = []
    data  = _ozon_perf_request(account, "GET", "/api/client/campaign")
    items = data.get("list") or []
    for c in items:
        rows.append([
            c.get("id"), c.get("title"), c.get("state"),
            c.get("advObjectType"), c.get("fromDate"), c.get("toDate"),
            c.get("dailyBudget"), c.get("budget"),
            c.get("createdAt"), c.get("updatedAt"),
        ])
    return [headers] + rows

def _ozon_fetch_perf_statistics(account: dict, start: str, end: str) -> list:
    """Daily statistics by campaign.

    Ozon Performance API — асинхронный flow:
      1) POST /api/client/statistics   -> {UUID}
      2) GET  /api/client/statistics/{UUID}   (polling до state=OK/ERROR)
      3) GET  /api/client/statistics/report?UUID=...   (CSV/JSON отчёт)
    """
    headers = ["date", "campaign_id", "campaign_title", "views", "clicks",
               "moneySpent", "ordersMoney", "orders"]
    rows: list = []

    camp_data = _ozon_perf_request(account, "GET", "/api/client/campaign")
    camps = camp_data.get("list") or []
    if not camps:
        return [headers]
    title_by_id = {str(c.get("id")): c.get("title", "") for c in camps}
    cids_all = [str(c.get("id")) for c in camps]

    # Performance API ждёт RFC3339 timestamp, не просто YYYY-MM-DD.
    def _to_ts(d: str, end_of_day: bool) -> str:
        if not d:
            return d
        if "T" in d:
            return d
        return f"{d}T23:59:59Z" if end_of_day else f"{d}T00:00:00Z"

    start_ts = _to_ts(start, end_of_day=False)
    end_ts   = _to_ts(end,   end_of_day=True)

    import time as _time
    CHUNK = 10  # Ozon: максимум 10 кампаний за один запрос отчёта

    # Собираем сырой текст отчётов со всех чанков, потом парсим единым проходом.
    chunk_payloads: list = []  # [(content_type, text), ...]

    for chunk_start in range(0, len(cids_all), CHUNK):
        cids = cids_all[chunk_start:chunk_start + CHUNK]
        body = {
            "campaigns": cids,
            "from":      start_ts,
            "to":        end_ts,
            "groupBy":   "DATE",
        }
        # Ozon разрешает только один активный отчёт. При 429 ждём и повторяем.
        submit = None
        for retry in range(20):
            try:
                submit = _ozon_perf_request(account, "POST", "/api/client/statistics/json", body=body)
                break
            except Exception as exc:
                msg = str(exc)
                if "429" in msg:
                    _time.sleep(10)
                    continue
                logger.warning("perf statistics submit failed (chunk %d): %s", chunk_start, exc)
                submit = None
                break
        if not submit:
            continue
        uuid = submit.get("UUID") or submit.get("uuid")
        if not uuid:
            logger.warning("perf statistics: no UUID in response: %s", str(submit)[:200])
            continue

        # Поллим. 404 во время ожидания — это "ещё не готов", не выход.
        state = ""
        for _ in range(120):  # до ~10 минут на чанк
            _time.sleep(5)
            try:
                status = _ozon_perf_request(account, "GET", f"/api/client/statistics/{uuid}")
            except Exception as exc:
                if "404" in str(exc):
                    continue
                continue
            state = (status.get("state") or status.get("status") or "").upper()
            if state in ("OK", "DONE", "READY", "SUCCESS"):
                break
            if state in ("ERROR", "CANCELLED", "FAILED"):
                logger.warning("perf statistics report %s: state=%s", uuid, state)
                state = ""
                break
        if state not in ("OK", "DONE", "READY", "SUCCESS"):
            logger.warning("perf statistics report %s timed out (chunk %d)", uuid, chunk_start)
            continue

        token = _ozon_perf_token(account)
        try:
            resp = requests.get(
                f"{OZON_PERF_BASE}/api/client/statistics/report",
                headers={"Authorization": f"Bearer {token}"},
                params={"UUID": uuid},
                timeout=180,
            )
        except Exception as exc:
            logger.warning("perf statistics report download failed: %s", exc)
            continue
        if resp.status_code >= 400:
            logger.warning("perf statistics report HTTP %d: %s", resp.status_code, resp.text[:200])
            continue
        chunk_payloads.append(((resp.headers.get("Content-Type") or "").lower(),
                               resp.text or ""))

    if not chunk_payloads:
        return [headers]

    def _num(v):
        if v in (None, ""):
            return None
        try:
            return float(str(v).replace(",", ".").replace(" ", ""))
        except Exception:
            return v

    import json as _json

    def _pick(d: dict, *keys):
        for k in keys:
            if isinstance(d, dict) and d.get(k) not in (None, ""):
                return d.get(k)
        return None

    def _norm_date(v):
        # Ozon Performance отдаёт даты в формате DD.MM.YYYY. Приводим к ISO.
        if not v:
            return v
        s = str(v).strip()
        m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return s

    for ci, (ctype, text) in enumerate(chunk_payloads):
        stripped = (text or "").lstrip()
        if not stripped:
            logger.warning("perf statistics chunk %d: empty body (ctype=%s)", ci, ctype)
            continue
        try:
            data = _json.loads(stripped)
        except Exception as exc:
            logger.warning("perf statistics: JSON parse failed (ctype=%s): %s; head=%r",
                           ctype, exc, stripped[:200])
            continue

        # Реальный формат Ozon: top-level dict, где ключ = campaign_id,
        # значение = {"title", "report": {"rows": [...], "totals": {...}}}.
        # Также подстраховываемся под устаревшие варианты ниже.
        items = []
        if isinstance(data, dict):
            # cначала пробуем "campaign_id -> obj" формат
            for k, v in data.items():
                if isinstance(v, dict) and ("report" in v or "rows" in v or "totals" in v):
                    items.append((str(k), v))
            # fallback: старые варианты
            if not items:
                for camp in (data.get("campaigns") or data.get("result") or []):
                    cid = str(camp.get("id") or camp.get("campaignId") or camp.get("campaign_id") or "")
                    items.append((cid, camp))

        for cid, camp in items:
            title = title_by_id.get(cid, camp.get("title", ""))
            report = camp.get("report") if isinstance(camp.get("report"), dict) else {}
            entries = (report.get("rows") if isinstance(report, dict) else None) \
                   or camp.get("rows") or []
            for entry in entries:
                date_v = _norm_date(_pick(entry, "date", "day", "Date", "period", "dt",
                                          "dateFrom", "date_from", "Day"))
                rows.append([
                    date_v,
                    cid, title,
                    _num(_pick(entry, "views", "showsCount", "shows")),
                    _num(_pick(entry, "clicks", "clicksCount")),
                    _num(_pick(entry, "moneySpent", "money_spent", "spent")),
                    _num(_pick(entry, "ordersMoney", "orders_money", "revenue")),
                    _num(_pick(entry, "orders", "ordersCount")),
                ])
            # Если построчных нет — кладём totals как единственную строку с датой == start.
            # Так хотя бы видно бюджет/показы/клики по кампании за период.
            if not entries:
                totals = report.get("totals") if isinstance(report, dict) else None
                if isinstance(totals, dict) and any(totals.values()):
                    rows.append([
                        start, cid, title,
                        _num(_pick(totals, "views", "shows", "showsCount")),
                        _num(_pick(totals, "clicks", "clicksCount")),
                        _num(_pick(totals, "moneySpent", "money_spent", "spent")),
                        _num(_pick(totals, "ordersMoney", "orders_money", "revenue")),
                        _num(_pick(totals, "orders", "ordersCount")),
                    ])
    return [headers] + rows

# ── Output column filter ──────────────────────────────────────────────────
def _ozon_filter_columns(raw: list, fields: Optional[list]) -> list:
    """Keep only requested columns (preserving their order). Empty = keep all.
    Unknown fields are silently ignored."""
    if not raw or not fields:
        return raw
    headers = raw[0]
    keep_idx = []
    new_headers = []
    seen = set()
    for f in fields:
        if f in seen:
            continue
        seen.add(f)
        if f in headers:
            keep_idx.append(headers.index(f))
            new_headers.append(f)
    if not keep_idx:
        return raw  # nothing recognised → return all
    out = [new_headers]
    for r in raw[1:]:
        out.append([r[i] if i < len(r) else None for i in keep_idx])
    return out

# ── Dispatcher ─────────────────────────────────────────────────────────────
def _ozon_fetch_dispatch(account: dict, entity: str,
                         start: str, end: str,
                         fields: Optional[list] = None) -> list:
    if entity == "ozon_product":              raw = _ozon_fetch_product(account)
    elif entity == "ozon_stock":              raw = _ozon_fetch_stock(account)
    elif entity == "ozon_posting_fbs":        raw = _ozon_fetch_posting_fbs(account, start, end)
    elif entity == "ozon_posting_fbo":        raw = _ozon_fetch_posting_fbo(account, start, end)
    elif entity == "ozon_returns_fbs":        raw = _ozon_fetch_returns_fbs(account)
    elif entity == "ozon_returns_fbo":        raw = _ozon_fetch_returns_fbo(account)
    elif entity == "ozon_finance_transaction":raw = _ozon_fetch_finance_transaction(account, start, end)
    elif entity == "ozon_analytics_data":     raw = _ozon_fetch_analytics_data(account, start, end)
    elif entity == "ozon_analytics_stocks":   raw = _ozon_fetch_analytics_stocks(account)
    elif entity == "ozon_perf_campaigns":     raw = _ozon_fetch_perf_campaigns(account)
    elif entity == "ozon_perf_statistics":    raw = _ozon_fetch_perf_statistics(account, start, end)
    else: raise Exception(f"Неизвестная сущность Ozon: {entity}")
    return _ozon_filter_columns(raw, fields)

# ── Async event iterator (mirrors Bitrix _export_event_iter) ───────────────
async def _ozon_export_event_iter(config: dict, account_name: str, entity: str,
                                  date_field: str, start_date: str, end_date: str,
                                  fields: Optional[list] = None):
    account = _ozon_account_by_name(config, account_name)
    if not account:
        yield {"status": "error", "error": f"Аккаунт Ozon '{account_name}' не найден"}
        return
    yield {"status": "info", "message": f"Аккаунт: {account_name} • сущность: {entity}"}

    has_period = bool(date_field and start_date and end_date)
    try:
        if has_period:
            yield {"status": "info",
                   "message": f"Период: {start_date} → {end_date} (по {date_field})"}
        else:
            yield {"status": "info", "message": "Без фильтра по дате (полная выгрузка)"}
        if fields:
            preview = ", ".join(fields[:8]) + ("..." if len(fields) > 8 else "")
            yield {"status": "info", "message": f"Поля ({len(fields)}): {preview}"}

        # Run blocking fetcher in a thread, with periodic heartbeats.
        _task = asyncio.ensure_future(asyncio.to_thread(
            _ozon_fetch_dispatch, account, entity,
            start_date or "", end_date or "", fields
        ))
        while True:
            try:
                await asyncio.wait_for(asyncio.shield(_task), timeout=20.0)
                break
            except asyncio.TimeoutError:
                yield {"status": "info", "message": "Загрузка из Ozon..."}
        raw = _task.result()
        bi_n = (len(raw) - 1) if raw and isinstance(raw, list) else 0
        yield {"status": "info", "message": f"Получено строк от Ozon: {bi_n}"}

        # Push to ClickHouse
        _ch_task = asyncio.ensure_future(
            asyncio.to_thread(push_to_clickhouse, config, entity, raw))
        while True:
            try:
                await asyncio.wait_for(asyncio.shield(_ch_task), timeout=20.0)
                break
            except asyncio.TimeoutError:
                yield {"status": "info", "message": "Запись в ClickHouse..."}
        rows = _ch_task.result()
        yield {"status": "done", "rows": rows, "total": rows}
    except Exception as exc:
        yield {"status": "error", "error": str(exc)}

# ── Endpoints ──────────────────────────────────────────────────────────────
@app.get("/api/ozon/entities")
async def api_ozon_entities():
    return OZON_ENTITIES

@app.get("/api/ozon/date-field-labels")
async def api_ozon_date_labels():
    return OZON_DATE_LABELS

@app.get("/api/ozon/entity-fields")
async def api_ozon_entity_fields(entity: str):
    """Return available columns for an Ozon entity, with Russian labels."""
    fields = OZON_ENTITY_FIELDS.get(entity, [])
    return {
        "fields": fields,
        "labels": {f: OZON_FIELD_LABELS.get(f, f) for f in fields},
    }

@app.get("/api/ozon/accounts")
async def api_ozon_accounts():
    """Return accounts WITHOUT secrets — for safe display in UI lists."""
    out = []
    for a in _ozon_accounts_list(load_config()):
        out.append({
            "name":            a.get("name", ""),
            "client_id":       a.get("client_id", ""),
            "has_api_key":     bool(a.get("api_key")),
            "perf_client_id":  a.get("perf_client_id", ""),
            "has_perf_secret": bool(a.get("perf_secret")),
        })
    return out

@app.get("/api/ozon/accounts/{name}")
async def api_ozon_account_get(name: str):
    """Full account incl. secrets — used by edit dialog only."""
    a = _ozon_account_by_name(load_config(), name)
    if not a:
        raise HTTPException(404, f"Аккаунт '{name}' не найден")
    return a

@app.post("/api/ozon/accounts")
async def api_ozon_account_save(data: dict):
    """Add or update one account (by name). Body: {name, client_id, api_key,
    perf_client_id?, perf_secret?, original_name?}.
    If original_name differs from name → rename.
    Empty api_key/perf_secret keeps existing value (so UI can show '••• сохранено')."""
    name      = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Укажите название аккаунта")
    client_id = (data.get("client_id") or "").strip()
    api_key   = (data.get("api_key")   or "").strip()
    perf_cid  = (data.get("perf_client_id") or "").strip()
    perf_sec  = (data.get("perf_secret")    or "").strip()
    original  = (data.get("original_name") or name).strip()

    config   = load_config()
    accounts = _ozon_accounts_list(config)

    existing = next((a for a in accounts if a.get("name") == original), None)
    if not existing and original != name:
        raise HTTPException(404, f"Аккаунт '{original}' не найден")

    if name != original and any(a.get("name") == name for a in accounts):
        raise HTTPException(409, f"Аккаунт '{name}' уже существует")

    if existing:
        existing["name"]      = name
        existing["client_id"] = client_id
        if api_key:
            existing["api_key"] = api_key
        existing["perf_client_id"] = perf_cid
        if perf_sec:
            existing["perf_secret"] = perf_sec
    else:
        accounts.append({
            "name":           name,
            "client_id":      client_id,
            "api_key":        api_key,
            "perf_client_id": perf_cid,
            "perf_secret":    perf_sec,
        })

    config.setdefault("ozon", {})["accounts"] = accounts
    save_config(config)
    # Also rename references in saved_configs / schedule
    if existing and original != name:
        for sc in config.get("saved_configs", []):
            if sc.get("provider") == "ozon" and sc.get("ozon_account") == original:
                sc["ozon_account"] = name
        save_config(config)
    return {"status": "ok"}

@app.delete("/api/ozon/accounts/{name}")
async def api_ozon_account_delete(name: str):
    config   = load_config()
    accounts = _ozon_accounts_list(config)
    new      = [a for a in accounts if a.get("name") != name]
    if len(new) == len(accounts):
        raise HTTPException(404, f"Аккаунт '{name}' не найден")
    config.setdefault("ozon", {})["accounts"] = new
    save_config(config)
    return {"status": "ok"}

@app.post("/api/ozon/test-connection")
async def api_ozon_test(data: dict):
    name    = (data.get("name") or "").strip()
    config  = load_config()
    account = _ozon_account_by_name(config, name)
    if not account:
        return {"seller": False, "seller_error": f"Аккаунт '{name}' не найден"}
    out: dict = {}
    # Seller API: ping with cheap call
    try:
        _ozon_seller_request(account, "POST", "/v3/product/list",
                             body={"filter": {"visibility": "ALL"}, "last_id": "", "limit": 1},
                             timeout=20)
        out["seller"] = True
    except Exception as exc:
        out["seller"] = False
        out["seller_error"] = str(exc)
    # Performance API (optional)
    if (account.get("perf_client_id") or "").strip() and (account.get("perf_secret") or "").strip():
        try:
            _ozon_perf_token(account)
            out["perf"] = True
        except Exception as exc:
            out["perf"] = False
            out["perf_error"] = str(exc)
    return out

@app.post("/api/ozon/export-stream")
async def api_ozon_export_stream(data: dict):
    config = load_config()
    account_name = (data.get("account") or "").strip()
    entity       = data.get("entity", "")
    date_field   = data.get("date_field", "")
    start_date   = data.get("start_date", "")
    end_date     = data.get("end_date", "")
    fields       = data.get("fields") or None

    async def gen():
        inner = _ozon_export_event_iter(config, account_name, entity,
                                        date_field, start_date, end_date, fields)
        async for ev in _iter_with_history(
            inner,
            source="manual_form", entity=entity, date_field=date_field,
            start_date=start_date, end_date=end_date,
            dimensions_filters=[{"fieldName": "OZON_ACCOUNT", "values": [account_name],
                                 "type": "INCLUDE", "operator": "IN_LIST"}] if account_name else [],
            fields=fields or [],
            provider="ozon",
            config_name=None,
        ):
            yield _sse(ev)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
