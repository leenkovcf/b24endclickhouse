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
            return json.load(f)
    return {
        "bitrix":     {"portal": "", "bi_key": ""},
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
    entry.setdefault("entity_name", _entity_display_name(entry.get("entity", "")))
    items.insert(0, entry)
    _save_history(items)

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

    # Use the actual table schema (not freshly inferred) so type conversions
    # stay consistent across days when column values change character (e.g.
    # CRM_PRODUCT_ID going from single int → comma-separated string).
    col_types = [existing.get(h, inferred[i]) for i, h in enumerate(headers)]

    converted = []
    for r in rows:
        padded = list(r) + [None] * (len(headers) - len(r))
        converted.append([_convert(v, t) for v, t in zip(padded, col_types)])

    client.insert(safe, converted, column_names=list(headers))
    return len(rows)

# ---------------------------------------------------------------------------
# Background export
# ---------------------------------------------------------------------------
def _do_export(data: dict, source: str = "manual_form",
               config_name: Optional[str] = None) -> None:
    global export_status
    export_status = {"running": True, "rows": 0, "error": None, "last_run": None}
    config  = load_config()
    started = datetime.now()
    rows    = 0
    error   = None
    try:
        raw_pair = fetch_from_bitrix_safe(
            config["bitrix"]["portal"],
            config["bitrix"]["bi_key"],
            data["entity"],
            data.get("date_field")         or None,
            data.get("start_date")         or None,
            data.get("end_date")           or None,
            data.get("dimensions_filters") or None,
            data.get("fields")             or None,
        )
        raw, _removed = raw_pair
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
            }, source="schedule", config_name=name)
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
        }, source="schedule")

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
                             config_name: Optional[str] = None):
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

    async def gen():
        inner = _export_event_iter(config, entity, date_field,
                                   start_date, end_date,
                                   dimensions_filters, fields)
        async for ev in _iter_with_history(
            inner,
            source="manual_form", entity=entity, date_field=date_field,
            start_date=start_date, end_date=end_date,
            dimensions_filters=dimensions_filters, fields=fields,
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
            try:
                inner = _export_event_iter(config, entity, df,
                                           start_date, end_date,
                                           dims, flds)
                async for ev in _iter_with_history(
                    inner,
                    source="manual_batch", entity=entity, date_field=df,
                    start_date=start_date, end_date=end_date,
                    dimensions_filters=dims, fields=flds,
                    config_name=name,
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
    return {
        "name":               name,
        "entity":             data.get("entity", ""),
        "date_field":         data.get("date_field", "") or "",
        "dimensions_filters": data.get("dimensions_filters") or [],
        "fields":             data.get("fields") or [],
    }

@app.get("/api/saved-configs")
async def api_get_saved_configs():
    return load_config().get("saved_configs", [])

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
    return items

@app.delete("/api/history")
async def api_clear_history():
    _save_history([])
    return {"status": "ok"}

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
