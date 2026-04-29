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

CONFIG_FILE = "config.json"
app = FastAPI(title="BI-коннектор Bitrix24 → ClickHouse")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

scheduler = BackgroundScheduler(timezone="UTC")
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
        {"code": "crm_lead_product_row",   "name": "Товары в лидах",                   "date_fields": []},
        {"code": "crm_deal",               "name": "Сделки",                           "date_fields": ["DATE_CREATE", "DATE_MODIFY", "CLOSEDATE"]},
        {"code": "crm_deal_uf",            "name": "Пользовательские поля сделок",     "date_fields": ["DATE_CREATE", "DATE_MODIFY"]},
        {"code": "crm_deal_stage_history", "name": "История статусов сделок",          "date_fields": []},
        {"code": "crm_deal_product_row",   "name": "Товары в сделках",                 "date_fields": []},
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
    "CREATED_DATE":   "Дата создания",
    "CHANGED_DATE":   "Дата изменения",
    "DEADLINE":       "Дедлайн",
    "CLOSED_DATE":    "Дата закрытия",
    "CALL_START_DATE":"Дата звонка",
    "DATE_INSERT":    "Дата создания",
    "DATE_UPDATE":    "Дата обновления",
}

SCHEDULE_LABELS = {
    "hourly": "Раз в час",
    "daily":  "Раз в сутки",
    "weekly": "Раз в неделю",
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
        "schedule":   {"enabled": False, "frequency": "daily", "entity": "", "date_field": "DATE_CREATE", "days_back": 1},
    }

def save_config(config: dict) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

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
        payload["dimensionsFilters"] = dimensions_filters
    if fields:
        payload["fields"] = fields
    if limit:
        payload["limit"] = limit
    resp = requests.post(url, params={"table": table}, json=payload, timeout=300)
    resp.raise_for_status()
    return resp.json()

def _rest(webhook: str, method: str, params: dict = None) -> dict:
    """Call Bitrix24 REST API via webhook."""
    url = f"{webhook.rstrip('/')}/{method}"
    resp = requests.get(url, params=params or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()

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

def push_to_clickhouse(config: dict, table_name: str, raw: list) -> int:
    if len(raw) < 2:
        return 0

    headers   = raw[0]
    rows      = raw[1:]
    sample_n  = min(200, len(rows))
    col_types = [
        _infer_type([rows[j][i] if i < len(rows[j]) else None for j in range(sample_n)])
        for i in range(len(headers))
    ]

    safe = re.sub(r"[^\w]", "_", table_name)
    client = get_ch_client(config)
    cols_sql = ",\n  ".join(f"`{h}` {t}" for h, t in zip(headers, col_types))
    client.command(f"""
        CREATE TABLE IF NOT EXISTS `{safe}` (
          {cols_sql}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
    """)

    converted = []
    for r in rows:
        padded = list(r) + [None] * (len(headers) - len(r))
        converted.append([_convert(v, t) for v, t in zip(padded, col_types)])

    client.insert(safe, converted, column_names=list(headers))
    return len(rows)

# ---------------------------------------------------------------------------
# Background export
# ---------------------------------------------------------------------------
def _do_export(data: dict) -> None:
    global export_status
    export_status = {"running": True, "rows": 0, "error": None, "last_run": None}
    config = load_config()
    try:
        raw  = fetch_from_bitrix(
            config["bitrix"]["portal"],
            config["bitrix"]["bi_key"],
            data["entity"],
            data.get("date_field") or None,
            data.get("start_date") or None,
            data.get("end_date")   or None,
        )
        rows = push_to_clickhouse(config, data["entity"], raw)
        export_status = {"running": False, "rows": rows, "error": None,
                         "last_run": datetime.now().strftime("%d.%m.%Y %H:%M")}
        logger.info("Export %s — %d rows", data["entity"], rows)
    except Exception as exc:
        export_status = {"running": False, "rows": 0, "error": str(exc),
                         "last_run": datetime.now().strftime("%d.%m.%Y %H:%M")}
        logger.error("Export failed: %s", exc)

def _run_scheduled() -> None:
    config = load_config()
    sch    = config.get("schedule", {})
    days   = int(sch.get("days_back", 1))
    _do_export({
        "entity":     sch.get("entity", ""),
        "date_field": sch.get("date_field", "DATE_CREATE"),
        "start_date": (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "end_date":   datetime.now().strftime("%Y-%m-%d"),
    })

def _apply_schedule(config: dict) -> None:
    if scheduler.get_job("export_job"):
        scheduler.remove_job("export_job")
    sch = config.get("schedule", {})
    if not sch.get("enabled"):
        return
    triggers = {
        "hourly": CronTrigger(minute=0),
        "daily":  CronTrigger(hour=3, minute=0),
        "weekly": CronTrigger(day_of_week="mon", hour=3, minute=0),
    }
    trigger = triggers.get(sch.get("frequency", "daily"))
    if trigger:
        scheduler.add_job(_run_scheduled, trigger, id="export_job", replace_existing=True)
        logger.info("Schedule set: %s", sch.get("frequency"))

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

@app.get("/api/crm-funnels")
async def api_crm_funnels(entity: str = "crm_deal"):
    """Return pipeline categories for deals or smart processes."""
    config = load_config()
    webhook = config["bitrix"].get("rest_webhook", "").strip().rstrip("/")
    if not webhook:
        return []
    try:
        if entity == "crm_deal":
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
        if entity == "crm_deal":
            for cat_id in ids:
                data = _rest(webhook, "crm.dealcategory.stage.list", {"id": cat_id})
                for s in data.get("result", []):
                    if s["STATUS_ID"] not in seen:
                        stages.append({"id": s["STATUS_ID"], "name": s["NAME"]})
                        seen.add(s["STATUS_ID"])
        elif entity == "crm_lead":
            data = _rest(webhook, "crm.status.list", {"filter[ENTITY_ID]": "STATUS"})
            stages = [{"id": s["STATUS_ID"], "name": s["NAME"]}
                      for s in data.get("result", [])]
        elif entity.startswith("crm_dynamic_items_") and not entity.endswith("_product_row"):
            eid  = entity.replace("crm_dynamic_items_", "").split("_")[0]
            data = _rest(webhook, "crm.item.stage.list", {"entityTypeId": eid})
            raw  = data.get("result", {}).get("stages", [])
            stages = [{"id": str(s.get("statusId", s.get("id", ""))), "name": s.get("name", "")}
                      for s in raw]
    except Exception as exc:
        logger.warning("crm-stages: %s", exc)
    return stages

@app.post("/api/export-stream")
async def api_export_stream(data: dict):
    entity             = data.get("entity", "")
    date_field         = data.get("date_field", "")
    start_date         = data.get("start_date", "")
    end_date           = data.get("end_date", "")
    dimensions_filters = data.get("dimensions_filters") or None
    fields             = data.get("fields") or None
    config             = load_config()

    async def generate():
        portal   = config["bitrix"]["portal"]
        bi_key   = config["bitrix"]["bi_key"]
        do_daily = bool(date_field and start_date and end_date)

        if not do_daily:
            try:
                yield f"data: {json.dumps({'status': 'info', 'message': 'Запрос данных из Bitrix24...'})}\n\n"
                raw  = await asyncio.to_thread(fetch_from_bitrix, portal, bi_key, entity,
                                               None, None, None, dimensions_filters, fields)
                yield f"data: {json.dumps({'status': 'info', 'message': 'Запись в ClickHouse...'})}\n\n"
                rows = await asyncio.to_thread(push_to_clickhouse, config, entity, raw)
                yield f"data: {json.dumps({'status': 'done', 'rows': rows, 'total': rows})}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'status': 'error', 'error': str(exc)})}\n\n"
            return

        current = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt  = datetime.strptime(end_date,   "%Y-%m-%d")
        total   = 0
        while current <= end_dt:
            day = current.strftime("%Y-%m-%d")
            try:
                raw  = await asyncio.to_thread(fetch_from_bitrix, portal, bi_key, entity,
                                               date_field, day, day, dimensions_filters, fields)
                rows = await asyncio.to_thread(push_to_clickhouse, config, entity, raw)
                total += rows
                yield f"data: {json.dumps({'date': day, 'rows': rows, 'total': total, 'status': 'ok'})}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'date': day, 'rows': 0, 'total': total, 'status': 'error', 'error': str(exc)})}\n\n"
            current += timedelta(days=1)

        yield f"data: {json.dumps({'status': 'done', 'total': total})}\n\n"

    return StreamingResponse(
        generate(),
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
