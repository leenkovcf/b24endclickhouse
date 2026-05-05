# claude_mind.md — память по проекту b24endclickhouse

## Назначение
Локальное FastAPI-приложение: выгружает данные из Bitrix24 (через BI-коннектор + REST webhook) и пишет их в ClickHouse. UI — одностраничник, запускается через `start.bat` на http://127.0.0.1:8000.

## Стек
- Python 3 + FastAPI + Uvicorn (`requirements.txt`)
- APScheduler (BackgroundScheduler, TZ=Europe/Moscow) — расписание
- clickhouse-connect — запись в ClickHouse
- Jinja2 + один шаблон `templates/index.html`
- Статика: `static/style.css`
- Состояние: `config.json` (создаётся в корне)

## Запуск
- `install.bat` → `pip install -r requirements.txt`
- `start.bat` → `python -m uvicorn main:app --host 127.0.0.1 --port 8000`
- Если ругается `No module named uvicorn` — пользователь не запустил `install.bat` (типичная проблема при переносе на новый ПК)

## Структура файлов
- `main.py` — весь backend (~1024 строки, всё в одном файле)
- `templates/index.html` — UI + JS (~1372 строки, всё в одном файле)
- `static/style.css` — стили (~618 строк)
- `config.json` — runtime-настройки (gitignored)

## Backend: ключевые блоки `main.py`

### Каталог сущностей
- `ENTITIES` (строки 34-95) — словарь категорий → список объектов `{code, name, date_fields}`
- `DATE_FIELD_LABELS` (97-109) — маппинг кодов дат на русские лейблы
- Кастомные сущности (смарт-процессы) хранятся в `config["custom_entities"]`, добавляются вручную или авто через `/api/smart-processes` (требуется REST webhook)

### Конфиг
- `load_config()` / `save_config()` (120-132) — JSON в `config.json`
- Дефолтная схема: `bitrix{portal,bi_key,rest_webhook}`, `clickhouse{host,port,database,username,password}`, `schedule{enabled,frequency,time_msk,entity,date_field,days_back,dimensions_filters?,fields?}`

### Bitrix24
- `fetch_from_bitrix()` (143-170) — POST на `/bitrix/tools/biconnector/pbi.php`. Принимает `dimensions_filters` (список фильтров `{fieldName, values, type, operator}`), `fields`, `limit`. Если ответ — dict, значит ошибка коннектора.
- `_rest()` / `_rest_post()` — обычные REST-вызовы webhook
- `_uf_labels_via_batch()` (186-221) — пакетный fetch русских названий пользовательских полей

### ClickHouse
- `get_ch_client()` — secure-режим если порт 8443/9440
- `_infer_type()`/`_convert()` — определение типов колонок по первым 30 значениям
- `push_to_clickhouse()` (287-321) — `CREATE TABLE IF NOT EXISTS`, типы берутся из существующей таблицы (важно: чтобы не сломать миграцию при смене формата значений)

### Экспорт
- `_do_export(data)` — фоновая выгрузка (BackgroundTasks)
- `export_status` — **глобальное состояние одной выгрузки** `{running, rows, error, last_run}`. Только одна выгрузка одновременно (см. `/api/export` — кидает 400 если уже идёт)
- `_run_scheduled()` — вызывается шедулером с диапазоном "сегодня минус days_back"
- `_apply_schedule()` — единственный job `"export_job"` пересоздаётся при сохранении расписания (ежедневно/еженедельно пн/ежемесячно 1-го)

### Streaming export — `/api/export-stream`
- POST с телом `{entity, date_field, start_date, end_date, dimensions_filters?, fields?}`
- Возвращает SSE-поток (`text/event-stream`) с событиями `info`/`ok`/`error`/`done`
- 3 спец-режима:
  1. **`crm_deal_product_row` + `DEAL_CLOSEDATE`**: двухшаговый pivot — сначала ID сделок из `crm_deal` за день, потом товары по `DEAL_ID IN [...]`
  2. **`crm_deal_uf` / `crm_deal_stage_history` с фильтром `CATEGORY_ID/STAGE_ID`**: BI-коннектор не принимает эти поля для UF-таблиц → pivot через `crm_deal` (см. `_needs_deal_id_pivot`)
  3. Обычный режим: либо целиком одним запросом (если нет date_field), либо по дням
- Каждый запрос обёрнут в `asyncio.shield` + heartbeat-таймауты по 20с — иначе клиент думает, что соединение умерло

### Воронки/стадии (REST)
- `/api/crm-funnels?entity=...` — `crm.category.list` (для сделок entityTypeId=2, для смарт-процессов извлекается из кода)
- `/api/crm-stages?entity=...&category_ids=1,2` — `crm.dealcategory.stage.list` или `crm.status.list` (для лидов) или `crm.item.stage.list` (для СП)

### Поля
- `/api/entity-fields?entity=...` — fetch первой строки BI с `limit=1`, оттуда headers
- `/api/field-labels?entity=...` — русские названия (стандартные через `crm.<entity>.fields`, UF — через batch userfield.get)

### Прочие endpoints
- `/api/test-connection` — POST `{test_bitrix?, test_clickhouse?}` → пингует оба
- `/api/connection-status` — текущие флаги (заполняются `_check_connection_on_startup`)
- `/api/export-status` — статус текущей выгрузки
- `/api/schedule` GET/POST/DELETE

## Frontend (`index.html`) — ключевые блоки JS

### State
- `ENTITIES`, `DATE_LABELS` грузятся в `boot()`
- `activeController` — `AbortController` текущего fetch streaming
- `currentFilterEntity`, `currentSchedEntity` — текущая выбранная сущность для каждой вкладки

### LocalStorage
- `LS.get/set/del` — JSON-обёртка
- Ключи: `bi_export_entity`, `bi_start_date`, `bi_end_date`, `bi_date_field_<entity>`, `bi_<groupId>_<entity>` (для funnels/stages/fields)
- Логика: если выбраны ВСЕ галочки в группе — сохраняется `null` (= "без фильтра по умолчанию"). Иначе массив выбранных.

### Навигация
- `.nav-item[data-page=...]` → переключает `.page#page-<name>` (4 страницы: export, settings, schedule, about)

### Фильтры (export и sched — две почти одинаковые ветки!)
- Воронки → стадии → поля
- Стадии перерисовываются при изменении воронок (`onFunnelChange` / `onSchedFunnelChange`)
- Два списка полей: оригинальные коды + русские названия, синхронизируются `wireSyncRuToOrig` / `wireSyncOrigToRu`
- Поиск по полям: `filterFields` / `filterSchedFields`

### `collectDimensionsFilters()` / `collectSchedDimensionsFilters()`
- Возвращают фильтры ТОЛЬКО если выбран **подмножество** галочек (не все). Все галочки = без фильтра.
- Аналогично для `collectSelectedFields` — если выбраны все, возвращает `[]` = все поля.

### Streaming reader
- `runExport()` (995) — POST на `/api/export-stream`, чтение `resp.body.getReader()`, парсинг `data: {...}\n\n`, `handleStreamEvent`

## Важные технические детали и подводные камни

1. **Только одна выгрузка одновременно** — глобальная `export_status`. Параллельный запуск отклоняется.
2. **Только один scheduled job** (`export_job`) — для нескольких сущностей по расписанию это узкое место.
3. **`_needs_deal_id_pivot`** — особенность BI-коннектора, нельзя фильтровать UF-таблицы по CATEGORY_ID/STAGE_ID напрямую.
4. **Русские названия UF-полей** — медленно (batch до 50 за раз), кешировать стоит на клиенте/сервере.
5. **Типы колонок** — после первой выгрузки берутся из таблицы, не из новой выборки (защита от смены типа).
6. **`localStorage`** хранит фильтры ПО СУЩНОСТИ — при удалении из UI не сбрасывается.
7. **Кодировка консоли Windows** — bat-файлы могут отображаться кракозябрами, это косметика.

## Новые фичи (после первоначального состояния)

### Сохранённые конфигурации (`saved_configs` в config.json)
- CRUD: `GET/POST /api/saved-configs`, `PUT /api/saved-configs/{name}` (полное обновление содержимого), `POST /api/saved-configs/{name}/rename`, `DELETE /api/saved-configs/{name}`
- При `POST` — конфликт имени → 409. При rename/delete — также чистится `schedule.configs`.
- Конфиг хранит: `name, entity, date_field, dimensions_filters[], fields[]`. Даты не хранит.

### Ручная отправка
- `/api/manual-export-stream` принимает `{jobs: [{name,entity,date_field,dimensions_filters?,fields?}], start_date, end_date}`
- Запускает их **последовательно** через `_export_event_iter`, обогащая каждое событие `job_idx/job_name`.
- Эмиттит `job_start`/`job_done`/`all_done`.

### Расписание (multi-config)
- `schedule.configs: [name1, name2]` — множественный выбор сохранённых конфигов
- `_run_scheduled` итерирует их последовательно через `_do_export`
- Backwards-compat: если `configs` пуст, но есть старый `entity` — работает как раньше.

### Сверка данных (`/api/check-updates-stream` + `/api/apply-updates`)
- **Цель**: ловить ситуации, когда данные в Bitrix изменились задним числом (например, поправили CLOSEDATE сделки)
- `_compare_with_clickhouse(config, entity, raw)`:
  - Берёт raw из Bitrix (формат BI: [headers, row, ...])
  - Требует колонку `ID` в headers (иначе warning, ничего не сравнивает)
  - Группирует raw по ID, выбирает из CH `SELECT ... WHERE toString(ID) IN (...)` чанками по 1000
  - Сравнивает только пересекающиеся колонки (CH ⋂ headers) после нормализации `_norm_for_compare`
  - Возвращает `{headers, id_col, new[], changed[{id,row,diff}], unchanged_count, total_bitrix, warning?}`
- `/api/check-updates-stream` стримит per-config события. Поле даты выбирается автоматически: первое из `[DATE_MODIFY, CHANGED_DATE, DATE_UPDATE]`, что есть в `entity.date_fields`.
- `_apply_reconciliation`:
  - Auto-ALTER ADD COLUMN для отсутствующих колонок
  - `ALTER TABLE ... DELETE WHERE toString(ID) IN (...) SETTINGS mutations_sync = 2` — синхронная мутация
  - `client.insert(...)` строки
- **Ограничения**:
  - Удалённые в Bitrix записи не обнаруживаются (DATE_MODIFY уже не приходит)
  - Сравнение по string-нормализации — может ложно сработать при редких типах (бинарные)
  - Сущности без ID пропускаются

### Auto-ALTER ADD COLUMN (`push_to_clickhouse`)
- Когда конфиг содержит поле, которого ещё нет в существующей CH-таблице (например, в Bitrix добавили UF), автоматически выполняется `ALTER TABLE ADD COLUMN IF NOT EXISTS \`<col>\` <type>`.
- Это ловит частую ошибку `Unrecognized column 'X' in table Y` со стороны ClickHouse при INSERT.

### История выгрузок (`history.json`)
- Файл `history.json` в корне (gitignored через `.gitignore` если будет нужно). Capped at `HISTORY_LIMIT=1000` записей.
- `_record_history(entry)` — prepend, автоинкремент `id`. Запись содержит `started_at, finished_at, duration_sec, source, config_name?, entity, entity_name, date_field, start_date, end_date, dimensions_filters, fields, rows, status, error?, deleted?`
- `source` ∈ `manual_form` (single через /api/export-stream), `manual_batch` (через /api/manual-export-stream), `schedule` (из `_run_scheduled` → `_do_export`), `reconciliation` (из `/api/apply-updates`).
- Хелпер `_iter_with_history(inner, source, ...)` — async pass-through generator, оборачивает `_export_event_iter` и записывает строку при завершении (счётчик rows из события `done`).
- `_do_export(data, source, config_name)` — теперь принимает source/config_name, использует `fetch_from_bitrix_safe`, в `finally` записывает историю.
- Endpoints: `GET /api/history?limit=N` (по умолчанию 200, при отсутствии — все), `DELETE /api/history` (полная очистка).
- Frontend: вкладка `history` в навигации, при клике вызывает `loadHistory()`. Таблица с фильтрами (источник/статус/поиск), сжатые grid-row'ы, раскрытие по клику показывает фильтры + chip'ы полей.

### Auto-skip removed Bitrix fields (`fetch_from_bitrix_safe`)
- Wrapper над `fetch_from_bitrix`: ловит ошибку `Unrecognized column 'X'` со стороны Bitrix BI, удаляет X из `fields`, повторяет (до 30 итераций).
- Возвращает `(raw, removed[])`. В стрим выводится info с пропущенными полями.

## Provider abstraction (подготовка к Ozon)

После рефакторинга добавлены концепции мульти-провайдерности — но **существующее поведение не изменилось**:

### Поле `provider` в данных
- `saved_configs[*].provider` — `"bitrix"` (обязательное поле для новых, бекфилл при чтении для legacy)
- `history[*].provider` — пишется при создании записи, бекфилл при чтении
- `config.json` имеет секцию `"ozon": {"client_id": "", "api_key": ""}` — добавляется автоматически в `load_config` (in-memory), на диск пишется только при первом сохранении настроек

### Endpoint'ы текущей версии
**Все Bitrix-endpoint'ы остались на своих URL** — НЕ переименовывались. Когда добавится Ozon, его endpoint'ы пойдут под префиксом `/api/ozon/...` рядом, без удаления старых.

### Параметр `provider` в коде
- `_do_export(data, source, config_name, provider="bitrix")`
- `_iter_with_history(..., provider="bitrix")`
- `_run_scheduled` берёт provider из самого saved-config'а
- `/api/export-stream` принимает provider в body (default "bitrix")
- `/api/manual-export-stream` берёт provider из jobs[i].provider

### Section markers в main.py
- `# === BITRIX24 PROVIDER ===` — после "Config helpers"
- `# === OZON PROVIDER ===` (stub с TODO) — внизу перед `if __name__`

### Ozon — реализован (Шаг 3 готов)
Под маркером `# === OZON PROVIDER ===` в main.py:
- `OZON_API_BASE` (Seller API) + `OZON_PERF_BASE` (Performance API)
- Клиент `_ozon_seller_request` (Client-Id + Api-Key headers)
- `_ozon_perf_token` + `_ozon_perf_request` — OAuth2 токен с кэшем (TTL 30 мин)
- `_ozon_accounts_list` / `_ozon_account_by_name` — multi-account доступ
- Каталог `OZON_ENTITIES` (4 категории, 10 сущностей)
- Fetchers (все возвращают BI-формат `[headers, ...rows]`):
  - `_ozon_fetch_product` — `/v3/product/list` + `/v3/product/info/list`
  - `_ozon_fetch_stock` — `/v4/product/info/stocks`
  - `_ozon_fetch_posting_fbs/fbo` — `/v3/posting/fbs/list` или `/v2/posting/fbo/list`, чанк 30 дней, **flat row per (posting × product)**
  - `_ozon_fetch_finance_transaction` — `/v3/finance/transaction/list`, services flat в строку
  - `_ozon_fetch_returns_fbs/fbo` — `/v1/returns/company/{fbs,fbo}`
  - `_ozon_fetch_analytics_data` — `/v1/analytics/data` с дефолтными метриками (выручка, заказы, показы, конверсия)
  - `_ozon_fetch_analytics_stocks` — `/v1/analytics/stocks` (требует прав)
  - `_ozon_fetch_perf_campaigns` — Performance: список кампаний
  - `_ozon_fetch_perf_statistics` — Performance: статистика по дням
- Dispatcher `_ozon_fetch_dispatch(account, entity, start, end)`
- Async `_ozon_export_event_iter(config, account_name, entity, ...)` — даёт events `info/done/error` совместимо с Bitrix-стримом
- Endpoints:
  - `GET /api/ozon/entities` / `GET /api/ozon/date-field-labels`
  - `GET /api/ozon/accounts` (без секретов) / `GET /api/ozon/accounts/{name}` (с секретами)
  - `POST /api/ozon/accounts` (создать/обновить, поддержка `original_name` для переименования; пустой api_key/perf_secret сохраняет существующее)
  - `DELETE /api/ozon/accounts/{name}`
  - `POST /api/ozon/test-connection` body={name} → проверяет Seller + Performance
  - `POST /api/ozon/export-stream` body={account, entity, date_field?, start_date?, end_date?}

### Multi-account Ozon в config.json
- `config["ozon"]["accounts"] = [{name, client_id, api_key, perf_client_id, perf_secret}]`
- При загрузке legacy-формата `{client_id, api_key}` — auto-wrap в один аккаунт с именем "Магазин"
- saved_configs поддерживает `ozon_account` (имя аккаунта)
- При rename аккаунта — обновляются ссылки в saved_configs

### Provider dispatch
- `_do_export(data, source, config_name, provider)` — для `provider="ozon"` вызывает `_ozon_fetch_dispatch` синхронно, иначе `fetch_from_bitrix_safe`
- Manual-batch — каждый job диспетчится отдельно (`provider`/`ozon_account` в job)
- `_export_event_iter` остался Bitrix-only; для Ozon отдельный `_ozon_export_event_iter`. Оба оборачиваются `_iter_with_history`.

### Frontend
- Страница "Подключение" переделана: 3 свёртываемые секции (`<details>`):
  - **ClickHouse** (всегда раскрыта, обязательна) — два ряда полей в `form-row-2`
  - **Bitrix24** (опциональна, свёрнута; раздел смарт-процессов — вложенный subblock)
  - **Ozon** (опциональна; multi-account UI с кнопкой "+ Добавить кабинет Ozon")
- Каждая секция имеет свой статус-бейдж (`#chStatusBadge`/`#bxStatusBadge`/`#ozStatusBadge` — пока неактивны, заглушка)
- Модалка `#ozonAccountModal` — редактор аккаунта (Seller + Performance), пустые поля паролей сохраняют текущие значения
- Единая система подсказок: `<span class="hint" data-tip="...">?</span>` — pure CSS tooltip на hover/focus
- На "Отправка данных":
  - Pill-переключатель Bitrix/Ozon (`.provider-switch`)
  - Когда Ozon — появляется dropdown `#exportOzonAccount`, и блок Bitrix-фильтров скрывается
  - Каталог сущностей подменяется в `fillEntitySelect(selectId, provider)`
  - `runExport` диспетчит на `/api/export-stream` или `/api/ozon/export-stream`
  - `applyConfigToExportForm(cfg)` сначала вызывает `switchProvider(cfg.provider)` чтобы dropdown сущностей был корректным
- Сохранённые конфиги показывают provider-бейдж (🔵 B24 / 🟠 Ozon) и имя Ozon-аккаунта в скобках

### TODO для следующих итераций
- Реальная индикация статуса подключения для каждой секции (`#chStatusBadge`/etc)
- Поддержка Ozon в "Сверке данных" (modify_field подбор для Ozon)
- Аналитика Ozon: возможность настраивать набор metrics через UI (сейчас захардкожен дефолт)
- Ozon Performance API: автоопределение типов кампаний для статистики (сейчас один общий запрос)
- Ozon analytics_stocks может вернуть 403 — нужны права в API ключе (показывать понятную ошибку)

## Стиль кода
- Всё в одном файле (backend и frontend) — пользователь готов к большим файлам, но добавление файлов нежелательно без явной необходимости
- Минимум комментариев на русском в backend, немного — на английском
- Git: репозиторий не в git (`Is a git repository: false` в worktree), пользователь переносит исходники вручную
