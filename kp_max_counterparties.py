import json
import os
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Thread

import requests

import api_proxy as core
import kp_max_navigation as nav

SESSION_TTL = 30 * 60
MAX_RESULTS = 12
_CONTACT_TTL = 10 * 60
_LOCK = Lock()
_SESSIONS: dict[str, dict] = {}
_CONTACTS: list[dict] = []
_CONTACTS_AT = 0.0
_CONTACTS_LOADING = False
_PROP_NAMES: dict[str, str] = {}
_REF_NAMES: dict[tuple[str, str], str] = {}
_CONTACT_FILE = Path(os.getenv("KP_MAX_ACCESS_FILE", "/opt/kp-api/data/kp_max_access.json")).parent / "kp_max_contact_persons.json"


def _base() -> str:
    return str(core.BASE).strip().strip(chr(34)).strip(chr(39)).rstrip("/")


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold().replace("ё", "е")

def _escape(value: str) -> str:
    return str(value or "").replace("'", "''")


def _cb(text: str, payload: str) -> dict:
    return {"type": "callback", "text": str(text), "payload": str(payload)}


def _keyboard(rows: list[list[dict]]) -> list[dict]:
    return [{"type": "inline_keyboard", "payload": {"buttons": rows}}]


def session_get(user_id: str) -> dict | None:
    now = int(time.time())
    with _LOCK:
        s = _SESSIONS.get(str(user_id))
        if not s or int(s.get("expiresAt") or 0) <= now:
            _SESSIONS.pop(str(user_id), None)
            return None
        return dict(s)


def clear(user_id: str) -> None:
    with _LOCK:
        _SESSIONS.pop(str(user_id), None)


def start(user_id: str) -> dict:
    with _LOCK:
        _SESSIONS[str(user_id)] = {"stage": "await_query", "expiresAt": int(time.time()) + SESSION_TTL}
    refresh_contacts_async()
    return {
        "text": "КОНТРАГЕНТЫ\n\nВведи часть рабочего наименования контрагента.",
        "attachments": _keyboard([[_cb("ОТМЕНА", "cp:cancel")]]),
    }


def _get(entity: str, *, params: dict | None = None, timeout: int = 30) -> list[dict]:
    r = requests.get(f"{_base()}/{entity}", headers=core._build_headers(), params=params, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"1C {entity} HTTP {r.status_code}")
    data = r.json() if r.content else {}
    rows = data.get("value", []) if isinstance(data, dict) else []
    return [x for x in rows if isinstance(x, dict)]


def search(user_id: str, query: str) -> dict:
    s = session_get(user_id)
    if not s or s.get("stage") != "await_query":
        raise RuntimeError("counterparty search is not active")
    q = str(query or "").strip()
    q_norm = _norm(q)
    if len(q_norm) < 2:
        return {"text": "Введи минимум 2 символа рабочего наименования.", "attachments": _keyboard([[_cb("ОТМЕНА", "cp:cancel")]])}
    tokens = [x for x in re.findall(r"[0-9a-zа-я]+", q_norm) if x]
    if not tokens:
        tokens = [q_norm]

    def fetch_token(token: str) -> list[dict]:
        return _get("Catalog_Контрагенты", params={
            "$select": "Ref_Key,Description,ИНН,DeletionMark,Партнер_Key",
            "$filter": f"substringof('{_escape(token)}',Description) eq true", "$top": "250"}, timeout=25)

    candidates: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(tokens))) as pool:
        for rows in pool.map(fetch_token, tokens):
            for row in rows:
                key = str(row.get("Ref_Key") or "").strip()
                if key and not bool(row.get("DeletionMark")):
                    candidates[key] = row
    matched = [x for x in candidates.values() if all(token in _norm(x.get("Description") or "") for token in tokens)]
    def rank(x: dict) -> tuple[int, int, int]:
        name = _norm(x.get("Description") or "")
        positions = [name.find(token) for token in tokens]
        return (0 if name.startswith(tokens[0]) else 1, sum(p for p in positions if p >= 0), len(name))
    matched.sort(key=rank)
    buttons = []
    for x in matched[:MAX_RESULTS]:
        key = str(x.get("Ref_Key") or "").strip()
        name = str(x.get("Description") or "—").strip()
        inn = str(x.get("ИНН") or "").strip()
        label = f"{name}  ИНН {inn}" if inn else name
        if key:
            buttons.append([_cb(label[:110], f"cp:open:{key}")])
    buttons.append([_cb("🔎 ИСКАТЬ ДРУГОГО", "cp:again")])
    buttons.append([_cb("ОТМЕНА", "cp:cancel")])
    with _LOCK:
        if str(user_id) in _SESSIONS:
            _SESSIONS[str(user_id)]["expiresAt"] = int(time.time()) + SESSION_TTL
    if not matched:
        text = f"По запросу «{q}» контрагенты не найдены."
    else:
        text = f"Найдено контрагентов: {len(matched)}. Выбери нужного:"
    return {"text": text, "attachments": _keyboard(buttons)}


def again(user_id: str) -> dict:
    return start(user_id)


def cancel(user_id: str, role: str) -> dict:
    clear(user_id)
    return nav.root_menu(role)

def _fetch_one(entity: str, ref_key: str, timeout: int = 30) -> dict:
    r = requests.get(f"{_base()}/{entity}(guid'{ref_key}')", headers=core._build_headers(), timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"1C {entity} read HTTP {r.status_code}")
    data = r.json() if r.content else {}
    return data if isinstance(data, dict) else {}


def _ref_name(entity: str, ref_key: str) -> str:
    ref = str(ref_key or "").strip()
    if not ref or ref == "00000000-0000-0000-0000-000000000000":
        return ""
    cache_key = (entity, ref)
    if cache_key in _REF_NAMES:
        return _REF_NAMES[cache_key]
    try:
        r = requests.get(f"{_base()}/{entity}(guid'{ref}')", headers=core._build_headers(), params={"$select":"Description"}, timeout=15)
        name = str((r.json() if r.status_code == 200 else {}).get("Description") or "").strip()
    except Exception:
        name = ""
    _REF_NAMES[cache_key] = name
    return name


def _property_name(ref_key: str) -> str:
    ref = str(ref_key or "").strip()
    if not ref:
        return ""
    if ref in _PROP_NAMES:
        return _PROP_NAMES[ref]
    name = _ref_name("ChartOfCharacteristicTypes_ДополнительныеРеквизитыИСведения", ref)
    _PROP_NAMES[ref] = name or ref
    return _PROP_NAMES[ref]

def _load_contacts_file() -> list[dict]:
    try:
        data=json.loads(_CONTACT_FILE.read_text(encoding="utf-8"))
        return [x for x in data if isinstance(x,dict)] if isinstance(data,list) else []
    except Exception:
        return []


def _save_contacts_file(rows: list[dict]) -> None:
    try:
        _CONTACT_FILE.parent.mkdir(parents=True,exist_ok=True)
        _CONTACT_FILE.write_text(json.dumps(rows,ensure_ascii=False,separators=(",",":")),encoding="utf-8")
        os.chmod(_CONTACT_FILE,0o600)
    except Exception:
        pass


def _load_contacts() -> list[dict]:
    collected: list[dict] = []
    for skip in range(0, 20000, 250):
        rows = _get("Catalog_КонтактныеЛицаПартнеров", params={
            "$select":"Ref_Key,Description,Owner_Key,DeletionMark,ДолжностьПоВизитке,Комментарий,ДополнительнаяИнформация,КонтактнаяИнформация",
            "$top":"250", "$skip":str(skip)}, timeout=30)
        if not rows:
            break
        collected.extend(x for x in rows if not bool(x.get("DeletionMark")))
        if len(rows) < 250:
            break
    return collected


def _contacts_worker() -> None:
    global _CONTACTS, _CONTACTS_AT, _CONTACTS_LOADING
    try:
        rows = _load_contacts()
        if rows:
            _save_contacts_file(rows)
        with _LOCK:
            if rows:
                _CONTACTS = rows
                _CONTACTS_AT = time.time()
    finally:
        with _LOCK:
            _CONTACTS_LOADING = False


def refresh_contacts_async() -> bool:
    global _CONTACTS_LOADING, _CONTACTS, _CONTACTS_AT
    if not _CONTACTS:
        disk = _load_contacts_file()
        if disk:
            with _LOCK:
                if not _CONTACTS:
                    _CONTACTS = disk
                    _CONTACTS_AT = _CONTACT_FILE.stat().st_mtime if _CONTACT_FILE.exists() else time.time()
    with _LOCK:
        fresh = bool(_CONTACTS and time.time() - _CONTACTS_AT < _CONTACT_TTL)
        if fresh or _CONTACTS_LOADING:
            return False
        _CONTACTS_LOADING = True
    Thread(target=_contacts_worker, name="kp-counterparty-contacts", daemon=True).start()
    return True

def _contacts_for(partner_key: str) -> list[dict]:
    global _CONTACTS_AT
    refresh_contacts_async()
    for _ in range(20):
        with _LOCK:
            rows = list(_CONTACTS)
            loading = _CONTACTS_LOADING
        if rows or not loading:
            break
        time.sleep(0.1)
    if not rows:
        rows = _load_contacts()
        if rows:
            _save_contacts_file(rows)
        with _LOCK:
            _CONTACTS[:] = rows
            _CONTACTS_AT = time.time()
    key = str(partner_key or "").strip()
    return [dict(x) for x in rows if str(x.get("Owner_Key") or "").strip() == key]


def _contact_values(rows: list[dict]) -> list[str]:
    result = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        value = str(row.get("Представление") or row.get("Значение") or row.get("НомерТелефона") or row.get("АдресЭП") or "").strip()
        kind = str(row.get("Тип") or "").strip()
        if value:
            result.append(f"{kind}: {value}" if kind else value)
    return result


_LABELS = {
    "НаименованиеПолное":"Полное наименование", "ЮридическоеФизическоеЛицо":"Тип лица",
    "ЮрФизЛицо":"Юр./физ. лицо", "КПП":"КПП", "ДополнительнаяИнформация":"Дополнительная информация",
    "КодПоОКПО":"Код по ОКПО", "РегистрационныйНомер":"Регистрационный номер",
    "НалоговыйНомер":"Налоговый номер", "НаименованиеМеждународное":"Международное наименование",
    "НаименованиеВТранскрипции":"Наименование в транскрипции", "НаименованиеЯзык1":"Наименование (язык 1)",
    "НаименованиеЯзык2":"Наименование (язык 2)", "ОбособленноеПодразделение":"Обособленное подразделение",
}
_EXCLUDE = {"Ref_Key","Predefined","PredefinedDataName","DataVersion","Description","DeletionMark","ИНН","Партнер_Key"}

def _other_info(data: dict) -> list[str]:
    lines: list[str] = []
    special = {
        "СтранаРегистрации_Key": ("Страна регистрации", "Catalog_СтраныМира"),
        "ГоловнойКонтрагент_Key": ("Головной контрагент", "Catalog_Контрагенты"),
    }
    for key, (label, entity) in special.items():
        name = _ref_name(entity, str(data.get(key) or ""))
        if name:
            lines.append(f"{label}: {name}")
    for key, value in data.items():
        if key in _EXCLUDE or key in special or key.endswith("@navigationLinkUrl") or key.endswith("_Key"):
            continue
        if isinstance(value, (list, dict)) or value in (None, "", False, 0):
            continue
        label = _LABELS.get(key, key)
        lines.append(f"{label}: {value}")
    for value in _contact_values(data.get("КонтактнаяИнформация") or []):
        lines.append(f"Контактная информация: {value}")
    extras = [x for x in (data.get("ДополнительныеРеквизиты") or []) if isinstance(x, dict)]
    if extras:
        with ThreadPoolExecutor(max_workers=8) as pool:
            names = list(pool.map(lambda x: _property_name(str(x.get("Свойство_Key") or "")), extras))
        for item, name in zip(extras, names):
            value = str(item.get("ТекстоваяСтрока") or item.get("Значение") or "").strip()
            if value:
                lines.append(f"{name}: {value}")
    return lines


def _contact_person_lines(person: dict) -> list[str]:
    name = str(person.get("Description") or "—").strip()
    position = str(person.get("ДолжностьПоВизитке") or "").strip()
    head = f"• {name}" + (f" — {position}" if position else "")
    lines = [head]
    for value in _contact_values(person.get("КонтактнаяИнформация") or []):
        lines.append(f"  {value}")
    comment = str(person.get("Комментарий") or "").strip()
    extra = str(person.get("ДополнительнаяИнформация") or "").strip()
    if comment:
        lines.append(f"  Комментарий: {comment}")
    if extra:
        lines.append(f"  Доп. информация: {extra}")
    return lines

def card(ref_key: str, role: str) -> dict:
    data = _fetch_one("Catalog_Контрагенты", ref_key)
    name = str(data.get("Description") or "—").strip()
    inn = str(data.get("ИНН") or "—").strip() or "—"
    partner_key = str(data.get("Партнер_Key") or "").strip()
    other = _other_info(data)
    contacts = _contacts_for(partner_key) if partner_key else []
    lines = [f"КОНТРАГЕНТ\n\nРабочее наименование: {name}", f"ИНН: {inn}", "", "ПРОЧАЯ ИНФОРМАЦИЯ"]
    lines.extend(other or ["[НЕТ ЗАПОЛНЕННЫХ ДАННЫХ]"])
    lines.extend(["", "КОНТАКТНЫЕ ЛИЦА"])
    if contacts:
        for person in contacts:
            lines.extend(_contact_person_lines(person))
    else:
        lines.append("[КОНТАКТНЫЕ ЛИЦА НЕ НАЙДЕНЫ]")
    rows = [
        [_cb("🔎 ИСКАТЬ ДРУГОГО", "cp:again")],
        [_cb("🟢🟢 ← ВЕРНУТЬСЯ НА ГЛАВНОЕ МЕНЮ", "cp:cancel")],
    ]
    return {"text": "\n".join(lines), "attachments": _keyboard(rows)}