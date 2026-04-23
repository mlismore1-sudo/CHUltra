import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

BASE_URL = "https://api.company-information.service.gov.uk"
AUTO_REFRESH_SECONDS = 3
AUTO_RUN_EVERY_SECONDS = 60

TECH_SIC_CODES = {
    "58210", "58290", "59111", "59113", "59120", "59133", "59140", "59200",
    "60100", "60200", "61100", "61200", "61300", "61900", "62011", "62012",
    "62020", "62030", "62090", "63110", "63120", "71121", "71122", "71200",
    "72110", "72190", "72200", "82290"
}

HOLDINGS_SIC_CODES = {
    "64201", "64202", "64203", "64204", "64205", "64209", "66300"
}

PROPERTY_SIC_CODES = {
    "68100", "68201", "68209", "41100", "41201", "41202", "42110", "43110"
}

TARGET_SIC_CODES = TECH_SIC_CODES | HOLDINGS_SIC_CODES

SEEN_FILE = "seen_companies.json"
RESULTS_FILE = "companies_house_results.csv"

SIC_GROUP_MAP = {}
for code in TECH_SIC_CODES:
    SIC_GROUP_MAP[code] = "Tech"
for code in HOLDINGS_SIC_CODES:
    SIC_GROUP_MAP[code] = "Holdings"
for code in PROPERTY_SIC_CODES:
    SIC_GROUP_MAP[code] = "Property"


def inject_auto_refresh(seconds: int):
    components.html(
        f"""
        <html>
            <head>
                <meta http-equiv="refresh" content="{seconds}">
            </head>
            <body></body>
        </html>
        """,
        height=0,
        width=0,
    )


def get_api_keys_from_streamlit_secrets() -> List[str]:
    """
    Expects Streamlit Secrets to contain:

    COMPANIES_HOUSE_API_KEYS = [
      "key1",
      "key2",
      "key3"
    ]
    """
    try:
        raw = st.secrets["COMPANIES_HOUSE_API_KEYS"]
    except Exception:
        return []

    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]

    if isinstance(raw, str):
        return [raw.strip()] if raw.strip() else []

    return []


class RotatingCHClient:
    def __init__(self, api_keys: List[str], rotate_every: int = 599):
        if not api_keys:
            raise ValueError("At least one Companies House API key is required.")
        self.api_keys = api_keys
        self.rotate_every = rotate_every
        self.key_index = 0
        self.request_count_on_key = 0
        self.session = requests.Session()

    def _rotate_key_if_needed(self):
        if self.request_count_on_key >= self.rotate_every:
            self.key_index = (self.key_index + 1) % len(self.api_keys)
            self.request_count_on_key = 0

    def _auth(self) -> Tuple[str, str]:
        return (self.api_keys[self.key_index], "")

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        retries = 0
        while retries < 5:
            self._rotate_key_if_needed()
            url = f"{BASE_URL}{path}"
            resp = self.session.get(url, params=params, auth=self._auth(), timeout=30)
            self.request_count_on_key += 1

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return {}
            if resp.status_code == 429:
                self.key_index = (self.key_index + 1) % len(self.api_keys)
                self.request_count_on_key = 0
                retries += 1
                time.sleep(2)
                continue
            if 500 <= resp.status_code < 600:
                retries += 1
                time.sleep(2)
                continue

            raise RuntimeError(f"Request failed: {resp.status_code} {resp.text[:500]}")

        raise RuntimeError(f"Failed after retries for path {path}")


def load_json_file(path: str, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def daterange_chunks(start_date: datetime, end_date: datetime, chunk_days: int = 7):
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def trim_postcode_area(postcode: Optional[str]) -> str:
    if not postcode:
        return ""
    postcode = postcode.strip().upper()
    return postcode[:-3].strip() if len(postcode) > 3 else postcode


def sic_matches(company_sic_codes: List[str]) -> bool:
    return any(code in TARGET_SIC_CODES for code in (company_sic_codes or []))


def get_sic_group(company_sic_codes: List[str]) -> str:
    groups = []
    for code in company_sic_codes or []:
        group = SIC_GROUP_MAP.get(code)
        if group in {"Tech", "Holdings"} and group not in groups:
            groups.append(group)
    return ", ".join(groups) if groups else "Other"


def get_company_officers(client: RotatingCHClient, company_number: str) -> List[dict]:
    data = client.get(f"/company/{company_number}/officers")
    return data.get("items", [])


def is_active_director(officer: dict) -> bool:
    role = (officer.get("officer_role") or "").lower()
    resigned_on = officer.get("resigned_on")
    name = officer.get("name")
    return role == "director" and not resigned_on and bool(name)


def advanced_search_companies(client: RotatingCHClient, start_date: str, end_date: str) -> List[dict]:
    results = []
    start_index = 0
    size = 5000

    while True:
        params = {
            "incorporated_from": start_date,
            "incorporated_to": end_date,
            "sic_codes": ",".join(sorted(TARGET_SIC_CODES)),
            "size": size,
            "start_index": start_index,
        }
        data = client.get("/advanced-search/companies", params=params)
        items = data.get("items", [])
        if not items:
            break

        results.extend(items)

        if len(items) < size:
            break

        start_index += size

        if start_index >= 10000:
            break

    return results


def collect_companies(
    client: RotatingCHClient,
    date_from: str,
    date_to: str,
    seen_companies: set,
) -> List[dict]:
    all_rows = []

    for chunk_start, chunk_end in daterange_chunks(parse_date(date_from), parse_date(date_to), chunk_days=7):
        chunk_from = chunk_start.strftime("%Y-%m-%d")
        chunk_to = chunk_end.strftime("%Y-%m-%d")
        companies = advanced_search_companies(client, chunk_from, chunk_to)

        for company in companies:
            company_number = company.get("company_number")
            company_name = company.get("company_name", "")
            sic_codes = company.get("sic_codes", []) or []
            ro_address = company.get("registered_office_address", {}) or {}
            ro_postcode = ro_address.get("postal_code") or ro_address.get("postcode") or company.get("postcode")

            if not company_number or company_number in seen_companies:
                continue

            if not sic_matches(sic_codes):
                continue

            officers = get_company_officers(client, company_number)
            directors = [o for o in officers if is_active_director(o)]

            row = {
                "company_name": company_name,
                "company_number": company_number,
                "SIC Group": get_sic_group(sic_codes),
                "Directors": len(directors),
                "sic_codes": "; ".join(sic_codes),
                "Postcode": trim_postcode_area(ro_postcode),
            }

            for i, director in enumerate(directors, start=1):
                row[f"director_{i}_name"] = director.get("name", "")
                row[f"director_{i}_postcode"] = (director.get("address") or {}).get("postal_code", "")

            all_rows.append(row)
            seen_companies.add(company_number)

    return all_rows


def write_results_csv(rows: List[dict], filename: str):
    if not rows:
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(filename):
        try:
            existing_df = pd.read_csv(filename)
        except Exception:
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()

    combined = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df

    if "company_number" in combined.columns:
        combined = combined.drop_duplicates(subset=["company_number"], keep="last")
    else:
        combined = combined.drop_duplicates()

    combined.to_csv(filename, index=False, encoding="utf-8-sig")


def load_results_df() -> pd.DataFrame:
    if os.path.exists(RESULTS_FILE):
        try:
            return pd.read_csv(RESULTS_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def prepare_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    display_df = df.copy()
    display_df = display_df.drop(columns=["company_number"], errors="ignore")

    ordered_cols = [
        "company_name",
        "SIC Group",
        "Directors",
        "sic_codes",
        "Postcode",
    ]
    dynamic_cols = [c for c in display_df.columns if c not in ordered_cols]
    final_cols = [c for c in ordered_cols if c in display_df.columns] + dynamic_cols

    display_df = display_df[final_cols]

    rename_map = {
        "company_name": "Company Name",
        "sic_codes": "SIC Codes",
    }
    return display_df.rename(columns=rename_map)


def run_pipeline(api_keys: List[str], date_from: str, date_to: str):
    seen_companies = set(load_json_file(SEEN_FILE, []))
    client = RotatingCHClient(api_keys, rotate_every=599)
    rows = collect_companies(client, date_from, date_to, seen_companies)
    save_json_file(SEEN_FILE, sorted(seen_companies))
    write_results_csv(rows, RESULTS_FILE)
    return rows


st.set_page_config(page_title="Companies House Live Monitor", layout="wide")
st.title("Companies House Live Monitor")
st.caption("Auto-refreshing dashboard for Tech and Holdings Companies House results.")

if "last_run_time" not in st.session_state:
    st.session_state.last_run_time = None

if "last_new_rows" not in st.session_state:
    st.session_state.last_new_rows = []

if "last_auto_run_ts" not in st.session_state:
    st.session_state.last_auto_run_ts = 0.0

if "last_status" not in st.session_state:
    st.session_state.last_status = "Waiting to run."

if "refresh_count" not in st.session_state:
    st.session_state.refresh_count = 0

with st.sidebar:
    st.header("Search settings")
    default_date = datetime.today().strftime("%Y-%m-%d")
    date_from = st.text_input("Incorporation start date", value=default_date)
    date_to = st.text_input("Incorporation end date", value=default_date)
    auto_refresh_enabled = st.toggle("Auto-refresh every 3 seconds", value=True)
    run_now = st.button("Refresh results now", type="primary")
    clear_data = st.button("Clear saved results")
    show_debug = st.toggle("Show secrets debug", value=False)

if auto_refresh_enabled:
    st.session_state.refresh_count += 1
    inject_auto_refresh(AUTO_REFRESH_SECONDS)

api_keys = get_api_keys_from_streamlit_secrets()

if api_keys:
    st.success(f"Loaded {len(api_keys)} API key(s) from Streamlit Secrets.")
else:
    st.error(
        "No API keys found in Streamlit Secrets. Add this exact key:\n\n"
        "COMPANIES_HOUSE_API_KEYS = [\"key1\", \"key2\", \"key3\"]"
    )

if show_debug:
    st.subheader("Secrets debug")
    try:
        has_secret = "COMPANIES_HOUSE_API_KEYS" in st.secrets
        st.write("Key exists:", has_secret)
        if has_secret:
            raw_value = st.secrets["COMPANIES_HOUSE_API_KEYS"]
            st.write("Secret type:", type(raw_value).__name__)
            if isinstance(raw_value, list):
                st.write("List length:", len(raw_value))
                st.write("Preview:", [f\"Key {i+1}: {str(v)[:6]}...\" for i, v in enumerate(raw_value)])
            else:
                st.write("Raw value:", raw_value)
    except Exception as e:
        st.error(f"Secrets debug error: {e}")

st.info(
    f"Page refreshes every {AUTO_REFRESH_SECONDS} seconds. Automatic data collection is throttled "
    f"to once every {AUTO_RUN_EVERY_SECONDS} seconds unless you click 'Refresh results now'."
)

if clear_data:
    for path in [RESULTS_FILE, SEEN_FILE]:
        if os.path.exists(path):
            os.remove(path)
    st.session_state.last_run_time = None
    st.session_state.last_new_rows = []
    st.session_state.last_auto_run_ts = 0.0
    st.session_state.last_status = "Saved results and caches cleared."
    st.session_state.refresh_count = 0
    st.success("Saved results and caches cleared.")
    st.rerun()

try:
    parse_date(date_from)
    parse_date(date_to)
except ValueError:
    st.error("Invalid date format. Please use YYYY-MM-DD.")
    st.stop()

now_ts = time.time()
seconds_since_last_auto_run = now_ts - st.session_state.last_auto_run_ts
auto_run_due = auto_refresh_enabled and seconds_since_last_auto_run >= AUTO_RUN_EVERY_SECONDS
should_run_pipeline = run_now or auto_run_due

if should_run_pipeline:
    if not api_keys:
        st.session_state.last_status = "Cannot run: no API keys found in Streamlit Secrets."
        st.error("Please add COMPANIES_HOUSE_API_KEYS in the Streamlit Secrets page before running the app.")
    else:
        try:
            with st.spinner("Checking Companies House for new matches..."):
                new_rows = run_pipeline(api_keys, date_from, date_to)
            st.session_state.last_new_rows = new_rows
            st.session_state.last_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state.last_auto_run_ts = time.time()
            st.session_state.last_status = f"Run completed at {st.session_state.last_run_time}"
            st.success(st.session_state.last_status)
        except Exception as e:
            st.session_state.last_status = f"Error during run: {e}"
            st.error(st.session_state.last_status)

results_df = load_results_df()
display_results_df = prepare_display_df(results_df)
new_results_df = (
    prepare_display_df(pd.DataFrame(st.session_state.last_new_rows))
    if st.session_state.last_new_rows
    else pd.DataFrame()
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total results", len(results_df))
col2.metric("New in last run", len(st.session_state.last_new_rows))
col3.metric("Seen companies", len(load_json_file(SEEN_FILE, [])))
col4.metric("Refresh count", st.session_state.refresh_count)

if st.session_state.last_run_time:
    st.caption(f"Last successful refresh: {st.session_state.last_run_time}")
else:
    st.caption("No successful run yet in this session.")

remaining = max(0, int(AUTO_RUN_EVERY_SECONDS - (time.time() - st.session_state.last_auto_run_ts)))
st.caption(f"Next automatic pipeline run in approximately {remaining} seconds.")
st.caption(st.session_state.last_status)

st.subheader("New results from last run")
if not new_results_df.empty:
    st.dataframe(new_results_df, use_container_width=True)
else:
    st.info("No new results found yet.")

st.subheader("All tracked results")
if not display_results_df.empty:
    st.dataframe(display_results_df, use_container_width=True)
else:
    st.info("No tracked results yet.")
