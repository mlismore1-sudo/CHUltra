import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Companies Incorporated Today", layout="wide")

TECH_SIC_CODES = {
    "62012", "62020", "58290", "58210", "61100", "61200", "61300", "61900",
    "62011", "62030", "62090", "63110", "63120", "71200", "72110", "72190",
    "72200", "71129",
}

HOLDINGS_SIC_CODES = {
    "64201", "64202", "64203", "64204", "64205", "64209", "66300",
}

TARGET_SIC_CODES = sorted(TECH_SIC_CODES | HOLDINGS_SIC_CODES)
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def today_uk_str() -> str:
    return datetime.now().astimezone().date().isoformat()


def get_api_keys() -> List[str]:
    keys: List[str] = []
    list_style_keys = st.secrets.get("COMPANIES_HOUSE_API_KEYS", [])
    if list_style_keys:
        keys.extend([str(k).strip() for k in list_style_keys if str(k).strip()])
    for key_name in ["CH_API_KEY_1", "CH_API_KEY_2", "CH_API_KEY_3"]:
        value = st.secrets.get(key_name, "")
        if value:
            keys.append(str(value).strip())
    deduped_keys = []
    seen = set()
    for key in keys:
        if key and key not in seen:
            deduped_keys.append(key)
            seen.add(key)
    return deduped_keys


def auth_header(api_key: str) -> Dict[str, str]:
    token = base64.b64encode(f"{api_key}:".encode()).decode()
    return {"Authorization": f"Basic {token}", "User-Agent": "streamlit-companies-house-today-app"}


def classify_sector(sic_codes: List[str]) -> str | None:
    codes = {str(code) for code in (sic_codes or [])}
    if codes & HOLDINGS_SIC_CODES:
        return "Holdings"
    if codes & TECH_SIC_CODES:
        return "Tech"
    return None


def fetch_with_rotation(url: str, params: Dict[str, str], api_keys: List[str], timeout: int = 30) -> requests.Response:
    last_response = None
    for api_key in api_keys:
        response = requests.get(url, headers=auth_header(api_key), params=params, timeout=timeout)
        if response.status_code in (401, 429):
            last_response = response
            continue
        response.raise_for_status()
        return response
    if last_response is not None:
        last_response.raise_for_status()
    raise RuntimeError("No valid Companies House API keys were available.")


def fetch_companies_incorporated_today(api_keys: List[str], run_date: str) -> pd.DataFrame:
    url = "https://api.company-information.service.gov.uk/advanced-search/companies"
    start_index = 0
    page_size = 5000
    rows = []
    pull_counter = 0

    while True:
        params = {
            "incorporated_from": run_date,
            "incorporated_to": run_date,
            "sic_codes": ",".join(TARGET_SIC_CODES),
            "size": str(page_size),
            "start_index": str(start_index),
        }
        response = fetch_with_rotation(url, params, api_keys)
        payload = response.json()
        items = payload.get("items", []) or []
        for item in items:
            sic_codes = [str(code) for code in item.get("sic_codes", []) if code]
            sector = classify_sector(sic_codes)
            if not sector:
                continue
            rows.append({
                "company_number": item.get("company_number", ""),
                "company_name": item.get("company_name", ""),
                "date_of_creation": item.get("date_of_creation", run_date),
                "sic_codes": ", ".join(sic_codes),
                "sector": sector,
                "kind": item.get("kind", ""),
                "company_status": item.get("company_status", ""),
                "pull_order": pull_counter,
            })
            pull_counter += 1
        if len(items) < page_size:
            break
        start_index += page_size

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["company_number", "company_name", "date_of_creation", "sic_codes", "sector", "kind", "company_status", "pull_order"])

    df = df.sort_values("pull_order", ascending=False, kind="stable").drop_duplicates(subset=["company_number"], keep="first").reset_index(drop=True)
    return df


def get_store_paths(run_date: str) -> Tuple[Path, Path]:
    snapshot_path = DATA_DIR / f"companies_{run_date}.csv"
    seen_path = DATA_DIR / f"seen_{run_date}.csv"
    return snapshot_path, seen_path


def load_csv_or_empty(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path, dtype=str).fillna("")
    return pd.DataFrame()


def identify_new_rows(current_df: pd.DataFrame, seen_df: pd.DataFrame) -> pd.DataFrame:
    if current_df.empty:
        return current_df.copy()
    if seen_df.empty or "company_number" not in seen_df.columns:
        return current_df.copy()
    unseen = current_df[~current_df["company_number"].isin(seen_df["company_number"].astype(str))].copy()
    return unseen.reset_index(drop=True)


def save_state(current_df: pd.DataFrame, snapshot_path: Path, seen_path: Path) -> None:
    current_df.to_csv(snapshot_path, index=False)
    current_df.to_csv(seen_path, index=False)


def render_table(df: pd.DataFrame, title: str) -> None:
    st.subheader(title)
    if df.empty:
        st.info("No companies to show yet.")
        return
    display_df = df[["company_name", "sector"]].rename(columns={"company_name": "Company Name", "sector": "Sector"})
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def main() -> None:
    st.title("Companies Incorporated Today")
    st.caption("Shows companies incorporated today that match your Tech and Holdings SIC code lists, with newest pulls shown first.")
    api_keys = get_api_keys()
    if not api_keys:
        st.error("Add COMPANIES_HOUSE_API_KEYS or CH_API_KEY_1/2/3 to your Streamlit secrets before running the app.")
        st.stop()
    run_date = today_uk_str()
    snapshot_path, seen_path = get_store_paths(run_date)
    st.sidebar.header("Controls")
    st.sidebar.write(f"Run date: {run_date}")
    st.sidebar.write(f"API keys loaded: {len(api_keys)}")
    refresh = st.sidebar.button("Refresh now", type="primary")
    if refresh or not snapshot_path.exists():
        with st.spinner("Fetching today’s companies from Companies House..."):
            current_df = fetch_companies_incorporated_today(api_keys, run_date)
        seen_df = load_csv_or_empty(seen_path)
        new_df = identify_new_rows(current_df, seen_df)
        save_state(current_df, snapshot_path, seen_path)
        st.session_state["latest_df"] = current_df
        st.session_state["new_df"] = new_df
        st.session_state["last_refresh"] = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        current_df = load_csv_or_empty(snapshot_path)
        st.session_state.setdefault("latest_df", current_df)
        st.session_state.setdefault("new_df", pd.DataFrame(columns=current_df.columns if not current_df.empty else ["company_number", "company_name", "date_of_creation", "sic_codes", "sector", "kind", "company_status", "pull_order"]))
        st.session_state.setdefault("last_refresh", "Not refreshed in this session")
    current_df = st.session_state.get("latest_df", pd.DataFrame())
    new_df = st.session_state.get("new_df", pd.DataFrame())
    c1, c2, c3 = st.columns(3)
    c1.metric("Total pulled today", int(len(current_df)))
    c2.metric("New on latest refresh", int(len(new_df)))
    c3.metric("Run date", run_date)
    st.write(f"Last refresh: {st.session_state.get('last_refresh', 'Unknown')}")
    render_table(new_df, "New companies found on the latest refresh")
    render_table(current_df, "All companies pulled so far today")
    if not current_df.empty:
        csv_bytes = current_df[["company_name", "sector"]].rename(columns={"company_name": "Company Name", "sector": "Sector"}).to_csv(index=False).encode("utf-8")
        st.download_button(label="Download today’s results as CSV", data=csv_bytes, file_name=f"companies_incorporated_{run_date}.csv", mime="text/csv")
    with st.expander("Suggested .streamlit/secrets.toml"):
        secrets_example = """COMPANIES_HOUSE_API_KEYS = [
  "your-first-key",
  "your-second-key",
  "your-third-key"
]

# Optional legacy format
# CH_API_KEY_1 = "your-first-key"
# CH_API_KEY_2 = "your-second-key"
# CH_API_KEY_3 = "your-third-key""" 
        st.code(secrets_example, language="toml")
    with st.expander("Notes"):
        st.markdown("""
- The app uses the Companies House advanced company search endpoint filtered by `incorporated_from`, `incorporated_to`, and your SIC code lists.
- Holdings takes priority if a company matches both sector lists.
- The results are kept in newest-pull-first order within each refresh.
- The top table shows companies that were not present in the saved daily snapshot before the latest manual refresh.
- Daily snapshots reset automatically because the file names are date-based.
        """)

if __name__ == "__main__":
    main()
