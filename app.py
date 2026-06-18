# app.py

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
import re

# =========================
# CONFIG
# =========================

YEAR = 2026

ROSTER_SPREADSHEET_ID = st.secrets["google"]["roster_spreadsheet_id"]
REVIEW_SPREADSHEET_ID = st.secrets["google"]["review_spreadsheet_id"]

SERVING_BASE_TAB = "ServingBase"
SERVING_BASE_NAME_COLUMN = "Serving Girl"

COUNTING_RULES_TAB = "Counting Rules"
COUNTING_ERRORS_TAB = "Counting Errors"

COUNTING_RULE_POSITION_COLUMN = "Position"
COUNTING_RULE_COUNTS_COLUMN = "Counts As Serving"

REVIEW_TAB = f"Serving Review {YEAR}"
STATS_TAB = "Serving Statistics"

SCHEDULE_TABS = [
    "January 2026",
    "February 2026",
    "March 2026",
    "April 2026",
    "May 2026",
    "June 2026",
    "July 2026",
    "August 2026",
    "September 2026",
    "October 2026",
    "November 2026",
    "December 2026",
]

YES_VALUES = {"yes", "y", "true", "1"}
NO_VALUES = {"no", "n", "false", "0"}


# =========================
# GOOGLE CONNECTION
# =========================

def get_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_info(
        st.secrets["google"],
        scopes=scopes,
    )

    return gspread.authorize(credentials)


def get_or_create_sheet(spreadsheet, title, rows=1000, cols=80):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


# =========================
# TEXT HELPERS
# =========================

def clean_text(value):
    if value is None:
        return ""

    value = str(value)
    value = value.replace("\n", " ")
    value = value.replace("\xa0", " ")
    value = value.strip()
    value = re.sub(r"\s+", " ", value)

    return value


def clean_name(value):
    value = clean_text(value)

    value = re.sub(r"\([^)]*\)", "", value).strip()
    value = re.sub(r"\bO:\s*", "", value).strip()
    value = value.replace("A:", "").strip()

    return value


def normalize(value):
    value = clean_text(value).lower()
    value = value.replace("–", "-")
    value = value.replace("—", "-")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_name(value):
    return normalize(clean_name(value))


def split_names(cell_value):
    if not cell_value:
        return []

    text = clean_text(cell_value)

    parts = re.split(r";|,|&|\band\b", text)

    names = []

    for part in parts:
        name = clean_name(part)

        if name and len(name) >= 3:
            names.append(name)

    return names


# =========================
# DATE HELPERS
# =========================

def all_sundays_for_year(year):
    d = date(year, 1, 1)

    while d.weekday() != 6:
        d += timedelta(days=1)

    sundays = []

    while d.year == year:
        sundays.append(d)
        d += timedelta(days=7)

    return sundays


def display_date(d):
    return d.strftime("%d %b").lstrip("0")


def parse_schedule_date(value, fallback_year):
    if not value:
        return None

    value = clean_text(value)

    match = re.search(r"(\d{1,2})\s+([A-Za-z]+)", value)

    if not match:
        return None

    day = int(match.group(1))
    month_text = match.group(2)[:3].title()

    try:
        return datetime.strptime(
            f"{day} {month_text} {fallback_year}",
            "%d %b %Y"
        ).date()
    except ValueError:
        return None


# =========================
# SERVING BASE
# =========================

def get_serving_base_names(review_spreadsheet):
    ws = review_spreadsheet.worksheet(SERVING_BASE_TAB)
    rows = ws.get_all_records()

    serving_base = {}

    for row in rows:
        name = clean_name(row.get(SERVING_BASE_NAME_COLUMN, ""))

        if name:
            serving_base[normalize_name(name)] = name

    return serving_base


# =========================
# COUNTING RULES
# =========================

def setup_counting_rules_sheet(review_spreadsheet):
    ws = get_or_create_sheet(
        review_spreadsheet,
        COUNTING_RULES_TAB,
        rows=500,
        cols=5,
    )

    values = ws.get_all_values()

    if not values:
        starter = [
            [COUNTING_RULE_POSITION_COLUMN, COUNTING_RULE_COUNTS_COLUMN],
            ["Oversight", "Yes"],
            ["Main Director", "Yes"],
            ["Babies Leader Age 1", "Yes"],
            ["Age 1", "Yes"],
            ["Age 1 Bags Girls", "Yes"],
            ["Age 1 Bags Boys", "Yes"],
            ["Age 1 Nappies", "Yes"],
            ["Babies Leader Age 2", "Yes"],
            ["Age 2", "Yes"],
            ["Age 2 Bags Girls", "Yes"],
            ["Age 2 Bags Boys", "Yes"],
            ["Age 2 Nappies", "Yes"],
            ["Pre-School Leader Age 3", "Yes"],
            ["Age 3", "Yes"],
            ["Age 3 Bags", "Yes"],
            ["Pre-School Leader Age 4", "Yes"],
            ["Age 4", "Yes"],
            ["Pre-School Leader Age 5", "Yes"],
            ["Age 5", "Yes"],
            ["Elementary Leader Age 6", "Yes"],
            ["Age 6", "Yes"],
            ["Elementary Leader Age 7", "Yes"],
            ["Age 7", "Yes"],
            ["Elementary Leader Age 8", "Yes"],
            ["Age 8", "Yes"],
            ["uGroup Age 9", "Yes"],
            ["Age 9", "Yes"],
            ["uGroup Age 10", "Yes"],
            ["Age 10", "Yes"],
            ["uGroup Age 11", "Yes"],
            ["Age 11", "Yes"],
            ["Special Needs", "Yes"],
            ["uGroup Boys Morning", "No"],
            ["uGroup Boys Evening", "No"],
            ["Director Roaming Inside Age 1-3", "No"],
            ["Director Roaming Inside Age 4-6", "No"],
            ["Director Roaming Inside Age 7-9", "No"],
            ["Director Roaming Inside Age 10-SN", "No"],
        ]

        ws.update(starter, "A1")

    return ws


def get_counting_rules(review_spreadsheet):
    setup_counting_rules_sheet(review_spreadsheet)

    ws = review_spreadsheet.worksheet(COUNTING_RULES_TAB)
    values = ws.get_all_values()

    if not values:
        return {}

    header = [clean_text(h) for h in values[0]]

    try:
        position_idx = header.index(COUNTING_RULE_POSITION_COLUMN)
        counts_idx = header.index(COUNTING_RULE_COUNTS_COLUMN)
    except ValueError:
        st.error(
            "Counting Rules headings must be exactly: "
            "`Position` and `Counts As Serving`"
        )
        return {}

    rules = {}

    for row in values[1:]:
        position = clean_text(row[position_idx]) if position_idx < len(row) else ""
        counts_value = clean_text(row[counts_idx]) if counts_idx < len(row) else ""

        if not position:
            continue

        value = normalize(counts_value)

        if value in YES_VALUES:
            rules[normalize(position)] = True
        elif value in NO_VALUES:
            rules[normalize(position)] = False

    return rules


def position_counts(position, rules):
    position_norm = normalize(position)

    if not position_norm:
        return None

    if position_norm in rules:
        return rules[position_norm]

    for rule_position, counts in rules.items():
        if not rule_position:
            continue

        if rule_position in position_norm:
            return counts

        if position_norm in rule_position:
            return counts

    return None


# =========================
# EXTRACT ROSTER
# =========================

def is_stop_row(position):
    position_norm = normalize(position)

    if not position_norm:
        return False

    stop_words = {
        "morning",
        "evening",
        "brooklyn",
        "tygerberg",
        "nelspruit",
        "polokwane",
    }

    return position_norm in stop_words


def find_person_columns(values, header_row_idx, date_col_idx):
    # Your main roster layout:
    # Column A = Position
    # Date columns = serving girl names
    if date_col_idx > 0:
        return [date_col_idx], 0

    return [date_col_idx], max(0, date_col_idx - 1)


def extract_serving_from_schedule_tab(ws, year):
    values = ws.get_all_values()

    if not values:
        return []

    found = []

    for row_idx, row in enumerate(values):
        for col_idx, cell in enumerate(row):
            service_date = parse_schedule_date(cell, year)

            if not service_date:
                continue

            if service_date.year != year:
                continue

            if service_date.weekday() != 6:
                continue

            person_cols, role_col = find_person_columns(values, row_idx, col_idx)

            for r in range(row_idx + 1, len(values)):
                if role_col >= len(values[r]):
                    continue

                position = clean_text(values[r][role_col])

                if parse_schedule_date(position, year):
                    break

                if is_stop_row(position):
                    continue

                if not position:
                    continue

                for person_col in person_cols:
                    if person_col >= len(values[r]):
                        continue

                    cell_value = values[r][person_col]

                    if parse_schedule_date(cell_value, year):
                        continue

                    names = split_names(cell_value)

                    for name in names:
                        found.append({
                            "Name": name,
                            "Date": service_date,
                            "Position": position,
                            "Source Tab": ws.title,
                        })

    return found


def collect_all_serving_data(roster_spreadsheet, review_spreadsheet):
    serving_base = get_serving_base_names(review_spreadsheet)
    counting_rules = get_counting_rules(review_spreadsheet)

    all_records = []
    counting_errors = []

    existing_tabs = [ws.title for ws in roster_spreadsheet.worksheets()]

    for tab_name in SCHEDULE_TABS:
        if tab_name not in existing_tabs:
            continue

        ws = roster_spreadsheet.worksheet(tab_name)
        records = extract_serving_from_schedule_tab(ws, YEAR)

        for record in records:
            normalized_name = normalize_name(record["Name"])

            if normalized_name not in serving_base:
                continue

            count_result = position_counts(record["Position"], counting_rules)

            if count_result is None:
                counting_errors.append({
                    "Position": record["Position"],
                    "Normalized Position": normalize(record["Position"]),
                    "First Found In": record["Source Tab"],
                    "Date": display_date(record["Date"]),
                    "Example Name": serving_base[normalized_name],
                    "Status": "Counting Error",
                })
                continue

            if count_result is False:
                continue

            all_records.append({
                "Name": serving_base[normalized_name],
                "Date": record["Date"],
                "Position": record["Position"],
                "Source Tab": record["Source Tab"],
            })

    serving_df = pd.DataFrame(all_records)

    if serving_df.empty:
        serving_df = pd.DataFrame(
            columns=["Name", "Date", "Position", "Source Tab"]
        )
    else:
        serving_df = serving_df.drop_duplicates(
            subset=["Name", "Date", "Position"]
        )
        serving_df = serving_df.sort_values(["Name", "Date", "Position"])

    errors_df = pd.DataFrame(counting_errors)

    if errors_df.empty:
        errors_df = pd.DataFrame(
            columns=[
                "Position",
                "Normalized Position",
                "First Found In",
                "Date",
                "Example Name",
                "Status",
            ]
        )
    else:
        errors_df = errors_df.drop_duplicates(subset=["Position"])
        errors_df = errors_df.sort_values(["Position"])

    return serving_df, errors_df, counting_rules


# =========================
# BUILD REVIEW
# =========================

def build_review_dataframe(serving_df, review_spreadsheet):
    sundays = all_sundays_for_year(YEAR)
    date_headers = [display_date(d) for d in sundays]

    serving_base = get_serving_base_names(review_spreadsheet)
    names = sorted(serving_base.values())

    review = pd.DataFrame({"Name": names})

    for d, header in zip(sundays, date_headers):
        served_names = set(serving_df.loc[serving_df["Date"] == d, "Name"])

        review[header] = review["Name"].apply(
            lambda name: 1 if name in served_names else ""
        )

    return review


def build_statistics_dataframe(review_df):
    sundays = all_sundays_for_year(YEAR)
    date_headers = [display_date(d) for d in sundays]

    rows = []

    for _, row in review_df.iterrows():
        name = row["Name"]

        served_dates = []

        for d, header in zip(sundays, date_headers):
            if str(row.get(header, "")).strip() == "1":
                served_dates.append(d)

        total_serves = len(served_dates)
        last_served = max(served_dates) if served_dates else ""

        max_streak = 0
        temp_streak = 0

        for d in sundays:
            if d in served_dates:
                temp_streak += 1
                max_streak = max(max_streak, temp_streak)
            else:
                temp_streak = 0

        current_streak = 0

        for d in reversed(sundays):
            if d in served_dates:
                current_streak += 1
            else:
                if current_streak > 0:
                    break

        warning = ""

        if max_streak >= 3:
            warning = "Already served 3 weekends consecutively"
        elif current_streak == 2:
            warning = "Next weekend should be blocked in availability"

        rows.append({
            "Name": name,
            "Total Serves": total_serves,
            "Last Served": display_date(last_served) if last_served else "",
            "Current Consecutive Streak": current_streak,
            "Max Consecutive Streak": max_streak,
            "Warning": warning,
        })

    stats = pd.DataFrame(rows)

    if not stats.empty:
        stats = stats.sort_values(
            ["Total Serves", "Name"],
            ascending=[False, True],
        )

    return stats


# =========================
# WRITE SHEETS
# =========================

def write_dataframe_to_sheet(ws, df):
    ws.clear()

    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    ws.update(values, "A1")
    ws.freeze(rows=1, cols=1)


def update_review_and_stats():
    client = get_client()

    roster_spreadsheet = client.open_by_key(ROSTER_SPREADSHEET_ID)
    review_spreadsheet = client.open_by_key(REVIEW_SPREADSHEET_ID)

    serving_df, errors_df, counting_rules = collect_all_serving_data(
        roster_spreadsheet,
        review_spreadsheet,
    )

    errors_ws = get_or_create_sheet(
        review_spreadsheet,
        COUNTING_ERRORS_TAB,
        rows=1000,
        cols=10,
    )

    write_dataframe_to_sheet(errors_ws, errors_df)

    if not errors_df.empty:
        return serving_df, None, None, errors_df, counting_rules

    review_df = build_review_dataframe(serving_df, review_spreadsheet)
    stats_df = build_statistics_dataframe(review_df)

    review_ws = get_or_create_sheet(
        review_spreadsheet,
        REVIEW_TAB,
        rows=1000,
        cols=80,
    )

    stats_ws = get_or_create_sheet(
        review_spreadsheet,
        STATS_TAB,
        rows=1000,
        cols=20,
    )

    write_dataframe_to_sheet(review_ws, review_df)
    write_dataframe_to_sheet(stats_ws, stats_df)

    return serving_df, review_df, stats_df, errors_df, counting_rules


# =========================
# STREAMLIT APP
# =========================

st.set_page_config(
    page_title="uKids Serving Review",
    layout="wide",
)

st.title("uKids Serving Review")

st.info(
    "This app reads the roster, checks names against ServingBase, "
    "then checks each position against Counting Rules. "
    "For the main roster layout, positions are read from column A."
)

st.write("Required tabs in the review spreadsheet:")
st.write(f"- `{SERVING_BASE_TAB}`")
st.write(f"- `{COUNTING_RULES_TAB}`")
st.write(f"- `{COUNTING_ERRORS_TAB}`")
st.write(f"- `{REVIEW_TAB}`")
st.write(f"- `{STATS_TAB}`")

with st.expander("Roster tabs this app will read"):
    st.write(SCHEDULE_TABS)

if st.button("Update Serving Review", type="primary"):
    with st.spinner("Reading roster and checking counting rules..."):
        serving_df, review_df, stats_df, errors_df, counting_rules = update_review_and_stats()

    with st.expander("Debug: Counting rules loaded"):
        st.write(f"Total counting rules loaded: {len(counting_rules)}")
        st.write(sorted(list(counting_rules.keys()))[:100])

    if not errors_df.empty:
        st.error(
            "Counting errors found. Add these positions to the Counting Rules tab, then run the app again."
        )

        st.subheader("Counting Errors")
        st.dataframe(errors_df, use_container_width=True)

        st.stop()

    st.success("Serving Review and Serving Statistics updated successfully.")

    st.subheader("Serving Records Counted")
    st.dataframe(serving_df, use_container_width=True)

    st.subheader(REVIEW_TAB)
    st.dataframe(review_df, use_container_width=True)

    st.subheader(STATS_TAB)
    st.dataframe(stats_df, use_container_width=True)

else:
    st.write("Click **Update Serving Review** to update the review tabs.")
