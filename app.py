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

SPREADSHEET_ID = st.secrets["google"]["spreadsheet_id"]

YEAR = 2026
REVIEW_TAB = f"Serving Review {YEAR}"
STATS_TAB = "Serving Statistics"

SCHEDULE_TABS = [
    "June 2026",
    "July 2026",
    "August 2026",
    "September 2026",
    "October 2026",
    "November 2026",
    "December 2026",
]

IGNORE_WORDS = {
    "",
    "x",
    "X",
    "morning",
    "evening",
    "director",
    "main director",
    "oversight",
    "special needs",
    "uGroup",
    "uKids",
    "babies",
    "age 1",
    "age 2",
    "age 3",
    "age 4",
    "age 5",
    "age 6",
    "age 7",
    "age 8",
}

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


def get_spreadsheet():
    client = get_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_or_create_sheet(spreadsheet, title, rows=500, cols=80):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


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
    return d.strftime("%-d %b")


def parse_schedule_date(value, fallback_year):
    if not value:
        return None

    value = str(value).strip()

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
# NAME CLEANING
# =========================

def clean_name(value):
    if value is None:
        return ""

    value = str(value).strip()

    if not value:
        return ""

    value = value.replace("\n", " ")
    value = re.sub(r"\s+", " ", value)

    # Remove notes like (N)
    value = re.sub(r"\([^)]*\)", "", value).strip()

    # Remove O: notes
    value = re.sub(r"\bO:\s*", "", value).strip()

    # Remove A: notes but keep the actual name after it
    value = value.replace("A:", "").strip()

    return value


def split_names(cell_value):
    if not cell_value:
        return []

    text = str(cell_value).strip()

    # Split common separators used in your sheet
    parts = re.split(r";|,|&|\band\b", text)

    names = []

    for part in parts:
        name = clean_name(part)

        if not name:
            continue

        if name.lower() in {w.lower() for w in IGNORE_WORDS}:
            continue

        if len(name) < 3:
            continue

        # Ignore headings/roles
        role_words = [
            "leader",
            "director",
            "assistant",
            "runner",
            "greeter",
            "wiggle",
            "hall",
            "sound",
            "lights",
            "offering",
            "announcements",
            "store",
            "prep",
            "outside",
            "inside",
            "babies",
            "age",
            "uGroup",
            "uKids",
            "pre-school",
            "elementary",
        ]

        if any(word.lower() in name.lower() for word in role_words):
            continue

        names.append(name)

    return names


# =========================
# READ SCHEDULE OUTPUT
# =========================

def extract_serving_from_schedule_tab(ws, year):
    values = ws.get_all_values()

    if not values:
        return []

    found = []

    for row_idx, row in enumerate(values):
        for col_idx, cell in enumerate(row):
            possible_date = parse_schedule_date(cell, year)

            if not possible_date:
                continue

            # Only use Sundays in the selected year
            if possible_date.year != year or possible_date.weekday() != 6:
                continue

            # Look down this date column until the next date/header section
            for r in range(row_idx + 1, len(values)):
                if col_idx >= len(values[r]):
                    continue

                value = values[r][col_idx]

                # Stop if another date appears in the same column lower down
                if parse_schedule_date(value, year):
                    break

                names = split_names(value)

                for name in names:
                    found.append({
                        "Name": name,
                        "Date": possible_date,
                        "Source Tab": ws.title,
                    })

    return found


def collect_all_serving_data(spreadsheet):
    all_records = []

    existing_tabs = [ws.title for ws in spreadsheet.worksheets()]

    for tab_name in SCHEDULE_TABS:
        if tab_name not in existing_tabs:
            continue

        ws = spreadsheet.worksheet(tab_name)
        records = extract_serving_from_schedule_tab(ws, YEAR)
        all_records.extend(records)

    df = pd.DataFrame(all_records)

    if df.empty:
        return pd.DataFrame(columns=["Name", "Date", "Source Tab"])

    df = df.drop_duplicates(subset=["Name", "Date"])
    df = df.sort_values(["Name", "Date"])

    return df


# =========================
# BUILD REVIEW TAB
# =========================

def build_review_dataframe(serving_df):
    sundays = all_sundays_for_year(YEAR)
    date_headers = [display_date(d) for d in sundays]

    names = sorted(serving_df["Name"].dropna().unique())

    review = pd.DataFrame({"Name": names})

    for d, header in zip(sundays, date_headers):
        served_names = set(serving_df.loc[serving_df["Date"] == d, "Name"])
        review[header] = review["Name"].apply(
            lambda name: 1 if name in served_names else ""
        )

    return review


# =========================
# BUILD STATS TAB
# =========================

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

        current_streak = 0
        max_streak = 0
        temp_streak = 0
        previous_served = False

        for d, header in zip(sundays, date_headers):
            served = d in served_dates

            if served:
                if previous_served:
                    temp_streak += 1
                else:
                    temp_streak = 1
                max_streak = max(max_streak, temp_streak)
            else:
                temp_streak = 0

            previous_served = served

        # Current streak from the latest Sunday backwards
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
    stats = stats.sort_values(["Total Serves", "Name"], ascending=[False, True])

    return stats


# =========================
# WRITE TO GOOGLE SHEETS
# =========================

def write_dataframe_to_sheet(ws, df):
    ws.clear()

    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    if values:
        ws.update(values, "A1")

    ws.freeze(rows=1, cols=1)


def update_review_and_stats():
    spreadsheet = get_spreadsheet()

    serving_df = collect_all_serving_data(spreadsheet)

    if serving_df.empty:
        return None, None, None

    review_df = build_review_dataframe(serving_df)
    stats_df = build_statistics_dataframe(review_df)

    review_ws = get_or_create_sheet(spreadsheet, REVIEW_TAB, rows=500, cols=80)
    stats_ws = get_or_create_sheet(spreadsheet, STATS_TAB, rows=500, cols=20)

    write_dataframe_to_sheet(review_ws, review_df)
    write_dataframe_to_sheet(stats_ws, stats_df)

    return serving_df, review_df, stats_df


# =========================
# STREAMLIT UI
# =========================

st.set_page_config(
    page_title="uKids Serving Review",
    layout="wide",
)

st.title("uKids Serving Review")
st.caption("Creates yearly serving history and serving statistics from the monthly uKids schedule tabs.")

st.info(
    "This app writes `1` under each Sunday where a serving girl served. "
    "The availability app can then read this tab and block dates that would create 3 weekends in a row."
)

with st.expander("Tabs this app will read"):
    st.write(SCHEDULE_TABS)

if st.button("Update Serving Review", type="primary"):
    with st.spinner("Reading schedule tabs and updating review sheets..."):
        serving_df, review_df, stats_df = update_review_and_stats()

    if serving_df is None:
        st.error("No serving data was found. Check that the schedule tab names match the names in the code.")
    else:
        st.success("Serving Review and Serving Statistics tabs updated successfully.")

        st.subheader("Serving Records Found")
        st.dataframe(serving_df, use_container_width=True)

        st.subheader(REVIEW_TAB)
        st.dataframe(review_df, use_container_width=True)

        st.subheader(STATS_TAB)
        st.dataframe(stats_df, use_container_width=True)

else:
    st.write("Click **Update Serving Review** to scan the schedule and update the two review tabs.")
