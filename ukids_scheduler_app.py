import pandas as pd
import random

# ========== CONFIG ==========

# Positions allowed a 3rd assignment
EXTRA_3_LIMIT_POSITIONS = {
    "Info 1", "Info 2", "Info 3", "Info 4",
    "uKids Setup 1", "uKids Setup 2", "uKids Setup 3", "uKids Setup 4"
}

# Positions that require person to have a priority 1 assignment somewhere else
REQUIRE_1_ROLE_POSITIONS = {
    "Outside assistant 1", "Outside assistant 2",
    "Helping Ninja & Check in (Only uKids Leaders)",
    "Helping Ninja 1", "Helping Ninja 2",
    "uKids Hall 1", "uKids Hall 2", "uKids Hall 3", "uKids Hall 4"
}

# Positions with no restrictions (original rules)
NO_RESTRICTION_POSITIONS = {
    "Brooklyn Runner 1", "Brooklyn Runner 2",
    "Brooklyn Babies Leader",
    "Brooklyn Babies 1", "Brooklyn Babies 2", "Brooklyn Babies 3",
    "Brooklyn Pre-school Leader",
    "Brooklyn Pre-School 1", "Brooklyn Pre-School 2",
    "Brooklyn Pre-School 3", "Brooklyn Pre-School 4"
}

# ========== LOAD FILES ==========

positions_df = pd.read_excel("Serving Positions.xlsx")
responses_df = pd.read_excel("Responses.xlsx")

# Make sure "Special Code" exists
if "Special Code" not in positions_df.columns:
    positions_df["Special Code"] = ""

# Create lookup for special codes
special_codes = dict(zip(positions_df["Name"], positions_df["Special Code"]))

# ========== HELPER FUNCTIONS ==========

def can_assign(person, pos_name, assigned_count, already_has_priority1, day_classroom_leaders):
    """Return True if person can be assigned to position under rules"""

    code = special_codes.get(person, "")

    # Rule 1: "D" → only 1 assignment in priority 1 roles
    if code == "D":
        if pos_name not in EXTRA_3_LIMIT_POSITIONS and assigned_count[person] >= 1:
            return False

    # Rule 2: PL, BL, EL, SL → Position 5 only if D is leader in same classroom that day
    if code in {"PL", "BL", "EL", "SL"} and pos_name.endswith("5"):
        # Find matching classroom leader
        if not any(c.startswith(pos_name.split()[0]) and special_codes.get(c, "") == "D"
                   for c in day_classroom_leaders):
            return False

    # Rule 3: EXTRA_3_LIMIT_POSITIONS → can have up to 3 assignments total
    if pos_name in EXTRA_3_LIMIT_POSITIONS:
        if assigned_count[person] >= 3:
            return False
    else:
        # Normal rule → max 2
        if assigned_count[person] >= 2 and code != "D":
            return False

    # Rule 4: REQUIRE_1_ROLE_POSITIONS → must already have a priority 1 somewhere
    if pos_name in REQUIRE_1_ROLE_POSITIONS and not already_has_priority1.get(person, False):
        return False

    return True


def schedule_by_slots(slots, positions_df, responses_df):
    """Main scheduling logic"""
    # Track assignments
    assigned = []
    assigned_count = {name: 0 for name in positions_df["Name"]}
    already_has_priority1 = {name: False for name in positions_df["Name"]}

    # Group slots by day for classroom leader checks
    for day, day_slots in slots.groupby("Date"):
        day_classroom_leaders = set()
