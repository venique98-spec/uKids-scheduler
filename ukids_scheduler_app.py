# ──────────────────────────────────────────────────────────────────────────────
# Updated slot plan with special position sets
# ──────────────────────────────────────────────────────────────────────────────
def build_slot_plan():
    slot_plan = {
        # Age 1
        "Age 1 leader": 1,
        "Age 1 classroom": 5,
        "Age 1 nappies": 1,
        "Age 1 bags girls": 1,
        "Age 1 bags boys": 1,
        # Age 2
        "Age 2 leader": 1,
        "Age 2 classroom": 4,
        "Age 2 nappies": 1,
        "Age 2 bags girls": 1,
        "Age 2 bags boys": 1,
        # Age 3
        "Age 3 leader": 1,
        "Age 3 classroom": 4,
        "Age 3 bags": 1,
        # Age 4
        "Age 4 leader": 1,
        "Age 4 classroom": 4,
        # Age 5
        "Age 5 leader": 1,
        "Age 5 classroom": 3,
        # Age 6
        "Age 6 leader": 1,
        "Age 6 classroom": 3,
        # Age 7
        "Age 7 leader": 1,
        "Age 7 classroom": 2,
        # Age 8
        "Age 8 leader": 1,
        "Age 8 classroom": 2,
        # Age 9
        "Age 9 leader": 1,
        "Age 9 classroom A": 1,
        "Age 9 classroom B": 1,
        # Age 10
        "Age 10 leader": 1,
        "Age 10 classroom": 1,
        # Age 11
        "Age 11 leader": 1,
        "Age 11 classroom": 1,
        # Special Needs
        "Special needs leader": 1,
        "Special needs classroom": 2,

        # New: Extra 3-limit positions
        "Info 1": 1,
        "Info 2": 1,
        "Info 3": 1,
        "Info 4": 1,
        "uKids Setup 1": 1,
        "uKids Setup 2": 1,
        "uKids Setup 3": 1,
        "uKids Setup 4": 1,

        # New: Require #1 role positions
        "Outside assistant 1": 1,
        "Outside assistant 2": 1,
        "Helping Ninja & Check in (Only uKids Leaders)": 1,
        "Helping Ninja 1": 1,
        "Helping Ninja 2": 1,
        "uKids Hall 1": 1,
        "uKids Hall 2": 1,
        "uKids Hall 3": 1,
        "uKids Hall 4": 1,

        # New: No restrictions positions
        "Brooklyn Runner 1": 1,
        "Brooklyn Runner 2": 1,
        "Brooklyn Babies Leader": 1,
        "Brooklyn Babies 1": 1,
        "Brooklyn Babies 2": 1,
        "Brooklyn Babies 3": 1,
        "Brooklyn Pre-school Leader": 1,
        "Brooklyn Pre-School 1": 1,
        "Brooklyn Pre-School 2": 1,
        "Brooklyn Pre-School 3": 1,
        "Brooklyn Pre-School 4": 1,
    }
    return slot_plan

# Special position sets for rules
extra_3_limit_positions = {
    "Info 1", "Info 2", "Info 3", "Info 4",
    "uKids Setup 1", "uKids Setup 2", "uKids Setup 3", "uKids Setup 4"
}

require_1_role_positions = {
    "Outside assistant 1", "Outside assistant 2",
    "Helping Ninja & Check in (Only uKids Leaders)",
    "Helping Ninja 1", "Helping Ninja 2",
    "uKids Hall 1", "uKids Hall 2", "uKids Hall 3", "uKids Hall 4"
}

# ──────────────────────────────────────────────────────────────────────────────
# Updated scheduler with special code rules
# ──────────────────────────────────────────────────────────────────────────────
def schedule_by_slots(long_df, availability, service_dates, special_codes, priorities, max_assignments_per_person=2):
    slot_plan = build_slot_plan()
    slot_rows, slot_to_role = expand_roles_to_slots(slot_plan)
    eligibility = build_eligibility(long_df)
    people = sorted(set(eligibility.keys()) & set(availability.keys()))

    grid = {(slot, d): "" for slot in slot_rows for d in service_dates}
    assign_count = defaultdict(int)
    role_assignments = defaultdict(list)  # person -> list of (role, date, priority)

    for d in service_dates:
        assigned_today = set()
        for slot_row in slot_rows:
            base_role = slot_to_role[slot_row]
            cands = []
            for p in people:
                # Daily double-booking check
                if p in assigned_today:
                    continue
                # Availability check
                if not availability.get(p, {}).get(d, False):
                    continue

                # Assignment limit check
                max_allowed = 3 if base_role in extra_3_limit_positions else max_assignments_per_person
                if assign_count[p] >= max_allowed:
                    continue

                # Special code "D" limit
                if special_codes.get(p) == "D":
                    # Only 1 assignment in their #1 roles for whole month
                    if priorities.get((p, base_role), 0) == 1:
                        if sum(1 for r, dt, pr in role_assignments[p] if pr == 1) >= 1:
                            continue

                # Positions requiring #1 role somewhere
                if base_role in require_1_role_positions:
                    if not any(pr == 1 for r, dt, pr in role_assignments[p]):
                        continue

                # Role eligibility
                elig_roles = eligibility.get(p, set())
                if base_role not in elig_roles and all(normalize(er) != normalize(base_role) for er in elig_roles):
                    continue

                cands.append(p)

            # Apply PL/BL/EL/SL rule
            if priorities:
                filtered_cands = []
                for p in cands:
                    code = special_codes.get(p, "")
                    pr_val = priorities.get((p, base_role), 0)
                    if code in {"PL", "BL", "EL", "SL"} and pr_val == 5:
                        # Only allowed if a D leader already scheduled in same classroom this day
                        age_prefix = base_role.split()[0:2]  # e.g., "Age 1"
                        age_prefix = " ".join(age_prefix)
                        if any(
                            special_codes.get(grid.get((slot, d), ""), "") == "D" and
                            "leader" in slot.lower() and age_prefix in slot
                            for slot in slot_rows
                        ):
                            filtered_cands.append(p)
                    else:
                        filtered_cands.append(p)
                cands = filtered_cands

            if cands:
                cands.sort(key=lambda name: assign_count[name])
                chosen = cands[0]
                grid[(slot_row, d)] = chosen
                assign_count[chosen] += 1
                pr_val = priorities.get((chosen, base_role), 0)
                role_assignments[chosen].append((base_role, d, pr_val))
                assigned_today.add(chosen)

    cols = [d.strftime("%Y-%m-%d") for d in service_dates]
    schedule_df = pd.DataFrame(index=slot_rows, columns=cols)
    for (slot_row, d), name in grid.items():
        schedule_df.loc[slot_row, d.strftime("%Y-%m-%d")] = name
    schedule_df = schedule_df.fillna("")
    return schedule_df, dict(assign_count), role_assignments
