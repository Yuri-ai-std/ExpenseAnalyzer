import json

def load_monthly_limits():
    try:
        with open("budget_limits.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}

def save_monthly_limits(budget_limits):
    with open("budget_limits.json", "w") as f:
        json.dump(budget_limits, f, indent=4)
