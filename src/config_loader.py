import json
from pathlib import Path

# Project root = parent of src folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG_PATH = PROJECT_ROOT / "config" / "databases.json"

def get_database_config(db_name):
    with open(CONFIG_PATH, "r") as file:
        configs = json.load(file)

    if db_name not in configs:
        raise ValueError(f"Database configuration not found: {db_name}")

    return configs[db_name]


if __name__ == "__main__":
    config = get_database_config("mysql_company")
    print(config)