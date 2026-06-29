import requests
import duckdb
from pathlib import Path

# --- Konfiguration ---
SCB_URL = "https://api.scb.se/OV0104/v1/doris/sv/ssd/START/BE/BE0101/BE0101A/BefolkningNy"
DB_PATH = "data/bronze.duckdb"
PARQUET_PATH = "data/bronze/population.parquet"

# --- Hämta data från SCB ---
def fetch_population() -> list[dict]:
    query = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "item",
                "values": ["01", "03", "04", "05", "06", "07", "08", "09",
                           "10", "12", "13", "14", "17", "18", "19", "20",
                           "21", "22", "23", "24", "25"]
            }
        },
        {
            "code": "Civilstand",
            "selection": {"filter": "item", "values": ["OG", "G", "SK", "ÄNKL"]}
        },
        {
            "code": "Alder",
            "selection": {"filter": "item", "values": ["tot"]}
        },
        {
            "code": "Kon",
            "selection": {"filter": "item", "values": ["1", "2"]}
        },
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["BE0101N1"]}
        },
        {
            "code": "Tid",
            "selection": {"filter": "item", "values": ["2020", "2021", "2022", "2023", "2024"]}
        }
    ],
    "response": {"format": "json"}
}

    response = requests.post(SCB_URL, json=query)
    print("Status:", response.status_code)
    print("Svar:", response.text)
    response.raise_for_status()
    return response.json()



# --- Platta ut SCB-svaret till rader ---
def parse_response(raw: dict) -> list[dict]:
    columns = [col["text"] for col in raw["columns"]]
    rows = []
    for entry in raw["data"]:
        row = dict(zip(columns, entry["key"] + entry["values"]))
        rows.append(row)
    return rows


# --- Ladda in i DuckDB och exportera Parquet ---
def load_to_duckdb(raw: dict) -> None:
    Path("data/bronze").mkdir(parents=True, exist_ok=True)

    # Hämta kolumnnamn direkt från SCB-svaret
    columns = [
        col["text"]
        .lower()
        .replace("å", "a")
        .replace("ä", "a")
        .replace("ö", "o")
        .replace(" ", "_")
        for col in raw["columns"]
    ]

    # Bygg rader som listor
    rows = []
    for entry in raw["data"]:
        row = entry["key"] + entry["values"]
        rows.append(row)

    con = duckdb.connect(DB_PATH)
    con.execute("DROP TABLE IF EXISTS raw_population")

    # Skapa tabell med rätt kolumnnamn via en relation
    col_defs = ", ".join([f"{col} VARCHAR" for col in columns])
    con.execute(f"CREATE TABLE raw_population ({col_defs})")
    con.executemany(
        f"INSERT INTO raw_population VALUES ({','.join(['?'] * len(columns))})",
        rows
    )

    con.execute(f"COPY raw_population TO '{PARQUET_PATH}' (FORMAT PARQUET)")
    print(f"✅ {len(rows)} rader laddade. Kolumner: {columns}")
    con.close()


if __name__ == "__main__":
    print("Hämtar data från SCB...")
    raw = fetch_population()
    print(f"Hämtade {len(raw['data'])} rader")
    load_to_duckdb(raw)