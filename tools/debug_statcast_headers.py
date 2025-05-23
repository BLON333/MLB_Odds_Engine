import pandas as pd

def check_statcast_headers(path="data/statcast.csv"):
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip().str.lower()
        headers = list(df.columns)

        print("\n📄 statcast.csv columns found:")
        for col in headers:
            print(f"  - {col}")

        # Check for common name fields
        print("\n🔍 Name column check:")
        if "first_name" in headers and "last_name" in headers:
            print("✅ Found: first_name + last_name")
        elif "last_name, first_name" in headers:
            print("✅ Found: last_name, first_name")
        elif "name" in headers:
            print("✅ Found: name")
        elif "player_name" in headers:
            print("✅ Found: player_name")
        else:
            print("❌ No usable name field found")

    except Exception as e:
        print(f"[❌] Error loading statcast.csv: {e}")

if __name__ == "__main__":
    check_statcast_headers()
