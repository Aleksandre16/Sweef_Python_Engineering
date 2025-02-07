import aiohttp
import asyncio
import json
import csv
from custom_orm import CustomORM # ჩვენი orm-ი

# OpenSea API URL
OPENSEA_API_URL = "https://api.opensea.io/api/v2/collections"
API_KEY = "1bd29433c1d741fa996a9d506e34f490"  # შეცვალე შენი API გასაღებით
HEADERS = {"Accept": "application/json", "X-API-KEY": API_KEY}

orm = CustomORM()  # Custom ORM ობიექტი
orm.create_table()  # ცხრილის შექმნა


async def fetch_collections(session, offset=0, limit=50):
    """API-დან კოლექციების წამოღება."""
    params = {"offset": offset, "limit": limit}
    async with session.get(OPENSEA_API_URL, headers=HEADERS, params=params) as response:
        if response.status != 200:
            print(f"🚨 API ERROR: {response.status}")
            return {}

        data = await response.json()
        return data


async def get_all_collections(max_collections=1000):
    """API-დან კოლექციების სრული მიღება (Ethereum მხოლოდ)."""
    async with aiohttp.ClientSession() as session:
        collections = []
        offset = 0
        limit = 50

        while len(collections) < max_collections:
            data = await fetch_collections(session, offset, limit)

            if not data.get("collections"):
                print(f"🚨 No more collections at offset {offset}. Stopping fetch.")
                break

            collections.extend(data["collections"])
            offset += limit
            await asyncio.sleep(1)  # API-ის ლიმიტი

        if not collections:
            print("🚨 API-დან მონაცემები არ წამოვიდა!")

        print(f"📦 RAW DATA FETCHED: {len(collections)} collections")
        return collections


def transform_data(raw_data):
    """მონაცემების გარდაქმნა ბაზისთვის"""
    transformed = []
    for collection in raw_data:
        if not collection.get("collection") or not collection.get("name"):
            continue  # გამოვტოვოთ ცარიელი ჩანაწერები

        contracts = collection.get("contracts", [])
        extracted_contracts = [c["address"] for c in contracts if "address" in c]

        transformed.append({
            "collection": collection.get("collection", "Unknown"),
            "name": collection.get("name", "Unknown"),
            "description": collection.get("description", "No description"),
            "image_url": collection.get("image_url", ""),
            "owner": collection.get("owner", "Unknown"),
            "twitter_username": collection.get("twitter_username") or "Unknown",
            "contracts": extracted_contracts  # JSON ფორმატის გარეშე, რადგან ORM თავად გარდაქმნის
        })

    print(f"🔄 TRANSFORMED DATA: {len(transformed)} collections")
    return transformed


def save_raw_data(raw_data, filename="opensea_raw.json"):
    """Raw JSON მონაცემების შენახვა Data Lake-ში"""
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(raw_data, file, indent=4, ensure_ascii=False)
    print("✅ Raw data saved successfully!")


def save_cleaned_data(data, filename="opensea_transformed.csv"):
    """გაწმენდილ მონაცემების შენახვა CSV ფორმატში"""
    if not data:
        print("⚠️ No data to save in CSV.")
        return

    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print("✅ Cleaned data saved successfully!")


async def etl_pipeline():
    """ETL პროცესი"""
    print("🚀 ETL Pipeline დაწყებულია...")
    raw_data = await get_all_collections()

    if not raw_data:
        print("🚨 API-დან მონაცემები არ წამოვიდა!")
        return

    save_raw_data(raw_data)  # Raw JSON მონაცემების შენახვა

    transformed_data = transform_data(raw_data)

    if not transformed_data:
        print("⚠️ No transformed data to save.")
        return

    save_cleaned_data(transformed_data)  # გაწმენდილი მონაცემების შენახვა CSV-ში
    orm.insert_data(transformed_data)  # მონაცემების ჩაწერა Custom ORM-ის გამოყენებით

    print("✅ ETL Pipeline დასრულებულია!")