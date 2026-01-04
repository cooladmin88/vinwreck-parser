import os
import re
import uuid
import time
import requests
from bs4 import BeautifulSoup
from supabase import create_client

# === НАСТРОЙКИ ИЗ GITHUB SECRETS ===
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
BUCKET = os.environ.get("SUPABASE_BUCKET", "lot-photos")

LOT_URLS = [
    u.strip() for u in os.environ.get("LOT_URLS", "").split(",") if u.strip()
]

ALLOW_KEYWORDS = [
    k.strip().lower()
    for k in os.environ.get(
        "ALLOW_KEYWORDS",
        "run and drive,runs and drives,engine starts,starts"
    ).split(",")
    if k.strip()
]

DENY_KEYWORDS = [
    k.strip().lower()
    for k in os.environ.get(
        "DENY_KEYWORDS",
        "parts only,non-repairable,scrap,junk,certificate of destruction"
    ).split(",")
    if k.strip()
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (vinwreck-parser/1.0)"
}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def text_ok(text: str) -> bool:
    t = (text or "").lower()
    if any(bad in t for bad in DENY_KEYWORDS):
        return False
    return any(good in t for good in ALLOW_KEYWORDS)


def safe_int(value: str):
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def upsert_lot(source_url, title, mileage_km, condition_text):
    res = supabase.table("lots").upsert(
        {
            "source": "carstat.info",
            "source_url": source_url,
            "title": title,
            "mileage_km": mileage_km,
            "condition_text": condition_text,
        },
        on_conflict="source_url",
    ).execute()

    return res.data[0]["id"]


def insert_photo(lot_id, path, sort):
    supabase.table("lot_photos").insert(
        {"lot_id": lot_id, "path": path, "sort": sort}
    ).execute()


def upload_photo(path, content, content_type):
    supabase.storage.from_(BUCKET).upload(
        path=path,
        file=content,
        file_options={"content-type": content_type, "upsert": "true"},
    )


def parse_lot(url: str):
    print("Парсим:", url)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    h1 = soup.find("h1")
    title = (
        h1.get_text(" ", strip=True)
        if h1
        else soup.title.get_text(strip=True)
    )

    mileage_match = re.search(
        r"(Mileage|Odometer|Пробег)\s*[:\-]?\s*([\d\., ]+)",
        page_text,
        re.I,
    )
    mileage_km = safe_int(mileage_match.group(2)) if mileage_match else None

    cond_match = re.search(
        r"(Run.*Drive|Runs.*Drives|Engine\s*Starts|Starts|Parts\s*Only|Non[-\s]?repairable)",
        page_text,
        re.I,
    )
    condition_text = cond_match.group(1) if cond_match else ""

    if not text_ok(condition_text):
        print("❌ Пропущен по фильтру:", condition_text)
        return

    # собираем картинки
    img_urls = set()

    for img in soup.find_all("img"):
        src = img.get("src")
        if src:
            img_urls.add(src)

    for a in soup.find_all("a"):
        href = a.get("href")
        if href and "lot-image" in href:
            img_urls.add(href)

    normalized = []
    for u in img_urls:
        if u.startswith("//"):
            u = "https:" + u
        elif u.startswith("/"):
            u = "https://carstat.info" + u

        if "lot-image" in u or re.search(r"\.(jpg|jpeg|png|webp)", u, re.I):
            normalized.append(u)

    normalized = list(dict.fromkeys(normalized))[:25]

    lot_id = upsert_lot(url, title, mileage_km, condition_text)
    print("✅ Лот сохранён:", title)

    for i, img_url in enumerate(normalized, start=1):
        try:
            ir = requests.get(img_url, headers=HEADERS, timeout=30)
            ir.raise_for_status()

            content_type = (
                ir.headers.get("Content-Type", "image/jpeg")
                .split(";")[0]
                .strip()
            )

            ext = "jpg"
            if content_type.endswith("png"):
              ext = "png"
            elif content_type.endswith("webp"):
                ext = "webp"

            path = f"lots/{lot_id}/{i:02d}_{uuid.uuid4().hex}.{ext}"
            upload_photo(path, ir.content, content_type)
            insert_photo(lot_id, path, i)
            time.sleep(0.4)

        except Exception as e:
            print("⚠️ Ошибка фото:", e)


def main():
    if not LOT_URLS:
        raise SystemExit("❗ Нужно указать LOT_URLS")

    for url in LOT_URLS:
        parse_lot(url)


if __name__ == "__main__":
    main()
