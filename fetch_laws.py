import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

print("=== fetch_laws.py 버전 v5 시작 ===")

SAFETY_KEYWORDS = [
    "산업안전", "안전보건", "재해예방", "산업재해",
    "보호구", "안전활동", "안전지도사", "안전보건지도사",
    "안전관리", "보건관리", "위험성평가", "밀폐공간",
    "추락방지", "화학물질", "유해위험", "작업환경",
    "건설안전", "안전검사", "유해인자", "안전교육",
    "보건규칙", "안전규칙", "근로자건강", "안전점검",
    "안전수칙", "안전기준", "안전조치", "산업보건",
    "중대재해", "중대산업재해", "공정안전",
]

BASE_URL = "https://www.moel.go.kr/info/lawinfo/instruction/list.do"

def init_firebase():
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if not cred_json:
        raise Exception("FIREBASE_CREDENTIALS 없음")
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()

def is_safety_related(title):
    return any(kw in title for kw in SAFETY_KEYWORDS)

def format_date(raw):
    raw = str(raw).strip().replace("-", "").replace(".", "").replace("/", "")
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw

def fetch_moel_notices(max_pages=5):
    """고용노동부 고시·예규·훈령 목록 수집"""
    items = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    for page in range(1, max_pages + 1):
        params = {
            "pageIndex": page,
            "searchType": "",
            "searchWord": "",
        }
        try:
            res = requests.get(BASE_URL, params=params,
                             headers=headers, timeout=15)
            res.encoding = "utf-8"
            soup = BeautifulSoup(res.text, "html.parser")

            rows = soup.select("table tbody tr")
            print(f"  페이지 {page}: {len(rows)}개 행")

            if not rows:
                break

            for row in rows:
                cols = row.select("td")
                if len(cols) < 4:
                    continue

                title_el = row.select_one("td a") or (cols[1] if len(cols) > 1 else None)
                title = title_el.get_text(strip=True) if title_el else ""

                if not title or not is_safety_related(title):
                    continue

                # 날짜
                date_text = cols[-2].get_text(strip=True) if len(cols) >= 3 else ""

                # 링크
                link_el = row.select_one("td a")
                href = ""
                if link_el and link_el.get("href"):
                    href = "https://www.moel.go.kr" + link_el["href"]
                    if not href.startswith("http"):
                        href = BASE_URL

                items.append({
                    "title": title,
                    "date": date_text,
                    "url": href or BASE_URL,
                    "type": "고시",
                })
                print(f"    ✅ {title} ({date_text})")

        except Exception as e:
            print(f"  ❌ 오류: {e}")
            break

    return items

def save_to_firestore(db, item):
    title = item["title"]
    raw_date = item["date"].replace("-", "").replace(".", "").replace("/", "")
    doc_id = f"notice_{raw_date}_{title[:15].replace(' ', '_')}"

    existing = db.collection("legal_updates").document(doc_id).get()
    if existing.exists:
        return False

    db.collection("legal_updates").document(doc_id).set({
        "category":   "notice",
        "lawType":    item.get("type", "고시"),
        "title":      title,
        "ministry":   "고용노동부",
        "lawName":    title,
        "date":       raw_date,
        "postedDate": format_date(raw_date),
        "sourceUrl":  item["url"],
        "summary":    "",
        "importance": "normal",
        "updatedAt":  datetime.now(timezone.utc),
    })
    print(f"  💾 저장: {title}")
    return True

def main():
    print(f"🚀 고용노동부 안전 법령 수집 시작")
    print(f"   실행: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = init_firebase()
    total = 0

    items = fetch_moel_notices(max_pages=5)
    print(f"\n총 {len(items)}개 안전 관련 항목 발견")

    for item in items:
        if save_to_firestore(db, item):
            total += 1

    print(f"\n🎉 완료! {total}개 저장")

if __name__ == "__main__":
    main()
