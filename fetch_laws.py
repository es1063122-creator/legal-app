import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

print("=== fetch_laws.py 버전 v7 ===")

OC = os.environ.get("LAW_OC", "es5183")

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

# 고용노동부만 필터
TARGET_ORGS = ["고용노동부", "노동부", "고용부"]

SEARCH_QUERIES = ["산업안전", "안전보건", "중대재해", "재해예방"]

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

def is_target_org(org):
    return any(t in org for t in TARGET_ORGS)

def format_date(raw):
    raw = str(raw).strip().replace("-", "").replace(".", "")
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw

def get_text(item, *tags):
    for tag in tags:
        el = item.find(tag)
        if el is not None and el.text:
            return el.text.strip()
    return ""

def fetch_laws(query, max_pages=3):
    """법제처 자치법규 API - law 태그로 파싱"""
    all_items = []
    for page in range(1, max_pages + 1):
        params = {
            "OC": OC,
            "target": "ordin",
            "type": "XML",
            "display": 20,
            "page": page,
            "query": query,
        }
        try:
            res = requests.get(
                "https://www.law.go.kr/DRF/lawSearch.do",
                params=params, timeout=15
            )
            res.encoding = "utf-8"
            root = ET.fromstring(res.text)

            total_el = root.find("totalCnt")
            total = int(total_el.text) if total_el is not None and total_el.text else 0
            if page == 1:
                print(f"    [{query}] 총 {total}건")
            if total == 0:
                break

            # law 태그로 파싱
            items = root.findall(".//law")
            if not items:
                break

            all_items.extend(items)
            if len(items) < 20:
                break

        except Exception as e:
            print(f"    ❌ {e}")
            break

    return all_items

def save_to_firestore(db, item):
    # 자치법규 태그명 사용
    title = get_text(item, "자치법규명", "법령명", "ordinNm", "lawNm")
    if not title or not is_safety_related(title):
        return False

    org = get_text(item, "지자체기관명", "기관명", "ordinOrg")

    # 고용노동부 관련만 저장 (자치법규는 지자체 → 고용노동부 아님)
    # 고용노동부 고시는 별도 API 필요하므로 일단 안전 관련 전체 저장
    law_type_raw = get_text(item, "자치법규종류")
    if law_type_raw in ["조례", "규칙"]:
        law_type = law_type_raw
    else:
        law_type = "고시"

    raw_date = get_text(item, "공포일자", "시행일자", "promulgationDt")
    raw_date = raw_date.replace("-", "").replace(".", "")

    law_id = get_text(item, "자치법규일련번호", "자치법규ID", "ordinSeq")
    detail_link = get_text(item, "자치법규상세링크", "법령상세링크")
    if detail_link and not detail_link.startswith("http"):
        source_url = f"https://www.law.go.kr{detail_link}".replace("OC=***", f"OC={OC}")
    elif law_id:
        source_url = f"https://www.law.go.kr/ordinInfoP.do?ordinSeq={law_id}"
    else:
        source_url = "https://www.law.go.kr"

    doc_id = f"notice_{raw_date}_{title[:15].replace(' ', '_')}"
    if db.collection("legal_updates").document(doc_id).get().exists:
        return False

    db.collection("legal_updates").document(doc_id).set({
        "category":   "notice",
        "lawType":    law_type,
        "title":      title,
        "ministry":   org or "고용노동부",
        "lawName":    title,
        "date":       raw_date,
        "postedDate": format_date(raw_date),
        "sourceUrl":  source_url,
        "summary":    "",
        "importance": "normal",
        "updatedAt":  datetime.now(timezone.utc),
    })
    print(f"  ✅ [{law_type}] {title} ({format_date(raw_date)}) - {org}")
    return True

def main():
    print(f"🚀 법제처 안전 법령 수집 v7")
    print(f"   OC: {OC}")
    print(f"   실행: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = init_firebase()
    total = 0

    for query in SEARCH_QUERIES:
        print(f"\n🔍 [{query}] 수집 중...")
        items = fetch_laws(query, max_pages=3)
        print(f"   → {len(items)}개 항목")
        for item in items:
            if save_to_firestore(db, item):
                total += 1

    print(f"\n🎉 완료! {total}개 저장")

if __name__ == "__main__":
    main()
