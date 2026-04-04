import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

OC = os.environ.get("LAW_OC", "es5183")
BASE_URL = "https://www.law.go.kr/DRF/lawSearch.do"

SAFETY_KEYWORDS = [
    "산업안전", "안전보건", "재해예방", "산업재해",
    "보호구", "안전활동", "안전지도사", "안전보건지도사",
    "안전관리", "보건관리", "위험성평가", "밀폐공간",
    "추락방지", "화학물질", "유해위험", "작업환경",
    "건설안전", "안전검사", "유해인자", "안전교육",
    "보건규칙", "안전규칙", "근로자건강", "안전점검",
    "안전수칙", "안전기준", "안전조치", "산업보건",
    "직업병", "근골격계", "중대재해", "중대산업재해",
]

# 검색 키워드별로 수집
SEARCH_QUERIES = [
    "산업안전",
    "안전보건",
    "중대재해",
    "재해예방",
    "안전관리",
]

LAW_TYPES = [
    {"code": "2", "label": "고시"},
    {"code": "3", "label": "예규"},
    {"code": "4", "label": "훈령"},
]

def init_firebase():
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if not cred_json:
        raise Exception("FIREBASE_CREDENTIALS 환경변수가 없습니다.")
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()

def is_safety_related(title):
    if not title:
        return False
    return any(kw in title for kw in SAFETY_KEYWORDS)

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

def fetch_by_keyword(query, type_code, max_pages=3):
    """키워드로 검색"""
    all_items = []
    for page in range(1, max_pages + 1):
        params = {
            "OC": OC,
            "target": "ordin",
            "type": "XML",
            "display": 20,
            "page": page,
            "ordinType": type_code,
            "query": query,
        }
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            res.encoding = "utf-8"
            root = ET.fromstring(res.text)

            # totalCnt 확인
            total_el = root.find("totalCnt")
            total = int(total_el.text) if total_el is not None and total_el.text else 0

            if page == 1:
                print(f"    [{query}] 총 {total}건")

            if total == 0:
                break

            # ordin 태그 찾기
            items = root.findall(".//ordin")
            if not items:
                items = root.findall(".//OrdinInfo")
            if not items:
                # XML 샘플 출력
                print(f"    XML 샘플: {res.text[:500]}")
                break

            all_items.extend(items)
            if len(items) < 20:
                break

        except Exception as e:
            print(f"    ❌ 오류: {e}")
            break

    return all_items

def save_to_firestore(db, item, type_label):
    title = get_text(item,
        "ordinNm", "법령명", "제목", "title", "lawNm",
        "ordin_nm", "ORDIN_NM", "name"
    )

    if not title or not is_safety_related(title):
        return False

    raw_date = get_text(item,
        "promulgationDt", "공포일자", "시행일자",
        "enforcementDt", "발령일자"
    )
    raw_date = raw_date.replace("-", "").replace(".", "")

    law_id = get_text(item, "ordinSeq", "법령ID", "ID")
    source_url = get_text(item, "법령상세링크", "detailLink")
    if not source_url:
        if law_id:
            source_url = f"https://www.law.go.kr/ordinInfoP.do?ordinSeq={law_id}"
        else:
            source_url = "https://www.moel.go.kr/info/lawinfo/instruction/list.do"

    ministry = get_text(item, "ordinOrg", "org", "기관명") or "고용노동부"

    doc_id = f"notice_{raw_date}_{title[:15].replace(' ', '_')}"
    existing = db.collection("legal_updates").document(doc_id).get()
    if existing.exists:
        return False

    db.collection("legal_updates").document(doc_id).set({
        "category":   "notice",
        "lawType":    type_label,
        "title":      title,
        "ministry":   ministry,
        "lawName":    title,
        "date":       raw_date,
        "postedDate": format_date(raw_date),
        "sourceUrl":  source_url,
        "summary":    get_text(item, "ordinCont", "조문내용", "summary") or "",
        "importance": "normal",
        "updatedAt":  datetime.now(timezone.utc),
    })
    print(f"  ✅ [{type_label}] {title} ({format_date(raw_date)})")
    return True

def main():
    print("🚀 세이프로 - 법제처 안전 법령 자동 수집 시작")
    print(f"   OC: {OC}")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = init_firebase()
    total_saved = 0

    for law_type in LAW_TYPES:
        print(f"\n🔍 [{law_type['label']}] 수집 중...")
        for query in SEARCH_QUERIES:
            items = fetch_by_keyword(query, law_type["code"])
            for item in items:
                if save_to_firestore(db, item, law_type["label"]):
                    total_saved += 1

    print(f"\n🎉 완료! 총 {total_saved}개 새 항목 저장됨")

if __name__ == "__main__":
    main()
