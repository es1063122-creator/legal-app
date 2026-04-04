import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

print("=== fetch_laws.py 버전 v6 ===")

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

LAW_TYPES = [
    {"code": "2", "label": "고시"},
    {"code": "3", "label": "예규"},
    {"code": "4", "label": "훈령"},
]

# 법제처 API - 키워드별 검색
SEARCH_QUERIES = ["산업안전", "안전보건", "중대재해", "재해예방", "안전관리"]

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

def fetch_laws_api(query, type_code):
    """법제처 행정규칙 API - query 검색"""
    items = []

    # 방법1: query 파라미터
    for url_template in [
        f"https://www.law.go.kr/DRF/lawSearch.do?OC={OC}&target=ordin&type=XML&display=20&page=1&ordinType={type_code}&query={query}",
        f"https://www.law.go.kr/DRF/lawSearch.do?OC={OC}&target=ordin&type=XML&display=20&page=1&ordinType={type_code}&search=1&query={query}",
    ]:
        try:
            res = requests.get(url_template, timeout=15)
            res.encoding = "utf-8"
            root = ET.fromstring(res.text)

            total_el = root.find("totalCnt")
            total = int(total_el.text) if total_el is not None and total_el.text else 0
            print(f"    [{query}/{type_code}] totalCnt={total}")

            if total > 0:
                found = root.findall(".//ordin") or root.findall(".//OrdinInfo")
                print(f"    → ordin 태그: {len(found)}개")
                if found:
                    # 첫 항목 태그 출력
                    first = found[0]
                    print(f"    첫 항목 태그들: {[c.tag for c in first]}")
                    items.extend(found)
                    break
                else:
                    # totalCnt>0인데 ordin이 없으면 전체 XML 출력
                    print(f"    XML: {res.text[:800]}")
                break
        except Exception as e:
            print(f"    ❌ {e}")

    return items

def save_to_firestore(db, item, type_label):
    title = get_text(item,
        "ordinNm", "법령명", "ordin_nm", "ORDIN_NM",
        "제목", "title", "lawNm", "name", "NM"
    )
    if not title or not is_safety_related(title):
        return False

    raw_date = get_text(item,
        "promulgationDt", "공포일자", "시행일자", "enforcementDt", "발령일자"
    ).replace("-", "").replace(".", "")

    law_id = get_text(item, "ordinSeq", "법령ID")
    source_url = get_text(item, "법령상세링크") or (
        f"https://www.law.go.kr/ordinInfoP.do?ordinSeq={law_id}" if law_id
        else "https://www.moel.go.kr/info/lawinfo/instruction/list.do"
    )
    ministry = get_text(item, "ordinOrg", "기관명") or "고용노동부"

    doc_id = f"notice_{raw_date}_{title[:15].replace(' ', '_')}"
    if db.collection("legal_updates").document(doc_id).get().exists:
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
        "summary":    "",
        "importance": "normal",
        "updatedAt":  datetime.now(timezone.utc),
    })
    print(f"  ✅ [{type_label}] {title}")
    return True

def main():
    print(f"🚀 법제처 안전 법령 수집 v6")
    print(f"   OC: {OC}")
    print(f"   실행: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = init_firebase()
    total = 0

    for law_type in LAW_TYPES:
        print(f"\n🔍 [{law_type['label']}]")
        for query in SEARCH_QUERIES:
            items = fetch_laws_api(query, law_type["code"])
            for item in items:
                if save_to_firestore(db, item, law_type["label"]):
                    total += 1

    print(f"\n🎉 완료! {total}개 저장")

if __name__ == "__main__":
    main()
