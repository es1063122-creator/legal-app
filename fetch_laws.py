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
    "직업병", "근골격계", "소음", "진동", "분진",
    "중대재해", "중대산업재해", "PSM", "공정안전",
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

# 조례 제외 키워드
EXCLUDE_KEYWORDS = ["조례", "시행조례", "자치법규"]

def is_safety_related(title):
    if not title:
        return False
    # 조례 포함 항목 제외
    if any(ex in title for ex in EXCLUDE_KEYWORDS):
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

def fetch_and_parse(type_code, max_pages=5):
    all_items = []
    for page in range(1, max_pages + 1):
        params = {
            "OC": OC,
            "target": "ordin",
            "type": "XML",
            "display": 20,
            "page": page,
            "ordinType": type_code,
            "org": "고용노동부",
        }
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            res.encoding = "utf-8"
            raw_xml = res.text

            # 첫 페이지 첫 실행 시 XML 구조 출력
            if page == 1 and type_code == "2":
                print(f"\n📄 XML 응답 샘플 (처음 1500자):")
                print(raw_xml[:1500])
                print("---")

            root = ET.fromstring(raw_xml)

            # root 직접 순회하며 ordin 찾기
            items = []

            # 방법1: 직접 태그 찾기
            for tag in ["ordin", "OrdinInfo", "law", "item", "result"]:
                found = root.findall(f".//{tag}")
                if found:
                    items = found
                    print(f"    태그 '{tag}'로 {len(found)}개 발견")
                    break

            # 방법2: root의 직계 자식 중 하위가 있는 것
            if not items:
                for child in root:
                    sub = list(child)
                    if sub:
                        items.append(child)
                print(f"    직계 자식에서 {len(items)}개 발견")

            # 방법3: root 자체가 리스트
            if not items:
                items = list(root)
                print(f"    root 자식 {len(items)}개 사용")

            if not items:
                print(f"    ❌ 항목 없음")
                break

            # 첫 항목 구조 출력
            if page == 1 and items and type_code == "2":
                first = items[0]
                print(f"\n🔍 첫 번째 항목 구조:")
                print(f"  태그: {first.tag}")
                print(f"  속성: {first.attrib}")
                print(f"  텍스트: {repr(first.text)}")
                for child in first:
                    print(f"  └ {child.tag}: {repr(child.text)}")
                    for sub in child:
                        print(f"    └ {sub.tag}: {repr(sub.text)}")

            all_items.extend(items)
            if len(items) < 20:
                break

        except Exception as e:
            print(f"  ❌ 오류: {e}")
            break

    return all_items

def save_to_firestore(db, item, type_label):
    # 속성에서 값 추출 시도
    title = (item.attrib.get("법령명") or
             item.attrib.get("ordinNm") or
             item.attrib.get("title") or
             get_text(item, "법령명", "ordinNm", "제목", "title", "name",
                      "조문제목", "NM", "lawNm", "lawName"))

    if not title:
        # 텍스트 직접 사용
        if item.text and item.text.strip():
            title = item.text.strip()

    if not title or not is_safety_related(title):
        return False

    raw_date = (item.attrib.get("공포일자", "") or
                get_text(item, "공포일자", "시행일자", "promulgationDt",
                         "enforcementDt", "발령일자", "개정일자"))
    raw_date = raw_date.replace("-", "").replace(".", "")

    law_id = (item.attrib.get("법령ID", "") or
              get_text(item, "법령ID", "ordinSeq", "ID"))
    source_url = get_text(item, "법령상세링크", "detailLink", "url")
    if not source_url:
        if law_id:
            source_url = f"https://www.law.go.kr/ordinInfoP.do?ordinSeq={law_id}"
        else:
            source_url = "https://www.moel.go.kr/info/lawinfo/instruction/list.do"

    doc_id = f"notice_{raw_date}_{title[:15].replace(' ', '_')}"
    existing = db.collection("legal_updates").document(doc_id).get()
    if existing.exists:
        return False

    db.collection("legal_updates").document(doc_id).set({
        "category":   "notice",
        "lawType":    type_label,
        "title":      title,
        "ministry":   "고용노동부",
        "lawName":    title,
        "date":       raw_date,
        "postedDate": format_date(raw_date),
        "sourceUrl":  source_url,
        "summary":    get_text(item, "조문내용", "summary", "content") or "",
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
        items = fetch_and_parse(law_type["code"])
        print(f"   → 총 {len(items)}개 항목")

        for item in items:
            if save_to_firestore(db, item, law_type["label"]):
                total_saved += 1

    print(f"\n🎉 완료! 총 {total_saved}개 새 항목 저장됨")

if __name__ == "__main__":
    main()
