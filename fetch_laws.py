import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

# ── 설정 ─────────────────────────────────────────────
OC = os.environ.get("LAW_OC", "es5183")
BASE_URL = "https://www.law.go.kr/DRF/lawSearch.do"

# 안전 관련 키워드
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

# 수집 대상 종류
LAW_TYPES = [
    {"code": "2", "label": "고시"},
    {"code": "3", "label": "예규"},
    {"code": "4", "label": "훈령"},
]

# ── Firebase 초기화 ───────────────────────────────────
def init_firebase():
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if not cred_json:
        raise Exception("FIREBASE_CREDENTIALS 환경변수가 없습니다.")
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ── 안전 키워드 포함 여부 ─────────────────────────────
def is_safety_related(title):
    if not title:
        return False
    return any(kw in title for kw in SAFETY_KEYWORDS)

# ── 날짜 포맷 변환 ────────────────────────────────────
def format_date(raw):
    raw = str(raw).strip().replace("-", "").replace(".", "")
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw

# ── XML에서 값 추출 (다양한 태그명 지원) ──────────────
def get_text(item, *tags):
    for tag in tags:
        el = item.find(tag)
        if el is not None and el.text:
            return el.text.strip()
    return ""

# ── 법제처 API 호출 (여러 페이지) ─────────────────────
def fetch_law_list(type_code, max_pages=5):
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
            print(f"    페이지 {page} 응답: {res.status_code}")

            root = ET.fromstring(res.text)

            # 다양한 태그명 시도
            items = (root.findall(".//ordin") or
                     root.findall(".//law") or
                     root.findall(".//item") or
                     root.findall(".//OrdinInfo") or
                     list(root))

            print(f"    → {len(items)}개 항목 (태그: {items[0].tag if items else 'none'})")

            if not items:
                break

            all_items.extend(items)

            # 20개 미만이면 마지막 페이지
            if len(items) < 20:
                break

        except Exception as e:
            print(f"  ❌ API 호출 오류 (페이지 {page}): {e}")
            break

    return all_items

# ── Firestore 저장 ────────────────────────────────────
def save_to_firestore(db, item, type_label):
    # 다양한 태그명으로 제목 추출
    title = get_text(item,
        "법령명", "ordinNm", "법령명한글", "제목",
        "조문제목", "title", "name", "NM"
    )

    if not title:
        # 모든 하위 태그 출력 (디버깅)
        print(f"    ⚠️ 제목 없음. 태그목록: {[c.tag for c in item]}")
        return False

    if not is_safety_related(title):
        return False

    # 날짜 추출
    raw_date = get_text(item,
        "공포일자", "시행일자", "promulgationDt",
        "enforcementDt", "발령일자", "개정일자", "date"
    )
    raw_date = raw_date.replace("-", "").replace(".", "")

    # URL 추출
    law_id = get_text(item, "법령ID", "ordinSeq", "ID", "id")
    source_url = get_text(item, "법령상세링크", "detailLink", "url", "URL")
    if not source_url:
        if law_id:
            source_url = f"https://www.law.go.kr/ordinInfoP.do?ordinSeq={law_id}"
        else:
            source_url = "https://www.moel.go.kr/info/lawinfo/instruction/list.do"

    doc_id = f"notice_{raw_date}_{title[:15].replace(' ', '_')}"

    # 중복 체크
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
    print(f"  ✅ 저장: [{type_label}] {title} ({format_date(raw_date)})")
    return True

# ── 메인 ─────────────────────────────────────────────
def main():
    print("🚀 세이프로 - 법제처 안전 법령 자동 수집 시작")
    print(f"   OC: {OC}")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = init_firebase()
    total_saved = 0

    for law_type in LAW_TYPES:
        print(f"\n🔍 [{law_type['label']}] 수집 중...")
        items = fetch_law_list(law_type["code"])
        print(f"   → 총 {len(items)}개 항목 발견")

        for item in items:
            saved = save_to_firestore(db, item, law_type["label"])
            if saved:
                total_saved += 1

    print(f"\n🎉 완료! 총 {total_saved}개 새 항목 저장됨")

if __name__ == "__main__":
    main()
