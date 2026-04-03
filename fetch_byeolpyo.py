"""
세이프로 - 법제처 API 별표 자동 수집 스크립트
GitHub Actions fetch_laws.py 에 통합되거나 단독 실행 가능

대상 법령 (별표 포함):
- 산업안전보건기준에 관한 규칙 (MST: 271485)
- 산업안전보건법 시행규칙 (MST: 271484)
- 중대재해 처벌 등에 관한 법률 시행령 (MST: 234347)
"""

import os
import json
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

# ── 설정 ─────────────────────────────────────────────
OC = os.environ.get("LAW_OC", "es5183")
BASE_URL = "https://www.law.go.kr/DRF"

# 별표 수집 대상 법령 (법령MST 번호)
TARGET_LAWS = [
    {
        "mst": "271485",
        "name": "산업안전보건기준에 관한 규칙",
        "short": "안전보건규칙",
    },
    {
        "mst": "271484",
        "name": "산업안전보건법 시행규칙",
        "short": "산안법시행규칙",
    },
    {
        "mst": "207648",
        "name": "산업안전보건법",
        "short": "산안법",
    },
    {
        "mst": "234347",
        "name": "중대재해 처벌 등에 관한 법률 시행령",
        "short": "중처법시행령",
    },
]

# ── Firebase 초기화 ───────────────────────────────────
def init_firebase():
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if cred_json:
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
    else:
        # 로컬 실행시 serviceAccountKey.json 사용
        cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ── 법제처 API: 법령 전문 조회 ────────────────────────
def fetch_law_detail(mst):
    url = f"{BASE_URL}/lawService.do"
    params = {
        "OC": OC,
        "target": "law",
        "MST": mst,
        "type": "XML",
    }
    try:
        res = requests.get(url, params=params, timeout=20)
        res.encoding = "utf-8"
        return ET.fromstring(res.text)
    except Exception as e:
        print(f"  ❌ API 오류 (MST:{mst}): {e}")
        return None

# ── 별표 항목 파싱 ────────────────────────────────────
def parse_byeolpyo(root, law_name):
    """
    법제처 XML에서 별표(附表) 항목을 추출합니다.
    태그: 별표, 서식, 부표 등
    """
    items = []

    # 별표 태그 검색
    for tag in ["별표", "附表", "서식", "부표"]:
        for el in root.iter(tag):
            title = el.findtext("제목") or el.findtext("title") or ""
            content = el.findtext("내용") or el.findtext("content") or ""
            no = el.findtext("번호") or el.findtext("no") or ""

            if not title:
                continue

            items.append({
                "type": tag,
                "no": no.strip(),
                "title": title.strip(),
                "content": content.strip()[:500],  # 최대 500자
                "lawName": law_name,
            })

    # 조문 내 별표 참조 검색 (조문 본문에 별표가 포함된 경우)
    for article in root.iter("조문"):
        article_no = article.findtext("조문번호") or ""
        article_title = article.findtext("조문제목") or ""
        article_content = article.findtext("조문내용") or ""

        # 별표 참조가 있는 조문만
        if "별표" in article_content or "별표" in article_title:
            items.append({
                "type": "조문참조",
                "no": article_no.strip(),
                "title": article_title.strip(),
                "content": article_content.strip()[:500],
                "lawName": law_name,
            })

    return items

# ── Firestore 저장 ────────────────────────────────────
def save_byeolpyo(db, items, law_info):
    saved = 0
    skipped = 0

    for item in items:
        # 안전 관련 키워드 필터
        combined = f"{item['title']} {item['content']}"
        doc_id = (
            f"byeolpyo_{law_info['short']}_{item['type']}"
            f"_{item['no'][:10].replace(' ', '_')}"
        ).replace("/", "_")

        # 중복 체크
        ref = db.collection("legal_updates").document(doc_id)
        if ref.get().exists:
            skipped += 1
            continue

        # 검색 키워드 생성
        keywords = list({
            law_info["name"],
            law_info["short"],
            item["title"],
            item["type"],
            f"별표{item['no']}",
            "별표",
        } | set(item["title"].split()))

        ref.set({
            "category": "law",
            "lawType": f"별표·서식",
            "lawName": law_info["name"],
            "title": f"[별표] {item['title']}",
            "ministry": "고용노동부",
            "date": datetime.now(timezone.utc).strftime("%Y%m%d"),
            "postedDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "sourceUrl": (
                f"https://www.law.go.kr/lsInfoP.do?"
                f"lsiSeq={law_info['mst']}"
            ),
            "summary": item["content"],
            "importance": "normal",
            "searchTokens": keywords,
            "updatedAt": datetime.now(timezone.utc),
        })
        print(f"    ✅ 저장: [{item['type']}] {item['title']}")
        saved += 1

    return saved, skipped

# ── 메인 ─────────────────────────────────────────────
def main():
    print("🚀 세이프로 - 법제처 별표 자동 수집 시작")
    print(f"   OC: {OC}")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    db = init_firebase()
    total_saved = 0
    total_skipped = 0

    for law in TARGET_LAWS:
        print(f"\n🔍 [{law['name']}] 별표 수집 중...")

        root = fetch_law_detail(law["mst"])
        if root is None:
            print(f"  ❌ 데이터 없음, 건너뜀")
            continue

        items = parse_byeolpyo(root, law["name"])
        print(f"  → {len(items)}개 별표·서식 발견")

        if not items:
            # 별표가 없으면 조문 전체를 current_laws 방식으로 저장
            print(f"  ℹ️  별표 없음 - 법령 전문만 저장")
            continue

        saved, skipped = save_byeolpyo(db, items, law)
        total_saved += saved
        total_skipped += skipped

        time.sleep(0.5)  # API 과부하 방지

    print(f"\n🎉 완료! 총 저장 {total_saved}개 / 스킵 {total_skipped}개")

if __name__ == "__main__":
    main()
