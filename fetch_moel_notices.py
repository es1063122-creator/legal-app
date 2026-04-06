import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin

import firebase_admin
from firebase_admin import credentials, firestore


BASE_LIST_URL = "https://www.moel.go.kr/info/lawinfo/instruction/list.do"
BASE_DOMAIN = "https://www.moel.go.kr"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
}

TITLE_KEYWORDS = [
    "안전", "보건", "산재", "재해", "보호구", "위험", "유해", "중대재해",
    "산업안전", "안전보건", "산업재해", "재해예방", "위험성평가",
    "작업환경", "근로자건강", "안전활동", "안전지도사", "보건지도사",
    "안전기준", "안전조치", "유해위험", "화학물질", "밀폐공간",
]

DEPT_KEYWORDS = [
    "산업안전정책과",
    "산업안전기준과",
    "산업보건정책과",
    "산재예방지원과",
    "직업건강증진팀",
    "산재보상정책과",
    "안전보건감독기획과",
]

BODY_KEYWORDS = [
    "산업안전", "안전보건", "산업재해", "재해예방", "보호구",
    "위험성평가", "유해위험", "안전기준", "안전조치", "중대재해",
    "밀폐공간", "근로자 건강", "작업환경", "산업안전보건",
    "보건지도사", "안전지도사",
]

OUTPUT_JSON_PATH = "data/moel_recent_notices.json"
MAX_PAGES = int(os.environ.get("MOEL_MAX_PAGES", "5"))

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def init_firebase():
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if not cred_json:
        raise Exception("FIREBASE_CREDENTIALS 환경변수가 없습니다.")

    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)

    return firestore.client()


def ensure_dir(path: str):
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def normalize_date(text: str) -> str:
    if not text:
        return ""
    raw = re.sub(r"[^0-9]", "", text)
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return text.strip()


def compact_date(text: str) -> str:
    if not text:
        return ""
    raw = re.sub(r"[^0-9]", "", text)
    return raw[:8] if len(raw) >= 8 else raw


def contains_any(text: str, keywords: list[str]) -> bool:
    if not text:
        return False
    return any(keyword in text for keyword in keywords)


def detect_law_type(number_text: str, title_text: str) -> str:
    merged = f"{number_text} {title_text}"
    if "고시" in merged:
        return "고시"
    if "예규" in merged:
        return "예규"
    if "훈령" in merged:
        return "훈령"
    return "기타"


def is_safety_related(title: str, department: str, body: str = "") -> tuple[bool, str]:
    if contains_any(title, TITLE_KEYWORDS):
        return True, "title"
    if contains_any(department, DEPT_KEYWORDS):
        return True, "department"
    if contains_any(body, BODY_KEYWORDS):
        return True, "body"
    return False, ""


def fetch_list_page(page: int) -> str:
    params = {"pageIndex": page}
    response = SESSION.get(BASE_LIST_URL, params=params, timeout=20)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def parse_list_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []

    table = soup.find("table")
    if not table:
        return rows

    tbody = table.find("tbody")
    if not tbody:
        return rows

    for tr in tbody.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 5:
            continue

        number = clean_text(cols[1].get_text(" ", strip=True)) if len(cols) > 1 else ""
        title_col = cols[2] if len(cols) > 2 else None
        department = clean_text(cols[3].get_text(" ", strip=True)) if len(cols) > 3 else ""
        posted_date = normalize_date(cols[4].get_text(" ", strip=True)) if len(cols) > 4 else ""

        title = ""
        detail_url = ""

        if title_col:
            link = title_col.find("a")
            if link:
                title = clean_text(link.get_text(" ", strip=True))
                href = (link.get("href") or "").strip()
                if href:
                    detail_url = urljoin(BASE_DOMAIN, href)
            else:
                title = clean_text(title_col.get_text(" ", strip=True))

        law_type = detect_law_type(number, title)

        if title:
            rows.append({
                "number": number,
                "title": title,
                "department": department,
                "postedDate": posted_date,
                "detailUrl": detail_url,
                "lawType": law_type,
            })

    return rows


def fetch_detail_text(detail_url: str) -> str:
    if not detail_url:
        return ""

    try:
        response = SESSION.get(detail_url, timeout=20)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or "utf-8"

        soup = BeautifulSoup(response.text, "lxml")

        candidates = [
            soup.select_one(".board_view"),
            soup.select_one(".view_cont"),
            soup.select_one(".bbs_view"),
            soup.select_one("#contents"),
            soup.select_one(".content"),
        ]

        texts = []
        for candidate in candidates:
            if candidate:
                texts.append(candidate.get_text(" ", strip=True))

        if not texts:
            texts.append(soup.get_text(" ", strip=True))

        merged = clean_text(" ".join(texts))
        return merged[:5000]

    except Exception as e:
        print(f"  ⚠ 상세페이지 읽기 실패: {detail_url} / {e}")
        return ""


def build_doc_id(posted_date: str, title: str) -> str:
    safe_title = re.sub(r"[^0-9A-Za-z가-힣]+", "_", title).strip("_")
    safe_title = safe_title[:40]
    return f"notice_{compact_date(posted_date)}_{safe_title}"


def build_summary(title: str, department: str, law_type: str, body: str) -> str:
    parts = [f"[{law_type}] {title}"]

    if department:
        parts.append(f"담당부서: {department}")

    if body:
        snippet = clean_text(body)[:220]
        parts.append(f"본문요약: {snippet}")

    return " | ".join(parts)


def save_to_firestore(db, item: dict, body_text: str, matched_by: str) -> bool:
    title = item.get("title", "")
    department = item.get("department", "")
    posted_date = item.get("postedDate", "")
    detail_url = item.get("detailUrl", "")
    law_type = item.get("lawType", "기타")

    if not title or not posted_date:
        return False

    doc_id = build_doc_id(posted_date, title)
    ref = db.collection("legal_updates").document(doc_id)

    if ref.get().exists:
        print(f"  ↺ 이미 존재: {title}")
        return False

    summary = build_summary(title, department, law_type, body_text)

    ref.set({
        "category": "notice",
        "lawType": law_type,
        "title": title,
        "ministry": "고용노동부",
        "lawName": title,
        "department": department,
        "date": compact_date(posted_date),
        "postedDate": posted_date,
        "sourceUrl": detail_url or BASE_LIST_URL,
        "summary": summary,
        "importance": "high" if matched_by in ("title", "department") else "normal",
        "matchedBy": matched_by,
        "updatedAt": datetime.now(timezone.utc),
    })

    print(f"  ✅ 저장: [{law_type}] {title} ({posted_date}) / match={matched_by}")
    return True


def write_json_snapshot(items: list[dict]):
    ensure_dir(OUTPUT_JSON_PATH)

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
    }

    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    print("🚀 고용노동부 최근 고시/예규/훈령 수집 시작")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   최대 페이지: {MAX_PAGES}")

    db = init_firebase()

    collected_for_json = []
    total_checked = 0
    total_saved = 0

    for page in range(1, MAX_PAGES + 1):
        print(f"\n📄 목록 페이지 수집: {page}")

        try:
            html = fetch_list_page(page)
            rows = parse_list_page(html)
        except Exception as e:
            print(f"  ❌ 목록 페이지 실패: {e}")
            continue

        if not rows:
            print("  ℹ 수집된 행 없음, 종료")
            break

        print(f"  → 발견 행 수: {len(rows)}")

        for row in rows:
            total_checked += 1

            title = row.get("title", "")
            department = row.get("department", "")
            law_type = row.get("lawType", "")
            posted_date = row.get("postedDate", "")
            detail_url = row.get("detailUrl", "")

            matched, matched_by = is_safety_related(title, department, "")
            body_text = ""

            if not matched and detail_url:
                time.sleep(0.3)
                body_text = fetch_detail_text(detail_url)
                matched, matched_by = is_safety_related(title, department, body_text)

            if not matched:
                print(f"  - 제외: [{law_type}] {title}")
                continue

            if not body_text and detail_url:
                time.sleep(0.2)
                body_text = fetch_detail_text(detail_url)

            item_for_json = {
                "title": title,
                "department": department,
                "lawType": law_type,
                "postedDate": posted_date,
                "detailUrl": detail_url,
                "matchedBy": matched_by,
                "summary": build_summary(title, department, law_type, body_text),
            }
            collected_for_json.append(item_for_json)

            if save_to_firestore(db, row, body_text, matched_by):
                total_saved += 1

    collected_for_json.sort(
        key=lambda x: compact_date(x.get("postedDate", "")),
        reverse=True
    )

    write_json_snapshot(collected_for_json)

    print("\n🎉 완료")
    print(f"   전체 검사: {total_checked}건")
    print(f"   안전 관련 JSON 저장: {len(collected_for_json)}건")
    print(f"   Firestore 신규 저장: {total_saved}건")
    print(f"   JSON 경로: {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
