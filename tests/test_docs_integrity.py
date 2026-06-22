"""Integrity checks for the teaching artifacts (lessons, reference, records).

The lessons/reference/records form a cross-linked graph that is the repo's real
product. These tests are the automated version of the manual rot-hunt: they fail
the moment a cross-link, stylesheet link, citation, or wikilink goes stale.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest
from conftest import LESSONS_DIR, RECORDS_DIR, REFERENCE_DIR, REPO_ROOT

HREF_RE = re.compile(r'href="([^"]+)"')
WIKILINK_RE = re.compile(r"\[\[([^\]]+)\]\]")
LEADING_NUM_RE = re.compile(r"^(\d+)")

ALL_HTML = sorted([*LESSONS_DIR.glob("*.html"), *REFERENCE_DIR.glob("*.html")])
LESSONS = sorted(LESSONS_DIR.glob("*.html"))
RECORDS = sorted(RECORDS_DIR.glob("*.md"))

# Lessons exempt from the primary-source rule: interleaved review/recall lessons
# that re-practice earlier material rather than introduce new content (see NOTES).
NO_CITATION_OK = {"0010-review-and-retrieve", "0016-review-the-fleet"}

# Wikilink targets that intentionally reference a concept rather than a repo file
# (e.g. the learner's "challenge me from first principles" teaching preference,
# which lives in NOTES/memory, not as a learning record). Listed explicitly so the
# resolver still catches genuinely dangling links (typo'd or deleted record slugs).
CONCEPT_WIKILINKS = {"challenge-me-first-principles"}


def _rel(path: Path) -> str:
    """Repo-relative path, for readable parametrize ids."""
    return str(path.relative_to(REPO_ROOT))


def _relative_hrefs(html: Path) -> list[str]:
    """External (http/mailto) links and pure #anchors are not file references."""
    out = []
    for href in HREF_RE.findall(html.read_text()):
        if href.startswith(("http://", "https://", "mailto:", "#")):
            continue
        out.append(href)
    return out


# --- structural invariants ---------------------------------------------------


def test_lessons_exist():
    assert LESSONS, "no lessons found — did the lessons/ dir move?"


@pytest.mark.parametrize("lesson", LESSONS, ids=_rel)
def test_lesson_links_shared_stylesheet(lesson: Path):
    # Every lesson links the one shared stylesheet so the course looks unified.
    assert "../assets/lesson.css" in lesson.read_text(), (
        f"{_rel(lesson)} does not link ../assets/lesson.css"
    )


@pytest.mark.parametrize("lesson", LESSONS, ids=_rel)
def test_lesson_cites_a_primary_source(lesson: Path):
    # The teach skill requires each content lesson to cite an external primary
    # source; review/recall lessons (NO_CITATION_OK) are deliberately exempt.
    if lesson.stem in NO_CITATION_OK:
        pytest.skip(f"{_rel(lesson)} is a review lesson, exempt from citation")
    assert re.search(r'href="https?://', lesson.read_text()), (
        f"{_rel(lesson)} has no external (primary-source) citation"
    )


def test_lessons_are_contiguously_numbered():
    nums = sorted(int(LEADING_NUM_RE.match(p.stem).group(1)) for p in LESSONS)
    assert nums == list(range(1, len(nums) + 1)), f"gap in lesson numbering: {nums}"


def test_records_are_contiguously_numbered():
    nums = sorted(int(LEADING_NUM_RE.match(p.stem).group(1)) for p in RECORDS)
    assert nums == list(range(1, len(nums) + 1)), f"gap in record numbering: {nums}"


# --- link graph --------------------------------------------------------------

_HREF_CASES = [(html, href) for html in ALL_HTML for href in _relative_hrefs(html)]


@pytest.mark.parametrize(
    ("html", "href"),
    _HREF_CASES,
    ids=[f"{_rel(h)}::{href}" for h, href in _HREF_CASES],
)
def test_relative_href_resolves(html: Path, href: str):
    target = (html.parent / href.split("#")[0]).resolve()
    assert target.exists(), f"{_rel(html)} links missing target: {href}"


def _wikilink_cases() -> list[tuple[Path, str]]:
    cases = []
    for md in RECORDS:
        for raw in WIKILINK_RE.findall(md.read_text()):
            target = raw.split("|")[0].split("#")[0].strip()
            cases.append((md, target))
    return cases


_WIKI_CASES = _wikilink_cases()


@pytest.mark.parametrize(
    ("record", "target"),
    _WIKI_CASES,
    ids=[f"{r.name}::{t}" for r, t in _WIKI_CASES],
)
def test_wikilink_resolves(record: Path, target: str):
    # A wikilink points at another record slug, a top-level doc (MISSION/NOTES),
    # or a known concept reference (CONCEPT_WIKILINKS).
    resolved = (
        target in CONCEPT_WIKILINKS
        or (RECORDS_DIR / f"{target}.md").exists()
        or (REPO_ROOT / target).exists()
        or (REPO_ROOT / f"{target}.md").exists()
    )
    assert resolved, f"{record.name} has dangling wikilink: [[{target}]]"


# --- well-formedness ---------------------------------------------------------


@pytest.mark.parametrize("html", ALL_HTML, ids=_rel)
def test_html_parses_and_has_title(html: Path):
    text = html.read_text()

    class _Collector(HTMLParser):
        def __init__(self):
            super().__init__()
            self.has_title = False

        def handle_starttag(self, tag, attrs):
            if tag == "title":
                self.has_title = True

    parser = _Collector()
    parser.feed(text)  # raises on grossly malformed markup
    assert parser.has_title, f"{_rel(html)} has no <title>"
