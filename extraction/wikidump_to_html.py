#!/usr/bin/env python3
"""Convert a MediaWiki XML export dump to a folder of static HTML pages.

Enhancements in this version:
- Adds basic category extraction ([[Category:Name]]) and per-page category footer
- Generates simple category listing pages (category-<slug>.html)
- Strips simple templates {{...}} (non-nested) iteratively
- External link bracket syntax: [http://example.com Label]
- Improved File: handling with optional caption placeholder
- Enhanced File: option parsing (alt=, link=, upright scaling, alignment classes)
- Optional --dump-old-revisions flag: writes older revisions to per-page subdir slug/<rev-id>.html
- Adds optional --media-dir CLI flag (reserved for future media copying)

Still intentionally minimal / not a full wikitext engine.

Usage:
  python wikidump_to_html.py --dump path/to/dump.xml --out outdir [--limit 100] [--dump-old-revisions] [--media-dir media]
"""
from __future__ import annotations
import argparse
import os
import re
import sys
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
from html import escape, unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field

NS = "{http://www.mediawiki.org/xml/export-0.10/}"

HEADING_RE = re.compile(r"^(=+)([^=].*?)(=+)\s*$")
BOLD_ITALIC_RE = re.compile(r"'''''(.*?)'''''")
BOLD_RE = re.compile(r"'''(.*?)'''")
ITALIC_RE = re.compile(r"''(.*?)''")
LINK_RE = re.compile(r"\[\[([^\]|]+?)(?:\|([^\]]+))?]]")
LIST_ITEM_RE = re.compile(r"^([*#]+)\s*(.*)")
FILE_LINK_ANY_RE = re.compile(r"\[\[(File:[^\]]+)]]", re.IGNORECASE)
CATEGORY_LINK_RE = re.compile(r"\[\[Category:([^|\]]+)(?:\|[^\]]*)?]]", re.IGNORECASE)
EXTERNAL_LINK_RE = re.compile(r"\[(https?://[^\s\]]+)(?:\s+([^\]]+))?]")
TEMPLATE_RE = re.compile(r"{{[^{}]*}}")
COLLAPSIBLE_OPEN_RE = re.compile(
    r'<div[^>]*class=["\'][^"\']*collapsible[^"\']*["\'][^>]*>', re.IGNORECASE
)

_slug_strip_re = re.compile(r"[^a-z0-9]+")


def slugify(title: str, used: set[str]) -> str:
    title_norm = (
        unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    )
    lowered = title_norm.lower()
    slug = _slug_strip_re.sub("-", lowered).strip("-") or "page"
    base = slug
    i = 2
    while slug in used:
        slug = f"{base}-{i}"
        i += 1
    used.add(slug)
    return slug


def category_slug(name: str) -> str:
    name_norm = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    )
    core = _slug_strip_re.sub("-", name_norm.lower()).strip("-") or "category"
    return f"category-{core}"


def normalize_title(title: str) -> str:
    t = title.replace("_", " ").strip()
    if not t:
        return t
    if ":" in t:
        ns, rest = t.split(":", 1)
        rest = rest[:1].upper() + rest[1:] if rest else ""
        return f"{ns}:{rest}"
    return t[:1].upper() + t[1:]


def heading_id(text: str) -> str:
    base = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    base = _slug_strip_re.sub("-", base.lower()).strip("-")
    return base or "section"


def yaml_quote(s: str) -> str:
    # Simple YAML-safe double-quoted scalar
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


@dataclass
class Revision:
    id: str
    timestamp: str
    text: str


@dataclass
class Page:
    title: str
    text: str
    slug: str
    categories: List[str] = field(default_factory=list)
    redirect_to: Optional[str] = None
    revisions: List[Revision] = field(default_factory=list)


@dataclass
class RawPage:
    title: str
    revisions: List[Revision]

    @property
    def latest_text(self) -> str:
        return self.revisions[-1].text if self.revisions else ""


class WikiConverter:
    def __init__(self, mode: str = "html"):
        self.page_slugs: Dict[str, str] = {}
        self.mode = mode  # 'html' or 'markdown'

    def _link_for_target(self, target: str, label: Optional[str] = None) -> str:
        label = label if label is not None else target
        if "#" in target:
            base, frag = target.split("#", 1)
        else:
            base, frag = target, None
        norm_base = normalize_title(base)
        slug = None
        candidates = [
            base,
            norm_base,
            base.replace("_", " "),
            norm_base.replace(" ", "_"),
        ]
        seen = set()
        ordered: List[str] = []
        for c in candidates:
            if c not in seen:
                ordered.append(c)
                seen.add(c)
        for key in ordered:
            if key in self.page_slugs:
                slug = self.page_slugs[key]
                break
        if slug:
            href = f"{slug}.html"
            if frag:
                href += f"#{heading_id(frag)}"
            return f"<a href='{escape(href)}'>{escape(label)}</a>"
        pred = (
            _slug_strip_re.sub(
                "-",
                unicodedata.normalize("NFKD", norm_base)
                .encode("ascii", "ignore")
                .decode("ascii")
                .lower(),
            ).strip("-")
            or "page"
        )
        href = f"{pred}.html"
        if frag:
            href += f"#{heading_id(frag)}"
        return f"<a class='missing' href='{escape(href)}'>{escape(label)}</a>"

    def convert(self, title: str, text: str) -> Tuple[str, List[str]]:
        text = text.replace("\r", "")
        redirect_match = re.match(
            r"^\s*#redirect\s*:?\[\[([^\]]+)]]", text, re.IGNORECASE
        )
        if redirect_match:
            target_raw = redirect_match.group(1).strip()
            target_html = self._link_for_target(target_raw)
            html = f"<div class='redirectbox'>Redirect to {target_html}</div>"
            return html, []
        for _ in range(10):
            new_text = TEMPLATE_RE.sub("", text)
            if new_text == text:
                break
            text = new_text
        lines = text.split("\n")
        html_lines: List[str] = []
        list_stack: List[str] = []
        categories: List[str] = []
        cat_set: Set[str] = set()

        def close_lists(to_level: int = 0):
            while len(list_stack) > to_level:
                t = list_stack.pop()
                html_lines.append(f"</{'ul' if t == '*' else 'ol'}>")

        def strip_categories(line: str) -> str:
            def repl(match):
                cat = match.group(1).strip()
                if cat not in cat_set:
                    cat_set.add(cat)
                    categories.append(cat)
                return ""

            return CATEGORY_LINK_RE.sub(repl, line)

        in_collapse = False
        collapse_lines: List[str] = []
        in_table = False
        table_lines: List[str] = []
        for raw in lines:
            line = raw.rstrip("\n")
            if not in_collapse and not in_table and line.lstrip().startswith("{|"):
                in_table = True
                table_lines = [line]
                continue
            if in_table:
                table_lines.append(line)
                if line.strip().startswith("|}"):
                    attrs_line = table_lines[0][2:].strip()
                    header_text = ""
                    rows: List[List[Tuple[str, bool]]] = []
                    current_row: List[Tuple[str, bool]] = []
                    pending_text_lines: List[str] = []

                    def flush_pending_into_cell():
                        nonlocal pending_text_lines, current_row
                        if pending_text_lines:
                            joined = " ".join(pending_text_lines).strip()
                            if joined:
                                current_row.append((joined, False))
                            pending_text_lines = []

                    for tl in table_lines[1:-1]:
                        stripped = tl.rstrip()
                        core = stripped.strip()
                        if not core:
                            continue
                        if core.startswith("|+"):
                            header_text = core[2:].strip()
                            continue
                        if core.startswith("|-"):
                            flush_pending_into_cell()
                            if current_row:
                                rows.append(current_row)
                                current_row = []
                            continue
                        if core.startswith("!") or core.startswith("|"):
                            flush_pending_into_cell()
                            is_header = core.startswith("!")
                            cell_line = core[1:].lstrip()
                            delimiter = (
                                "!!"
                                if is_header and "!!" in cell_line
                                else ("||" if "||" in cell_line else None)
                            )
                            parts = (
                                [cell_line]
                                if not delimiter
                                else cell_line.split(delimiter)
                            )
                            for part in parts:
                                part = part.strip()
                                if "|" in part and re.match(r"^[^|]+\|", part):
                                    attr_split = part.split("|", 1)
                                    part = attr_split[1].strip()
                                part = strip_categories(part)
                                current_row.append((part, is_header))
                            continue
                        else:
                            pending_text_lines.append(core)
                    flush_pending_into_cell()
                    if current_row:
                        rows.append(current_row)
                    close_lists(0)
                    col_counts = [len(r) for r in rows if r]
                    max_cols = max(col_counts) if col_counts else 0
                    key_value_style = max_cols == 2 and len(
                        [r for r in rows if len(r) == 2]
                    ) >= max(1, int(0.6 * len(rows)))
                    header_html = ""
                    if header_text:
                        header_html = f"<header>{self.inline(header_text)}</header>"
                    if self.mode == "markdown":
                        # Produce Markdown representation of table/infobox
                        def _sanitize_md(cell: str) -> str:
                            return cell.replace("|", "\\|").replace("\n", " ").strip()

                        if key_value_style:
                            if header_text:
                                html_lines.append(
                                    f"**{_sanitize_md(self.inline(header_text))}**"
                                )
                            html_lines.append("| Key | Value |")
                            html_lines.append("| --- | ----- |")
                            for r in rows:
                                if len(r) == 2:
                                    k, v = r
                                    k_txt = _sanitize_md(self.inline(k[0]))
                                    v_txt = _sanitize_md(self.inline(v[0]))
                                    html_lines.append(f"| {k_txt} | {v_txt} |")
                                else:
                                    joined = _sanitize_md(
                                        " — ".join(self.inline(c[0]) for c in r)
                                    )
                                    html_lines.append(f"|  | {joined} |")
                        else:
                            if rows:
                                header_row = rows[0]
                                headers = []
                                any_header = any(c[1] for c in header_row)
                                for idx in range(max_cols):
                                    if idx < len(header_row):
                                        txt, is_header = header_row[idx]
                                        headers.append(_sanitize_md(self.inline(txt)))
                                    else:
                                        headers.append("")
                                html_lines.append("| " + " | ".join(headers) + " |")
                                html_lines.append(
                                    "| " + " | ".join(["---"] * max_cols) + " |"
                                )
                                start_idx = 1
                                for r in rows[start_idx:]:
                                    cells = []
                                    for idx in range(max_cols):
                                        if idx < len(r):
                                            cells.append(
                                                _sanitize_md(self.inline(r[idx][0]))
                                            )
                                        else:
                                            cells.append("")
                                    html_lines.append("| " + " | ".join(cells) + " |")
                        in_table = False
                        continue
                    if key_value_style:
                        body_parts = [
                            f"<aside class='collapsible infobox'>{header_html}<dl>"
                        ]
                        for r in rows:
                            if len(r) == 2:
                                k, v = r
                                body_parts.append(
                                    f"<dt>{self.inline(k[0])}</dt><dd>{self.inline(v[0])}</dd>"
                                )
                            else:
                                joined = " — ".join(self.inline(c[0]) for c in r)
                                body_parts.append(f"<dd>{joined}</dd>")
                        body_parts.append("</dl></aside>")
                        html_lines.append("".join(body_parts))
                    else:
                        table_html = [f"<table class='wikitable'>"]
                        if header_html:
                            table_html.append(header_html)
                        for r in rows:
                            table_html.append("<tr>")
                            for cell_text, is_header in r:
                                tag = "th" if is_header else "td"
                                table_html.append(
                                    f"<{tag}>{self.inline(cell_text)}</{tag}>"
                                )
                            table_html.append("</tr>")
                        table_html.append("</table>")
                        html_lines.append("".join(table_html))
                    in_table = False
                continue
            if not in_collapse and COLLAPSIBLE_OPEN_RE.search(line):
                in_collapse = True
                collapse_lines = [line]
                continue
            if in_collapse:
                collapse_lines.append(line)
                if "</div>" in line.lower():
                    block = "\n".join(collapse_lines)
                    header_match = re.search(
                        r"<(strong|b|h[1-6])[^>]*>(.*?)</\1>", block, re.IGNORECASE
                    )
                    header_html = ""
                    if header_match:
                        header_html = f"<header>{header_match.group(2)}</header>"
                    inner = re.sub(r"^<div[^>]*>", "", block, flags=re.IGNORECASE)
                    inner = re.sub(r"</div>\s*$", "", inner, flags=re.IGNORECASE)
                    converted = self.inline(inner)
                    html_lines.append(
                        f"<aside class='collapsible'>{header_html}{converted}</aside>"
                    )
                    in_collapse = False
                continue
            line = strip_categories(line)
            if not line.strip():
                close_lists(0)
                html_lines.append("")
                continue
            m = HEADING_RE.match(line)
            if m:
                close_lists(0)
                level = len(m.group(1))
                level = min(level, 6)
                content = m.group(2).strip()
                hid = heading_id(content)
                html_lines.append(
                    f"<h{level} id='{escape(hid)}'>{escape(content)}</h{level}>"
                )
                continue
            m = LIST_ITEM_RE.match(line)
            if m:
                markers, body = m.groups()
                level = len(markers)
                marker_type = markers[-1]
                current_depth = len(list_stack)
                for i in range(current_depth, level):
                    list_stack.append(marker_type)
                    html_lines.append(f"<{'ul' if marker_type == '*' else 'ol'}>")
                if level < current_depth:
                    close_lists(level)
                if level and list_stack and list_stack[-1] != marker_type:
                    close_lists(level - 1)
                    list_stack.append(marker_type)
                    html_lines.append(f"<{'ul' if marker_type == '*' else 'ol'}>")
                body_html = self.inline(body)
                html_lines.append(f"<li>{body_html}</li>")
                continue
            else:
                close_lists(0)
            html_lines.append(f"<p>{self.inline(line)}</p>")
        close_lists(0)
        html = "\n".join(html_lines)
        return html, categories

    def inline(self, text: str) -> str:
        def file_sub(match):
            inside = match.group(1)
            parts = inside.split("|")
            file_part = parts[0]
            opts = parts[1:]
            filename = file_part[len("File:") :] or "File"
            caption = ""
            width = 600
            height = 400
            base_default_width = 600
            alt_text: Optional[str] = None
            link_target: Optional[str] = None
            classes: List[str] = ["file-ref"]
            alignment_tokens = {"left", "right", "center", "none"}
            frame_tokens = {"thumb", "thumbnail", "frame", "frameless"}
            known_flags = alignment_tokens | frame_tokens | {"upright"}
            size_re = re.compile(r"^(\d+)(?:x(\d+))?px$", re.IGNORECASE)
            upright_re = re.compile(
                r"^upright(?::?=([0-9]*\.?[0-9]+))?$", re.IGNORECASE
            )
            technical_indices: Set[int] = set()
            last_explicit_caption_idx: Optional[int] = None
            explicit_size = False
            for idx, opt in enumerate(opts):
                o = opt.strip()
                if not o:
                    continue
                if o.lower().startswith("alt="):
                    alt_text = o[4:].strip()
                    technical_indices.add(idx)
                    continue
                if o.lower().startswith("link="):
                    link_target = o[5:].strip()
                    technical_indices.add(idx)
                    continue
                m = size_re.match(o)
                if m:
                    try:
                        w = int(m.group(1))
                        if m.group(2):
                            h = int(m.group(2))
                        else:
                            h = int(w * 3 / 4)
                        width, height = w, h
                        explicit_size = True
                    except ValueError:
                        pass
                    technical_indices.add(idx)
                    continue
                um = upright_re.match(o)
                if um:
                    factor = um.group(1)
                    try:
                        scale = float(factor) if factor else 1.0
                        if not explicit_size:
                            width = int(base_default_width * 0.37 * scale)
                            height = int(width * 3 / 4)
                    except ValueError:
                        pass
                    technical_indices.add(idx)
                    continue
                low = o.lower()
                if low in alignment_tokens:
                    classes.append(f"align-{low}")
                    technical_indices.add(idx)
                    continue
                if low in frame_tokens:
                    classes.append(f"mode-{low}")
                    technical_indices.add(idx)
                    continue
                if low in known_flags:
                    technical_indices.add(idx)
                    continue
                last_explicit_caption_idx = idx
            if (
                last_explicit_caption_idx is not None
                and last_explicit_caption_idx not in technical_indices
            ):
                caption = opts[last_explicit_caption_idx].strip()
            width = max(40, min(width, 1600))
            height = max(40, min(height, 1200))
            label_text = filename[:60]
            label_enc = urllib.parse.quote(label_text)
            img_url = f"https://placehold.co/{width}x{height}?text={label_enc}"
            alt_final = alt_text if alt_text is not None else filename
            # Special case: transform YouTube icon file links into plain text link
            if (
                link_target
                and (
                    "youtube.com" in link_target.lower()
                    or "youtu.be" in link_target.lower()
                )
                and "youtube" in filename.lower()
                and not caption
            ):
                if self.mode == "markdown":
                    return f"[YouTube]({link_target})"
                else:
                    return f"<a href='{escape(link_target)}'>YouTube</a>"
            if self.mode == "markdown":
                # Produce a markdown-friendly representation directly to avoid leftover parameters later.
                md_img = f"![{alt_final}]({img_url})"
                if link_target:
                    if not (
                        link_target.startswith("http://")
                        or link_target.startswith("https://")
                    ):
                        link_target_clean = link_target.strip("[]")
                        internal_anchor = self._link_for_target(
                            link_target_clean, label="__PLACEHOLDER__"
                        )
                        m_href = re.search(r"href='([^']+)'", internal_anchor)
                        href = m_href.group(1) if m_href else "#"
                        href = href.replace(".html", ".md")
                        md_img = f"[{md_img}]({href})"
                    else:
                        md_img = f"[{md_img}]({link_target})"
                if caption:
                    # Append caption as italic text following image
                    md_img += f" *{caption}*"
                return md_img
            img_html = f"<img class=\"placeholder-img\" src='{escape(img_url)}' alt='{escape(alt_final)}' loading='lazy'/>"
            if link_target:
                if not (
                    link_target.startswith("http://")
                    or link_target.startswith("https://")
                ):
                    link_target_clean = link_target.strip("[]")
                    internal_anchor = self._link_for_target(
                        link_target_clean, label="__PLACEHOLDER__"
                    )
                    m = re.search(r"href='([^']+)'", internal_anchor)
                    href = m.group(1) if m else "#"
                    img_html = f"<a href='{escape(href)}'>{img_html}</a>"
                else:
                    img_html = f"<a href='{escape(link_target)}'>{img_html}</a>"
            class_attr = " ".join(classes)
            fig = f'<figure class="{escape(class_attr)}">{img_html}<div class="file-name">{escape(filename)}</div>'
            if caption:
                fig += f"<figcaption>{escape(caption)}</figcaption>"
            fig += "</figure>"
            return fig

        text = FILE_LINK_ANY_RE.sub(file_sub, text)

        def link_sub(match):
            target = match.group(1).strip()
            label = match.group(2).strip() if match.group(2) else target
            if target.lower().startswith("file:"):
                return label
            if target.startswith("http://") or target.startswith("https://"):
                return f"<a href='{escape(target)}'>{escape(label)}</a>"
            if target.lower().startswith("category:"):
                cat_name = target.split(":", 1)[1]
                slug = category_slug(cat_name) + ".html"
                return f"<a href='{escape(slug)}'>{escape(label)}</a>"
            return self._link_for_target(target, label)

        text = LINK_RE.sub(link_sub, text)

        def external_sub(match):
            url = match.group(1)
            label = match.group(2).strip() if match.group(2) else url
            return f"<a href='{escape(url)}'>{escape(label)}</a>"

        text = EXTERNAL_LINK_RE.sub(external_sub, text)

        text = BOLD_ITALIC_RE.sub(
            lambda m: f"<strong><em>{escape(m.group(1))}</em></strong>", text
        )
        text = BOLD_RE.sub(lambda m: f"<strong>{escape(m.group(1))}</strong>", text)
        text = ITALIC_RE.sub(lambda m: f"<em>{escape(m.group(1))}</em>", text)
        return text


def iter_pages(dump_path: Path, keep_revisions: bool = False):
    context = ET.iterparse(str(dump_path), events=("start", "end"))
    page_title: Optional[str] = None
    page_revisions: List[Revision] = []
    for event, elem in context:
        tag = elem.tag
        if event == "start" and tag == NS + "page":
            page_title = None
            page_revisions = []
        elif event == "end":
            if tag == NS + "title":
                page_title = elem.text or ""
            elif tag == NS + "revision":
                text_el = elem.find(NS + "text")
                id_el = elem.find(NS + "id")
                ts_el = elem.find(NS + "timestamp")
                rev_id = id_el.text if id_el is not None and id_el.text else "0"
                rev_ts = ts_el.text if ts_el is not None and ts_el.text else ""
                rev_text = text_el.text if text_el is not None and text_el.text else ""
                if keep_revisions:
                    page_revisions.append(
                        Revision(id=rev_id, timestamp=rev_ts, text=rev_text)
                    )
                else:
                    page_revisions = [
                        Revision(id=rev_id, timestamp=rev_ts, text=rev_text)
                    ]
            elif tag == NS + "page":
                if page_title and page_revisions:
                    yield RawPage(title=page_title, revisions=page_revisions)
                elem.clear()


def build_pages(
    pages: List[RawPage],
    outdir: Path,
    limit: Optional[int] = None,
    media_dir: Optional[Path] = None,
    dump_old_revisions: bool = False,
    output_format: str = "html",
):
    outdir.mkdir(parents=True, exist_ok=True)
    used_slugs: set[str] = set()
    converter = WikiConverter(mode=output_format)
    page_objects: List[Page] = []
    for i, raw in enumerate(pages):
        if limit is not None and i >= limit:
            break
        slug = slugify(raw.title, used_slugs)
        converter.page_slugs[raw.title] = slug
        norm = normalize_title(raw.title)
        converter.page_slugs.setdefault(norm, slug)
        converter.page_slugs.setdefault(raw.title.replace(" ", "_"), slug)
        converter.page_slugs.setdefault(norm.replace(" ", "_"), slug)
        page_objects.append(
            Page(
                title=raw.title,
                text=raw.latest_text,
                slug=slug,
                revisions=raw.revisions,
            )
        )

    template_head = """<!DOCTYPE html><html><head><meta charset='utf-8'>
<title>{title}</title>
<link
  rel='stylesheet'
  href='https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css'
>
</head><body>"""
    footer = "<footer>Generated from MediaWiki dump.</footer></body></html>"

    category_map: Dict[str, List[Page]] = {}

    for p in page_objects:
        html_body, cats = converter.convert(p.title, p.text)
        p.categories = cats
        for c in cats:
            category_map.setdefault(c, []).append(p)
        if output_format == "html":
            with (outdir / f"{p.slug}.html").open("w", encoding="utf-8") as f:
                f.write(template_head.format(title=escape(p.title)))
                f.write(
                    f"<nav class='index'><a href='index.html'>&larr; Index</a></nav>"
                )
                f.write(f"<h1>{escape(p.title)}</h1>\n")
                if dump_old_revisions and len(p.revisions) > 1:
                    latest_rev = p.revisions[-1]
                    f.write(
                        f"<div class='revinfo'>Latest revision {escape(latest_rev.id)} @ {escape(latest_rev.timestamp)} (older revisions under {escape(p.slug)}/)</div>"
                    )
                f.write(html_body)
                if p.categories:
                    f.write("<section class='categories'><h2>Categories</h2><ul>")
                    for c in sorted(set(p.categories), key=str.lower):
                        cslug = category_slug(c) + ".html"
                        f.write(f"<li><a href='{cslug}'>{escape(c)}</a></li>")
                    f.write("</ul></section>")
                f.write(footer)
        else:  # markdown
            # Basic markdown conversion from already HTML-ish body; we keep headings and paragraphs.
            # YAML frontmatter for 11ty
            fm_lines = [
                "---",
                f"title: {yaml_quote(p.title)}",
                f"slug: {yaml_quote(p.slug)}",
            ]
            if p.categories:
                fm_lines.append("categories:")
                for c in sorted(set(p.categories), key=str.lower):
                    fm_lines.append(f"  - {yaml_quote(c)}")
            if dump_old_revisions and len(p.revisions) > 1:
                latest_rev = p.revisions[-1]
                fm_lines.append(f"latest_revision: {yaml_quote(latest_rev.id)}")
                fm_lines.append(
                    f"latest_revision_timestamp: {yaml_quote(latest_rev.timestamp)}"
                )
            fm_lines.append("---")
            md_lines: List[str] = []
            if dump_old_revisions and len(p.revisions) > 1:
                latest_rev = p.revisions[-1]
                md_lines.append(
                    f"_Latest revision {latest_rev.id} @ {latest_rev.timestamp}_"
                )
            # Very naive replacements for block tags
            body_for_md = html_body
            # Replace headings already produced as <hN id='...'>text</hN>
            for n in range(6, 0, -1):
                body_for_md = re.sub(
                    rf"<h{n}[^>]*>(.*?)</h{n}>",
                    lambda m: "\n" + ("#" * n) + " " + m.group(1) + "\n",
                    body_for_md,
                )
            body_for_md = re.sub(
                r"<p>(.*?)</p>",
                lambda m: m.group(1) + "\n",
                body_for_md,
                flags=re.DOTALL,
            )
            body_for_md = re.sub(
                r"<li>(.*?)</li>", lambda m: "- " + m.group(1) + "\n", body_for_md
            )
            body_for_md = re.sub(r"</?(ul|ol)>", "", body_for_md)
            body_for_md = re.sub(
                r"<strong>(.*?)</strong>", lambda m: f"**{m.group(1)}**", body_for_md
            )
            body_for_md = re.sub(
                r"<em>(.*?)</em>", lambda m: f"*{m.group(1)}*", body_for_md
            )

            def _anchor_sub(m):
                href = m.group(1)
                label = m.group(2)
                if not (href.startswith("http://") or href.startswith("https://")):
                    href = href.replace(".html", ".md")
                return f"[{label}]({href})"

            body_for_md = re.sub(
                r"<a\s+[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
                _anchor_sub,
                body_for_md,
                flags=re.IGNORECASE | re.DOTALL,
            )

            # Fallback: orphaned size|link=...]] fragments (e.g. 60px|link=https://..]] )
            def _orphan_file_fragment(m):
                url = m.group(1)
                label = "YouTube" if "youtu" in url.lower() else "Link"
                return f"[{label}]({url})"

            body_for_md = re.sub(
                r"\b\d+px\|link=(https?://[^\]]+)]\]",
                _orphan_file_fragment,
                body_for_md,
            )
            # Remove leftover HTML tags from simple constructs (figure, etc.) crudely
            body_for_md = re.sub(r"<[^>]+>", "", body_for_md)
            # Decode HTML entities (e.g., &nbsp; &amp; etc.)
            body_for_md = unescape(body_for_md)
            # Replace non-breaking spaces with regular spaces
            body_for_md = body_for_md.replace("\u00a0", " ")
            md_lines.append(body_for_md.strip())
            if p.categories:
                md_lines.append(
                    "\n**Categories:** "
                    + ", ".join(sorted(set(p.categories), key=str.lower))
                )
            final_md_parts = fm_lines + [l for l in md_lines if l.strip()]
            (outdir / f"{p.slug}.md").write_text(
                "\n\n".join(final_md_parts), encoding="utf-8"
            )
        if dump_old_revisions and len(p.revisions) > 1:
            rev_dir = outdir / p.slug
            rev_dir.mkdir(parents=True, exist_ok=True)
            older = p.revisions[:-1]
            for r in older:
                html_rev, _ = converter.convert(p.title, r.text)
                if output_format == "html":
                    with (rev_dir / f"{r.id}.html").open("w", encoding="utf-8") as rf:
                        rf.write(
                            template_head.format(
                                title=f"{escape(p.title)} (rev {escape(r.id)})"
                            )
                        )
                        rf.write(
                            f"<nav class='index'><a href='../index.html'>&larr; Index</a> | <a href='../{p.slug}.html'>Latest</a></nav>"
                        )
                        rf.write(f"<h1>{escape(p.title)}</h1>")
                        rf.write(
                            f"<div class='revinfo'>Revision {escape(r.id)} @ {escape(r.timestamp)}</div>"
                        )
                        rf.write(html_rev)
                        rf.write(footer)
                else:
                    fm_rev = [
                        "---",
                        f'title: {yaml_quote(p.title + " (rev " + r.id + ")")}',
                        f"page: {yaml_quote(p.title)}",
                        f"revision_id: {yaml_quote(r.id)}",
                        f"revision_timestamp: {yaml_quote(r.timestamp)}",
                        f"slug: {yaml_quote(p.slug)}",
                        "---",
                    ]
                    rev_md_lines = fm_rev + [
                        f"# {p.title} (rev {r.id})",
                        f"_Revision {r.id} @ {r.timestamp}_",
                    ]
                    body_for_md = html_rev
                    for n in range(6, 0, -1):
                        body_for_md = re.sub(
                            rf"<h{n}[^>]*>(.*?)</h{n}>",
                            lambda m: "\n" + ("#" * n) + " " + m.group(1) + "\n",
                            body_for_md,
                        )
                    body_for_md = re.sub(
                        r"<p>(.*?)</p>",
                        lambda m: m.group(1) + "\n",
                        body_for_md,
                        flags=re.DOTALL,
                    )
                    body_for_md = re.sub(
                        r"<li>(.*?)</li>",
                        lambda m: "- " + m.group(1) + "\n",
                        body_for_md,
                    )
                    body_for_md = re.sub(r"</?(ul|ol)>", "", body_for_md)
                    body_for_md = re.sub(
                        r"<strong>(.*?)</strong>",
                        lambda m: f"**{m.group(1)}**",
                        body_for_md,
                    )
                    body_for_md = re.sub(
                        r"<em>(.*?)</em>", lambda m: f"*{m.group(1)}*", body_for_md
                    )

                    def _anchor_sub_rev(m):
                        href = m.group(1)
                        label = m.group(2)
                        if not (
                            href.startswith("http://") or href.startswith("https://")
                        ):
                            href = href.replace(".html", ".md")
                        return f"[{label}]({href})"

                    body_for_md = re.sub(
                        r"<a\s+[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
                        _anchor_sub_rev,
                        body_for_md,
                        flags=re.IGNORECASE | re.DOTALL,
                    )

                    # Orphaned size|link fragments in revisions
                    def _orphan_file_fragment_rev(m):
                        url = m.group(1)
                        label = "YouTube" if "youtu" in url.lower() else "Link"
                        return f"[{label}]({url})"

                    body_for_md = re.sub(
                        r"\b\d+px\|link=(https?://[^\]]+)]\]",
                        _orphan_file_fragment_rev,
                        body_for_md,
                    )
                    body_for_md = re.sub(r"<[^>]+>", "", body_for_md)
                    body_for_md = unescape(body_for_md)
                    body_for_md = body_for_md.replace("\u00a0", " ")
                    rev_md_lines.append(body_for_md.strip())
                    (rev_dir / f"{r.id}.md").write_text(
                        "\n\n".join([l for l in rev_md_lines if l.strip()]),
                        encoding="utf-8",
                    )

    for cat, plist in sorted(category_map.items(), key=lambda kv: kv[0].lower()):
        cslug = category_slug(cat)
        final_slug = cslug
        i = 2
        while final_slug in used_slugs:
            final_slug = f"{cslug}-{i}"
            i += 1
        if output_format == "html":
            cat_filename = final_slug + ".html"
            with (outdir / cat_filename).open("w", encoding="utf-8") as f:
                f.write(template_head.format(title=f"Category: {escape(cat)}"))
                f.write(
                    f"<nav class='index'><a href='index.html'>&larr; Index</a></nav>"
                )
                f.write(f"<h1>Category: {escape(cat)}</h1><ul>")
                for p in sorted(plist, key=lambda x: x.title.lower()):
                    f.write(f"<li><a href='{p.slug}.html'>{escape(p.title)}</a></li>")
                f.write("</ul>")
                f.write(footer)
        else:
            cat_filename = final_slug + ".md"
            lines = [
                "---",
                f'title: {yaml_quote("Category: " + cat)}',
                f"category: {yaml_quote(cat)}",
                "---",
                f"# Category: {cat}",
                "",
                "## Pages",
                "",
            ]
            for p in sorted(plist, key=lambda x: x.title.lower()):
                lines.append(f"- {p.title} ({p.slug}.md)")
            (outdir / cat_filename).write_text("\n".join(lines), encoding="utf-8")

    if output_format == "html":
        with (outdir / "index.html").open("w", encoding="utf-8") as f:
            f.write(template_head.format(title="Index"))
            f.write("<h1>Pages</h1><ul>")
            for p in sorted(page_objects, key=lambda x: x.title.lower()):
                f.write(f"<li><a href='{p.slug}.html'>{escape(p.title)}</a></li>")
            f.write("</ul>")
            if category_map:
                f.write("<h2>Categories</h2><ul>")
                for cat in sorted(category_map.keys(), key=str.lower):
                    f.write(
                        f"<li><a href='{category_slug(cat)}.html'>{escape(cat)}</a></li>"
                    )
                f.write("</ul>")
            f.write(footer)
    else:
        lines = [
            "---",
            'title: "Pages Index"',
            "---",
            "# Pages",
            "",
        ]
        for p in sorted(page_objects, key=lambda x: x.title.lower()):
            lines.append(f"- {p.title} ({p.slug}.md)")
        if category_map:
            lines.append("\n## Categories\n")
            for cat in sorted(category_map.keys(), key=str.lower):
                lines.append(f"- {cat} ({category_slug(cat)}.md)")
        (outdir / "index.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Convert MediaWiki XML dump to static HTML"
    )
    ap.add_argument("--dump", required=True, help="Path to MediaWiki XML export file")
    ap.add_argument("--out", required=True, help="Output directory for HTML pages")
    ap.add_argument("--limit", type=int, help="Limit number of pages (for testing)")
    ap.add_argument(
        "--media-dir",
        help="Path containing media files (optional; not yet implemented)",
    )
    ap.add_argument(
        "--dump-old-revisions",
        action="store_true",
        help="Write older revisions to per-page subdirectory (slug/<rev-id>.html)",
    )
    ap.add_argument(
        "--format",
        choices=["html", "markdown"],
        default="html",
        help="Output format (html or markdown)",
    )
    args = ap.parse_args(argv)

    dump_path = Path(args.dump)
    if not dump_path.exists():
        print("Dump file not found", file=sys.stderr)
        return 1
    outdir = Path(args.out)
    media_dir = Path(args.media_dir) if args.media_dir else None

    pages = list(iter_pages(dump_path, keep_revisions=args.dump_old_revisions))
    build_pages(
        pages,
        outdir,
        limit=args.limit,
        media_dir=media_dir,
        dump_old_revisions=args.dump_old_revisions,
        output_format=args.format,
    )
    if args.format == "html":
        print(f"Done. HTML written to {outdir}")
    else:
        print(f"Done. Markdown written to {outdir}")
    if args.dump_old_revisions:
        print("Older revisions written under each page slug directory.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
