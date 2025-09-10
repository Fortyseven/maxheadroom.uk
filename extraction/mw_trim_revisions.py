#!/usr/bin/env python3
"""mw_trim_revisions: Trim old revisions from a MediaWiki XML dump.

Overview
========
MediaWiki full-history dumps contain every revision for each page. For many
use‑cases (search index experiments, lightweight mirrors, analytics pre-stage) you
only need the most recent revision (or the most recent N revisions) of every page.
This tool streams an input XML dump and writes a new XML file containing only the
latest revisions per <page> element.

Features
--------
- Streaming mode powered by lxml (low memory; processes one <page> at a time)
- Fallback full-tree mode using the Python standard library when lxml is absent
  (higher memory; loads the entire XML tree)
- Keep the last N revisions (default 1)
- Optional sorting by <timestamp> or revision <id> to select recency if the dump
  ordering is not guaranteed
- Supports gzipped input/output (.gz suffix auto-detected)
- Progress feedback (pages processed) to stderr

Limitations / Notes
-------------------
- Sorting uses lexical comparison for ISO8601 timestamps which matches
  chronological order for MediaWiki dump timestamps.
- Typical dumps list revisions chronologically (oldest -> newest); if that is
  true you can skip --sort-by for best performance.
- Namespace handling is preserved in streaming mode via lxml. In stdlib mode,
  original namespace declarations are preserved by re-serializing the modified
  tree; ordering of namespace declarations may differ.
- The full-tree fallback can consume large amounts of memory on big dumps; use
  lxml for production-scale history dumps.

Usage Examples
--------------
Keep only the latest revision per page (auto streaming if lxml present):
  python mw_trim_revisions.py input.xml -o trimmed.xml

Keep last 5 revisions per page, sorting defensively by timestamp, show progress:
  python mw_trim_revisions.py input.xml --keep 5 --sort-by timestamp --progress -o trimmed.xml

Read from stdin, write to stdout (use '-' for either path):
  zcat input.xml.gz | python mw_trim_revisions.py - --keep 1 > trimmed.xml

Force stdlib (non-streaming) mode (for debugging, or if lxml streaming quirks):
  python mw_trim_revisions.py input.xml -o trimmed.xml --force-stdlib

Exit Codes
----------
0 success
1 argument / runtime error

"""
from __future__ import annotations

import argparse
import gzip
import io
import sys
from typing import Any, BinaryIO, List, Optional, TextIO, Tuple, Union

try:  # Optional dependency
    from lxml import etree as LET  # type: ignore
    _HAVE_LXML = True
except Exception:  # pragma: no cover - environment dependent
    _HAVE_LXML = False
    LET = None  # type: ignore

import xml.etree.ElementTree as ET  # stdlib fallback

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def eprint(*args: object, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def smart_open(path: str, mode: str) -> Union[BinaryIO, TextIO]:
    """Open a file path or '-' (stdin/stdout). Binary/text decided by caller.

    If the path ends with .gz, wrap with gzip. Caller supplies correct mode
    ('rb', 'wb') etc.
    """
    if path == '-':
        if 'r' in mode:
            return sys.stdin.buffer if 'b' in mode else sys.stdin
        else:
            return sys.stdout.buffer if 'b' in mode else sys.stdout
    if path.endswith('.gz'):
        return gzip.open(path, mode)  # type: ignore[arg-type]
    return open(path, mode)  # type: ignore[arg-type]


def localname(tag: str) -> str:
    return tag.split('}')[-1]

# ---------------------------------------------------------------------------
# Streaming implementation (lxml)
# ---------------------------------------------------------------------------

def stream_trim_lxml(inp: BinaryIO, out: BinaryIO, keep: int, sort_by: Optional[str], progress: bool) -> None:
    assert _HAVE_LXML and LET is not None

    context = LET.iterparse(inp, events=('start', 'end'))
    root = None
    pages_processed = 0

    with LET.xmlfile(out, encoding='utf-8') as xf:
        for event, elem in context:
            lname = localname(elem.tag)
            if event == 'start' and root is None:
                root = elem
                with xf.element(root.tag, root.attrib, nsmap=root.nsmap):
                    continue
            if event != 'end':
                continue
            if lname == 'siteinfo':
                xf.write(elem)
                elem.clear()
            elif lname == 'page':
                trim_page_lxml(elem, keep=keep, sort_by=sort_by)
                xf.write(elem)
                pages_processed += 1
                if progress and pages_processed % 1000 == 0:
                    eprint(f"Processed {pages_processed} pages...")
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
    if progress:
        eprint(f"Done. Total pages processed: {pages_processed}")


def trim_page_lxml(page_elem, keep: int, sort_by: Optional[str]):  # type: ignore[no-untyped-def]
    revisions = [c for c in page_elem if localname(c.tag) == 'revision']
    if len(revisions) <= keep:
        return
    if sort_by == 'timestamp':
        rev_infos: List[Tuple[Tuple[str, int], int, Any]] = []
        for rev in revisions:
            rid_elem = rev.find('./{*}id')
            rid = int(rid_elem.text.strip()) if rid_elem is not None and rid_elem.text else -1
            ts_elem = rev.find('./{*}timestamp')
            ts = ts_elem.text.strip() if ts_elem is not None and ts_elem.text else ''
            key = (ts, rid)
            rev_infos.append((key, rid, rev))
        rev_infos.sort(key=lambda x: x[0])
        keep_set = {id_ for _, id_, _ in rev_infos[-keep:]}
    elif sort_by == 'id':
        rev_infos_id: List[Tuple[int, int, Any]] = []
        for rev in revisions:
            rid_elem = rev.find('./{*}id')
            rid = int(rid_elem.text.strip()) if rid_elem is not None and rid_elem.text else -1
            rev_infos_id.append((rid, rid, rev))
        rev_infos_id.sort(key=lambda x: x[0])
        keep_set = {id_ for _, id_, _ in rev_infos_id[-keep:]}
    else:
        # Assume chronological ordering; drop earliest
        for rev in revisions[:-keep]:
            page_elem.remove(rev)
        return
    for rev in revisions:
        rid_elem = rev.find('./{*}id')
        rid = int(rid_elem.text.strip()) if rid_elem is not None and rid_elem.text else -1
        if rid not in keep_set:
            page_elem.remove(rev)

# ---------------------------------------------------------------------------
# Full-tree (stdlib) implementation
# ---------------------------------------------------------------------------

def fulltree_trim_stdlib(inp: BinaryIO, out: BinaryIO, keep: int, sort_by: Optional[str], progress: bool) -> None:
    tree = ET.parse(inp)
    root = tree.getroot()
    pages_processed = 0

    for page in list(root):
        if localname(page.tag) != 'page':
            continue
        trim_page_stdlib(page, keep=keep, sort_by=sort_by)
        pages_processed += 1
        if progress and pages_processed % 1000 == 0:
            eprint(f"Processed {pages_processed} pages...")

    if progress:
        eprint(f"Serialization starting. Pages processed: {pages_processed}")
    tree.write(out, encoding='utf-8', xml_declaration=True)
    if progress:
        eprint("Done.")


def trim_page_stdlib(page_elem: ET.Element, keep: int, sort_by: Optional[str]):
    revisions = [c for c in page_elem if localname(c.tag) == 'revision']
    if len(revisions) <= keep:
        return
    if sort_by == 'timestamp':
        rev_infos: List[Tuple[Tuple[str, int], int, ET.Element]] = []
        for rev in revisions:
            rid_elem = rev.find('id') or next((child for child in rev if localname(child.tag) == 'id'), None)
            rid = int(rid_elem.text.strip()) if (rid_elem is not None and rid_elem.text) else -1
            ts_elem = rev.find('timestamp') or next((child for child in rev if localname(child.tag) == 'timestamp'), None)
            ts = ts_elem.text.strip() if (ts_elem is not None and ts_elem.text) else ''
            rev_infos.append(((ts, rid), rid, rev))
        rev_infos.sort(key=lambda x: x[0])
        keep_set = {id_ for _, id_, _ in rev_infos[-keep:]}
    elif sort_by == 'id':
        rev_infos_id: List[Tuple[int, int, ET.Element]] = []
        for rev in revisions:
            rid_elem = rev.find('id') or next((child for child in rev if localname(child.tag) == 'id'), None)
            rid = int(rid_elem.text.strip()) if (rid_elem is not None and rid_elem.text) else -1
            rev_infos_id.append((rid, rid, rev))
        rev_infos_id.sort(key=lambda x: x[0])
        keep_set = {id_ for _, id_, _ in rev_infos_id[-keep:]}
    else:
        for rev in revisions[:-keep]:
            page_elem.remove(rev)
        return
    for rev in revisions:
        rid_elem = rev.find('id') or next((child for child in rev if localname(child.tag) == 'id'), None)
        rid = int(rid_elem.text.strip()) if (rid_elem is not None and rid_elem.text) else -1
        if rid not in keep_set:
            page_elem.remove(rev)

# ---------------------------------------------------------------------------
# Argument parsing & main
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Trim old revisions from MediaWiki XML dumps (keep latest N).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument('input', help="Input XML dump file path or '-' for stdin.")
    p.add_argument('-o', '--output', default='-', help="Output XML path ('.gz' => gzip) or '-' for stdout.")
    p.add_argument('-k', '--keep', type=int, default=1, help="Number of most recent revisions to keep per page.")
    p.add_argument('--sort-by', choices=['timestamp', 'id'], help="Select most recent revisions by key instead of assuming chronological order in dump.")
    p.add_argument('--progress', action='store_true', help="Print progress (every 1000 pages) to stderr.")
    p.add_argument('--force-stdlib', action='store_true', help="Force full-tree stdlib mode even if lxml is available.")
    p.add_argument('--version', action='version', version='mw_trim_revisions 0.1.0')
    args = p.parse_args(argv)

    if args.keep < 1:
        p.error('--keep must be >= 1')
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    use_streaming = _HAVE_LXML and not args.force_stdlib
    if args.progress:
        mode = 'streaming (lxml)' if use_streaming else 'full-tree (stdlib)'
        eprint(f"Mode: {mode}; keeping last {args.keep} revision(s)")
        if args.sort_by:
            eprint(f"Selecting revisions by: {args.sort_by}")
        if not _HAVE_LXML and not args.force_stdlib:
            eprint("lxml not available; falling back to full-tree mode.")

    try:
        with smart_open(args.input, 'rb') as inf, smart_open(args.output, 'wb') as outf:  # type: ignore[assignment]
            if use_streaming:
                stream_trim_lxml(inf, outf, keep=args.keep, sort_by=args.sort_by, progress=args.progress)  # type: ignore[arg-type]
            else:
                fulltree_trim_stdlib(inf, outf, keep=args.keep, sort_by=args.sort_by, progress=args.progress)  # type: ignore[arg-type]
    except KeyboardInterrupt:  # pragma: no cover
        eprint("Interrupted.")
        return 1
    except Exception as exc:  # pragma: no cover
        eprint(f"Error: {exc}")
        return 1
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
