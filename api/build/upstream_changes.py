#!/usr/bin/env python
"""Report or bump the bbc/tams commit this implementation is aligned with.

The pinned commit is stored in api/TAMS_UPSTREAM_SHA. Requires an authenticated
GitHub CLI (`gh`).

Usage:
    upstream_changes.py changes [REF]   List upstream changes since the pin
    upstream_changes.py bump [REF]      Set the pin to REF (default: main)
"""

import base64
import json
import re
import subprocess  # nosec B404
import sys
from pathlib import Path

REPO = "bbc/tams"
PIN_FILE = Path(__file__).resolve().parent.parent / "TAMS_UPSTREAM_SHA"
CATEGORIES = [
    ("ADRs", "docs/adr/"),
    ("AppNotes", "docs/appnotes/"),
    ("API spec", "api/"),
]


def gh_api(path):
    # Fixed executable and arguments; path is built from constants and API data
    result = subprocess.run(  # nosec B603 B607
        ["gh", "api", path],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if result.returncode != 0:
        sys.exit(f"gh api {path} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def resolve_sha(ref):
    return gh_api(f"repos/{REPO}/commits/{ref}")["sha"]


def read_pin():
    if not PIN_FILE.exists():
        sys.exit(f"{PIN_FILE} not found; run 'make tams-bump TAMS_REF=<sha>' first")
    return PIN_FILE.read_text(encoding="utf-8").strip()


def category(filename):
    for name, prefix in CATEGORIES:
        if filename.startswith(prefix):
            return name
    return "Other"


def adr_status(filename, ref):
    """Return the front-matter status of an ADR at ref, if it has one."""
    try:
        content = gh_api(f"repos/{REPO}/contents/{filename}?ref={ref}")
    except SystemExit:
        return None
    text = base64.b64decode(content["content"]).decode("utf-8")
    match = re.search(r'^status:\s*"?([^"\n]+)"?', text, re.MULTILINE)
    return match.group(1) if match else None


def print_prs(commits):
    print("\n== Upstream PRs ==")
    seen = {}
    for commit in commits:
        prs = gh_api(f"repos/{REPO}/commits/{commit['sha']}/pulls")
        if not prs:
            message = commit["commit"]["message"].splitlines()[0]
            print(f"  {commit['sha'][:7]} {message} (no PR)")
        for pr in prs:
            seen.setdefault(pr["number"], pr)
    for number, pr in sorted(seen.items()):
        touched = sorted(
            {category(f["filename"]) for f in gh_api(f"{pr['url']}/files")}
        )
        print(f"  #{number} {pr['title']} [{', '.join(touched)}]")
        print(f"      {pr['html_url']}")


def print_files(files, head):
    print("\n== Changed files ==")
    for name in [c[0] for c in CATEGORIES] + ["Other"]:
        group = [f for f in files if category(f["filename"]) == name]
        if not group:
            continue
        print(f"{name}:")
        for f in group:
            line = f"  [{f['status']}] {f['filename']}"
            if name == "ADRs" and f["status"] != "removed":
                status = adr_status(f["filename"], head)
                if status:
                    line += f" (status: {status})"
            print(line)


def changes(ref):
    pin = read_pin()
    head = resolve_sha(ref)
    if head == pin:
        print(f"Aligned: pin {pin[:7]} is {REPO}@{ref}.")
        return
    compare = gh_api(f"repos/{REPO}/compare/{pin}...{head}")
    files = compare.get("files", [])
    print(f"{REPO}: {pin[:7]}...{head[:7]} ({ref}), {compare['ahead_by']} commits")
    print(f"https://github.com/{REPO}/compare/{pin[:7]}...{head[:7]}")
    # The compare API caps commits at 250 and files at 300
    if compare["ahead_by"] > len(compare["commits"]) or len(files) >= 300:
        print("WARNING: compare output truncated; review the link above.")
    print_prs(compare["commits"])
    print_files(files, head)
    print(
        "\nAfter reviewing and implementing these, run "
        f"'make tams-bump TAMS_REF={head[:7]}'."
    )


def bump(ref):
    sha = resolve_sha(ref)
    PIN_FILE.write_text(f"{sha}\n", encoding="utf-8")
    print(f"Pinned {REPO}@{ref} -> {sha}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("changes", "bump"):
        sys.exit(__doc__)
    ref = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else "main"
    (changes if sys.argv[1] == "changes" else bump)(ref)


if __name__ == "__main__":
    main()
