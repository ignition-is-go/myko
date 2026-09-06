#!/usr/bin/env python3
"""Wrap existing local ViewHandler plans; explicit file list, dry-run by default."""

import argparse
from pathlib import Path
import re


def masked_rust(source):
    # Preserve byte/character offsets while hiding braces inside literals/comments.
    token = re.compile(r'//[^\n]*|/\*.*?\*/|r(?P<hash>\#+)?".*?"(?P=hash)|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])\'', re.S)
    return token.sub(lambda match: " " * len(match.group()), source)


def migrate(source, prefix):
    masked = masked_rust(source)
    edits = []
    for method in re.finditer(r"\bfn\s+build_cell\s*\(", masked):
        arrow = masked.index("->", method.end())
        opening = masked.index("{", arrow)
        signature = source[arrow + 2:opening]
        if "ViewBuildOutput" in signature:
            continue
        result, *bounds = re.split(r"\bwhere\b", signature, maxsplit=1)
        normalized = re.sub(r"\s+", "", result)
        if normalized not in (
            "implhyphae::MapQuery<Key=Arc<str>,Value=Arc<Self::Item>>",
            "implMapQuery<Key=Arc<str>,Value=Arc<Self::Item>>",
        ):
            raise ValueError(f"unsupported build_cell return type: {result.strip()}")
        depth = 1
        closing = opening + 1
        while depth:
            if masked[closing] == "{":
                depth += 1
            elif masked[closing] == "}":
                depth -= 1
            closing += 1
        if re.search(r"\breturn\b", masked[opening + 1:closing - 1]):
            raise ValueError("build_cell contains return; migrate this body explicitly")
        suffix = "where" + bounds[0] if bounds else ""
        header = f" {prefix}::ViewBuildOutput<Item = Self::Item> {suffix}"
        body = source[opening + 1:closing - 1]
        replacement = f"impl{header}{{\n{prefix}::LocalView::new({{{body}}})\n}}"
        edits.append((arrow + 2, closing, replacement))
    for start, end, replacement in reversed(edits):
        source = source[:start] + replacement + source[end:]
    return source, len(edits)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    staged = []
    for name in args.files:
        path = Path(name).resolve()
        prefix = "crate::view" if "/libs/myko/core/" in str(path) else "myko::view"
        try:
            updated, count = migrate(path.read_text(), prefix)
        except ValueError as error:
            raise ValueError(f"{path}: {error}") from error
        if count:
            staged.append((path, updated, count))
    for path, updated, count in staged:
        if args.write:
            path.write_text(updated)
        print(f"{count}\t{path}")


if __name__ == "__main__":
    main()
