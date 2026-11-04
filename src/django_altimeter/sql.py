"""Bounded PostgreSQL descriptions, independent of driver and privacy policy.

This lexer removes values; it neither validates nor executes SQL. Identifiers are
visible and must never contain application data. Unsupported or ambiguous input
has no description. The process-local LRU retains at most 1,024 raw queries, each
at most 4,096 characters (16 KiB of UTF-8); larger inputs are never cached. The
input ceiling is 16,384 characters. Nothing is persisted or adapted.
"""

import re
from dataclasses import dataclass
from functools import lru_cache

DB_OPERATIONS = frozenset(
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "EXPLAIN",
        "WITH",
        "CALL",
    }
)
MAX_QUERY_CHARS = 16_384
_MAX_CACHED_QUERY_CHARS = 4096
_SUMMARY_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
_PARAMETER = r"%(?:[sbt%]|\([A-Za-z_][A-Za-z0-9_]*\)[sbt])|\$[0-9]+"
_PARAMETER_TOKEN = re.compile(_PARAMETER)
_NUMBER = (
    r"(?:0[xX][0-9a-fA-F](?:_?[0-9a-fA-F])*"
    r"|0[oO][0-7](?:_?[0-7])*|0[bB][01](?:_?[01])*"
    r"|(?:[0-9](?:_?[0-9])*(?:\.(?:[0-9](?:_?[0-9])*)?)?"
    r"|\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?)(?![\w$])"
)
_QUOTED_IDENTIFIER = r'(?:[uU]&)?"[^"]*(?:""[^"]*)*"'
# Every alternative consumes a complete lexical token. The final alternative
# catches unclosed quotes/comments, nested comments and unsupported characters.
# Operator matching stops before comments and adjacent DB-API placeholders.
_LEXER = re.compile(
    rf"(?:(?P<quoted>{_QUOTED_IDENTIFIER}(?:\.{_QUOTED_IDENTIFIER})*)"
    r"|(?P<space>[ \t\n\r\f\v]+)"
    r"|(?P<symbol>\.\.\.|::|:=|[(),;\[\]]|\.(?![0-9]))"
    r"|(?P<word>(?![eEnNbBxX]'|[uU]&['\"])[^\W\d][\w$]*)"
    r"|(?P<comment>--[^\r\n]*|/\*(?:[^*/]|/(?!\*)|\*(?!/))*\*/)"
    r"|(?P<literal>[eE]'(?:''|\\[\s\S]|[^'\\])*'"
    r"|[uU]&'(?:''|[^'])*'"
    r"|[nNbBxX]?'(?:''|[^'\\])*'"
    r"|\$(?P<tag>[^\W\d]\w*|)\$[\s\S]*?\$(?P=tag)\$)"
    rf"|(?P<parameter>{_PARAMETER})"
    rf"|(?P<number>{_NUMBER})"
    r"|(?P<operator>(?:(?!--|/\*|\*/|%[sbt%]|%\()[+\-*/<>=~!@#%^&|\x60?])+)"
    r"|(?P<invalid>[\s\S]))[ \t\n\r\f\v]*"
)
_PAREN_KEYWORDS = frozenset(
    {
        "IN",
        "VALUES",
        "AS",
        "OVER",
        "FILTER",
        "EXISTS",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "ON",
        "JOIN",
        "FROM",
        "SELECT",
        "WHEN",
        "THEN",
        "ELSE",
        "BY",
        "HAVING",
        "USING",
        "RETURNING",
    }
)


@dataclass(frozen=True)
class SanitizedQuery:
    text: str
    summary: str
    operation: str


def is_query_identifier(value: object) -> bool:
    """Allow only an ASCII identifier or schema.identifier, at most 128 bytes."""
    if type(value) is not str or not value or len(value) > 128:
        return False
    parts = value.split(".")
    return len(parts) <= 2 and all(
        _SUMMARY_IDENTIFIER.fullmatch(part) for part in parts
    )


def sanitize_query(query: object, *, max_text_bytes: int) -> SanitizedQuery | None:
    """Describe a builtin string with bounded work, or decline without adapting it."""
    if (
        type(query) is not str
        or not query
        or len(query) > MAX_QUERY_CHARS
        or type(max_text_bytes) is not int
        or max_text_bytes <= 0
    ):
        return None
    maximum = min(max_text_bytes, MAX_QUERY_CHARS * 4)
    if len(query) <= _MAX_CACHED_QUERY_CHARS:
        return _sanitize(query, maximum)
    return _describe(query, maximum)


def _tokens(query: str) -> list[str] | None:
    if "\0" in query:
        return None
    try:
        query.encode("utf-8")
    except UnicodeEncodeError:
        return None
    tokens = []
    for match in _LEXER.finditer(query):
        kind = match.lastgroup
        if kind in {"space", "comment"}:
            continue
        if kind is None or kind == "invalid":
            return None
        text = match[kind]
        if kind in {"literal", "number"} or (
            kind == "word" and text.upper() in {"TRUE", "FALSE"}
        ):
            text = "?"
        tokens.append(text)
    return tokens


def _placeholder_row(tokens: list[str], start: int) -> int | None:
    """Return the closing parenthesis of a row made only of bind/value markers."""
    if start >= len(tokens) or tokens[start] != "(":
        return None
    position = start + 1
    while position < len(tokens) and (
        tokens[position] == "?" or _PARAMETER_TOKEN.fullmatch(tokens[position])
    ):
        position += 1
        if position < len(tokens) and tokens[position] == ")":
            return position
        if position >= len(tokens) or tokens[position] != ",":
            return None
        position += 1
    return None


def _collapse(tokens: list[str]) -> list[str]:
    result: list[str] = []
    copied = 0
    for index, token in enumerate(tokens):
        if index < copied or token.upper() not in {"IN", "VALUES"}:
            continue
        start = index + 1
        end = _placeholder_row(tokens, start)
        if end is None:
            continue
        row = tokens[start : end + 1]
        if token.upper() == "IN" and len(row) > 3:
            result.extend(tokens[copied:start])
            result.extend(["(", row[1], ",", "...", ")"])
            copied = end + 1
        elif token.upper() == "VALUES":
            following = end + 1
            while following < len(tokens) and tokens[following] == ",":
                row_end = _placeholder_row(tokens, following + 1)
                if row_end is None or tokens[following + 1 : row_end + 1] != row:
                    break
                following = row_end + 1
            if following > end + 1:
                result.extend(tokens[copied : end + 1])
                result.extend([",", "..."])
                copied = following
    if copied == 0:
        return tokens
    result.extend(tokens[copied:])
    return result


def _summary(tokens: list[str], operation: str) -> str:
    marker = {
        "SELECT": "FROM",
        "DELETE": "FROM",
        "INSERT": "INTO",
        "UPDATE": "UPDATE",
    }.get(operation)
    if marker is None:
        return operation
    depth = 0
    start = len(tokens)
    for index, token in enumerate(tokens):
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0 and token.upper() == marker:
            start = index + 1
            break
    parts = []
    while start < len(tokens):
        for name in tokens[start].split("."):
            quoted = name.endswith('"') and (
                name.startswith('"') or name[:3].upper() == 'U&"'
            )
            if quoted:
                start_quote = 3 if name[:3].upper() == 'U&"' else 1
                name = name[start_quote:-1].replace('""', '"')
            if not _SUMMARY_IDENTIFIER.fullmatch(name) or (
                not quoted and name.upper() in {"SELECT", "ONLY", "LATERAL", "VALUES"}
            ):
                return operation
            parts.append(name)
        start += 1
        if start >= len(tokens) or tokens[start] != ".":
            break
        start += 1
        if len(parts) == 2 or start >= len(tokens):
            return operation
    target = ".".join(parts)
    return f"{operation} {target}" if is_query_identifier(target) else operation


def _identifier_token(value: str) -> bool:
    return value[0].isalpha() or value[0] in '_"'


def _render(tokens: list[str], maximum: int) -> str:
    output: list[str] = []
    previous = ""
    for text in tokens:
        space = (
            bool(output)
            and text not in {",", ")", "]", ";", "::"}
            and previous not in {"(", "[", "::"}
        )
        if (text == "." and _identifier_token(previous)) or (
            previous == "." and _identifier_token(text)
        ):
            space = False
        if (
            text == "("
            and previous
            and previous.upper() not in _PAREN_KEYWORDS
            and _identifier_token(previous)
        ):
            space = False
        output.append((" " if space else "") + text)
        previous = text
    joined = "".join(output)
    if len(joined.encode("utf-8")) <= maximum:
        return joined
    size = 0
    for index, part in enumerate(output):
        size += len(part.encode("utf-8"))
        if size > maximum:
            return "".join(output[:index])
    return joined


def _describe(query: str, maximum: int) -> SanitizedQuery | None:
    tokens = _tokens(query)
    if not tokens:
        return None
    operation = tokens[0].upper()
    if operation not in DB_OPERATIONS:
        return None
    text = _render(_collapse(tokens), maximum)
    return (
        SanitizedQuery(text, _summary(tokens, operation), operation) if text else None
    )


@lru_cache(maxsize=1024)
def _sanitize(query: str, maximum: int) -> SanitizedQuery | None:
    return _describe(query, maximum)
