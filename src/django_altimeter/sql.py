"""Bounded PostgreSQL descriptions, independent of driver and privacy policy.

This lexer removes values; it neither validates nor executes SQL. Identifiers are
visible and must never contain application data. Unsupported or ambiguous input
has no description. Process-local LRUs retain up to 1,024 small raw queries
(4,096 characters each) and 32 large raw queries (16,384 characters each): at
most 16 MiB and 2 MiB of UTF-8 keys. Cached result text is at most 2,048 bytes
per entry. A separate 512-entry verification cache holds candidate texts of at most
2,048 UTF-8 bytes each (1 MiB of keys); it cannot evict raw queries. These are
payload bounds, excluding Python object overhead. Nothing is persisted/adapted.
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
_MAX_SMALL_QUERY_CHARS = 4096
_MAX_CACHED_TEXT_BYTES = 2048
_SUMMARY_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
_PARAMETER = r"%(?:[sbt%]|\([A-Za-z_][A-Za-z0-9_]*\)[sbt])|\$[0-9]+(?![\w$])"
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
    if maximum > _MAX_CACHED_TEXT_BYTES:
        return _describe(query, maximum)
    if len(query) <= _MAX_SMALL_QUERY_CHARS:
        return _sanitize(query, maximum)
    return _sanitize_large(query, maximum)


def sanitized_query_text(
    query: object, *, max_text_bytes: int
) -> SanitizedQuery | None:
    """Verify an already-sanitized description without using the raw-query caches."""
    if (
        type(query) is not str
        or not query
        or len(query) > MAX_QUERY_CHARS
        or type(max_text_bytes) is not int
        or max_text_bytes <= 0
        or len(query) > max_text_bytes
    ):
        return None
    try:
        size = len(query.encode("utf-8"))
    except UnicodeEncodeError:
        return None
    if size > max_text_bytes:
        return None
    maximum = min(max_text_bytes, MAX_QUERY_CHARS * 4)
    if size <= _MAX_CACHED_TEXT_BYTES:
        return _verify(query, maximum)
    return _fixed_point(query, maximum)


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
        if kind == "parameter" and text[0] == "$":
            # Check length before conversion: never parse an unbounded integer.
            if len(text) > 6 or text[1] == "0" or int(text[1:]) > 65535:
                return None
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


def _top_level_keyword(tokens: list[str], keyword: str) -> int | None:
    depth = 0
    for index, token in enumerate(tokens):
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0 and token.upper() == keyword:
            return index
    return None


def _summary(tokens: list[str], operation: str) -> str:
    marker = {
        "SELECT": "FROM",
        "DELETE": "FROM",
        "INSERT": "INTO",
        "UPDATE": "UPDATE",
    }.get(operation)
    if marker is None:
        return operation
    position = _top_level_keyword(tokens, marker)
    if position is None:
        return operation
    start = position + 1
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


def _elide_select_list(tokens: list[str]) -> list[str]:
    if tokens[0].upper() != "SELECT":
        return tokens
    position = _top_level_keyword(tokens, "FROM")
    start = 2 if len(tokens) > 1 and tokens[1].upper() == "DISTINCT" else 1
    if position is None or position <= start or tokens[start:position] == ["..."]:
        return tokens
    return tokens[:start] + ["..."] + tokens[position:]


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
    # Keep the target near the start of wide-model queries before applying the
    # byte cutoff. An already-elided list is unchanged, so this recurses once.
    elided = _elide_select_list(tokens)
    if elided is not tokens:
        return _render(elided, maximum)
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


@lru_cache(maxsize=32)
def _sanitize_large(query: str, maximum: int) -> SanitizedQuery | None:
    return _describe(query, maximum)


def _fixed_point(query: str, maximum: int) -> SanitizedQuery | None:
    result = _describe(query, maximum)
    if result is None or result.text != query:
        return None
    # Reuse the cache key's text instead of retaining an identical string copy.
    return SanitizedQuery(query, result.summary, result.operation)


@lru_cache(maxsize=512)
def _verify(query: str, maximum: int) -> SanitizedQuery | None:
    return _fixed_point(query, maximum)
