"""Bounded PostgreSQL query descriptions, independent of driver and privacy policy.

This lexer removes values; it neither validates nor executes SQL. Identifiers are
visible and must never contain application data. Unsupported or ambiguous input
has no description. At most 128 raw queries of 16,384 characters are retained in
the process-local LRU cache; nothing is persisted. Never pass adapted parameters.
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
_IDENTIFIER = re.compile(r"[^\W\d][\w$]*", re.UNICODE)
_SUMMARY_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
_PARAMETER = re.compile(r"%(?:[sbt%]|\([A-Za-z_][A-Za-z0-9_]*\)[sbt])|\$[0-9]+")
_DOLLAR_QUOTE = re.compile(r"\$(?:[^\W\d]\w*)?\$", re.UNICODE)
_STRING_PREFIX = re.compile(r"(?i:E|N|B|X|U&)'")
_NEWLINE = re.compile(r"[\r\n]")
_NUMBER = re.compile(
    r"(?:0[xX][0-9a-fA-F](?:_?[0-9a-fA-F])*"
    r"|0[oO][0-7](?:_?[0-7])*|0[bB][01](?:_?[01])*"
    r"|(?:[0-9](?:_?[0-9])*(?:\.(?:[0-9](?:_?[0-9])*)?)?"
    r"|\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?)"
)
_OPERATORS = frozenset("+-*/<>=~!@#%^&|`?")
_PAREN_KEYWORDS = frozenset({"IN", "VALUES", "AS", "OVER", "FILTER", "EXISTS"})


@dataclass(frozen=True)
class _Token:
    text: str
    kind: str


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
    return _sanitize(query, min(max_text_bytes, MAX_QUERY_CHARS * 4))


def _quoted_end(query: str, start: int, *, escapes: bool = False) -> int | None:
    quote = query[start]
    position = start + 1
    while position < len(query):
        char = query[position]
        if char == quote:
            if position + 1 < len(query) and query[position + 1] == quote:
                position += 2
                continue
            return position + 1
        if char == "\\" and quote == "'":
            if escapes:
                position += 2
                continue
            # Without connection settings, ordinary backslash strings have two
            # possible quote boundaries. Decline rather than guess which applies.
            return None
        position += 1
    return None


def _tokens(query: str) -> list[_Token] | None:
    if "\0" in query:
        return None
    try:
        query.encode("utf-8")
    except UnicodeEncodeError:
        return None
    tokens = []
    position = 0
    while position < len(query):
        char = query[position]
        if char in " \t\n\r\f":
            position += 1
            continue
        if query.startswith("--", position):
            newline = _NEWLINE.search(query, position + 2)
            position = newline.end() if newline else len(query)
            continue
        if query.startswith("/*", position):
            end = query.find("*/", position + 2)
            if end == -1 or query.find("/*", position + 2, end) != -1:
                return None
            position = end + 2
            continue
        if query.startswith("*/", position):
            return None
        prefix = _STRING_PREFIX.match(query, position)
        if char == "'" or prefix:
            start = position + len(prefix[0]) - 1 if prefix else position
            # U& strings use ordinary quote doubling, with Unicode escapes that
            # cannot escape a quote. Treat their backslashes conservatively as E
            # escapes: this can omit text, but cannot expose their contents.
            escapes = bool(prefix and prefix[0][0].upper() in {"E", "U"})
            quote_end = _quoted_end(query, start, escapes=escapes)
            if quote_end is None:
                return None
            tokens.append(_Token("?", "parameter"))
            position = quote_end
            continue
        # Unicode-escaped identifiers require decoding UESCAPE clauses; no guess.
        if query[position : position + 3].upper() == 'U&"':
            return None
        if char == '"':
            quote_end = _quoted_end(query, position)
            if quote_end is None:
                return None
            tokens.append(_Token(query[position:quote_end], "identifier"))
            position = quote_end
            continue
        dollar = _DOLLAR_QUOTE.match(query, position) if char == "$" else None
        if dollar:
            end = query.find(dollar[0], dollar.end())
            if end == -1:
                return None
            tokens.append(_Token("?", "parameter"))
            position = end + len(dollar[0])
            continue
        parameter = _PARAMETER.match(query, position)
        if parameter:
            tokens.append(_Token(parameter[0], "parameter"))
            position = parameter.end()
            continue
        number = _NUMBER.match(query, position)
        if number:
            position = number.end()
            if position < len(query) and (
                query[position].isalnum() or query[position] in "_$"
            ):
                return None
            tokens.append(_Token("?", "parameter"))
            continue
        identifier = _IDENTIFIER.match(query, position)
        if identifier:
            text = identifier[0]
            tokens.append(
                _Token("?", "parameter")
                if text.upper() in {"TRUE", "FALSE", "NULL"}
                else _Token(text, "word")
            )
            position = identifier.end()
            continue
        special = next(
            (
                value
                for value in ("...", "::", ":=")
                if query.startswith(value, position)
            ),
            None,
        )
        if special:
            tokens.append(_Token(special, "symbol"))
            position += len(special)
            continue
        if char in "(),.;[]":
            tokens.append(_Token(char, "symbol"))
            position += 1
            continue
        if char in _OPERATORS:
            end = position + 1
            while end < len(query) and query[end] in _OPERATORS:
                if query.startswith(("--", "/*", "*/"), end):
                    break
                end += 1
            text = query[position:end]
            tokens.append(_Token(text, "parameter" if text == "?" else "symbol"))
            position = end
            continue
        return None
    return tokens


def _placeholder_row(tokens: list[_Token], start: int) -> int | None:
    """Return the closing parenthesis of a row made only of bind/value markers."""
    if start >= len(tokens) or tokens[start].text != "(":
        return None
    position = start + 1
    while position < len(tokens) and tokens[position].kind == "parameter":
        position += 1
        if position < len(tokens) and tokens[position].text == ")":
            return position
        if position >= len(tokens) or tokens[position].text != ",":
            return None
        position += 1
    return None


def _collapse(tokens: list[_Token]) -> list[_Token]:
    result = []
    position = 0
    while position < len(tokens):
        token = tokens[position]
        result.append(token)
        position += 1
        if token.kind != "word" or token.text.upper() not in {"IN", "VALUES"}:
            continue
        end = _placeholder_row(tokens, position)
        if end is None:
            continue
        row = tokens[position : end + 1]
        if token.text.upper() == "IN" and len(row) > 3:
            result.extend(
                [
                    row[0],
                    row[1],
                    _Token(",", "symbol"),
                    _Token("...", "symbol"),
                    row[-1],
                ]
            )
            position = end + 1
        elif token.text.upper() == "VALUES":
            result.extend(row)
            position = end + 1
            repeated = False
            while position < len(tokens) and tokens[position].text == ",":
                following = _placeholder_row(tokens, position + 1)
                if following is None or tokens[position + 1 : following + 1] != row:
                    break
                repeated = True
                position = following + 1
            if repeated:
                result.extend([_Token(",", "symbol"), _Token("...", "symbol")])
    return result


def _summary(tokens: list[_Token], operation: str) -> str:
    marker = {
        "SELECT": "FROM",
        "DELETE": "FROM",
        "INSERT": "INTO",
        "UPDATE": "UPDATE",
    }.get(operation)
    if marker is None:
        return operation
    start = next(
        (
            index + 1
            for index, token in enumerate(tokens)
            if token.kind == "word" and token.text.upper() == marker
        ),
        len(tokens),
    )
    parts = []
    while start < len(tokens):
        token = tokens[start]
        if token.kind not in {"word", "identifier"}:
            return operation
        name = (
            token.text[1:-1].replace('""', '"')
            if token.kind == "identifier"
            else token.text
        )
        if not _SUMMARY_IDENTIFIER.fullmatch(name) or name.upper() in {
            "SELECT",
            "ONLY",
            "LATERAL",
            "VALUES",
        }:
            return operation
        parts.append(name)
        start += 1
        if start >= len(tokens) or tokens[start].text != ".":
            break
        start += 1
        if len(parts) == 2 or start >= len(tokens):
            return operation
    target = ".".join(parts)
    return f"{operation} {target}" if is_query_identifier(target) else operation


def _render(tokens: list[_Token], maximum: int) -> str:
    output: list[str] = []
    size = 0
    previous = ""
    previous_kind = ""
    for token in tokens:
        text = token.text
        space = (
            bool(output)
            and text not in {",", ")", "]", ";", "::"}
            and previous not in {"(", "[", "::"}
        )
        if (text == "." and previous_kind in {"word", "identifier"}) or (
            previous == "." and token.kind in {"word", "identifier"}
        ):
            space = False
        if text == "(" and previous.upper() not in _PAREN_KEYWORDS:
            space = False
        addition = (" " if space else "") + text
        size += len(addition.encode("utf-8"))
        if size > maximum:
            break
        output.append(addition)
        previous = text
        previous_kind = token.kind
    return "".join(output)


@lru_cache(maxsize=128)
def _sanitize(query: str, maximum: int) -> SanitizedQuery | None:
    tokens = _tokens(query)
    if not tokens or tokens[0].kind != "word":
        return None
    operation = tokens[0].text.upper()
    if operation not in DB_OPERATIONS:
        return None
    text = _render(_collapse(tokens), maximum)
    return (
        SanitizedQuery(text, _summary(tokens, operation), operation) if text else None
    )
