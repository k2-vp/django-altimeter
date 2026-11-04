"""Literal removal, bounded work and fixed points for PostgreSQL query text."""

import random
import string
from dataclasses import FrozenInstanceError

import pytest

from django_altimeter.sql import (
    MAX_QUERY_CHARS,
    SanitizedQuery,
    is_query_identifier,
    sanitize_query,
)


def sanitize(query, maximum=2048):
    result = sanitize_query(query, max_text_bytes=maximum)
    assert result is not None
    assert len(result.text.encode("utf-8")) <= maximum
    again = sanitize_query(result.text, max_text_bytes=maximum)
    assert again is not None and again.text == result.text
    return result


@pytest.mark.parametrize(
    "literal",
    [
        "'SQL_CANARY'",
        "'SQL_CANARY''quoted'",
        "E'SQL_CANARY\\'escaped'",
        "e'SQL_CANARY\\\\'",
        "N'SQL_CANARY'",
        "U&'SQL_CANARY\\0061'",
        "B'100101'",
        "X'AbCD'",
        "$$SQL_CANARY$$",
        "$tag$SQL_CANARY$tag$",
        "$tag$SQL_CANARY $other$ nested $other$ $tag$",
        "9284712635",
        "9284.712635",
        ".9284712635",
        "9284712635.",
        "9.284712635e+12",
        "9284712635E-2",
        "0xABCDEF",
        "0o1234567",
        "0b1011001",
        "9_284_712_635",
        "0xAB_CD",
        "TRUE",
        "false",
    ],
)
def test_every_literal_is_a_placeholder_with_identifiers_preserved(literal):
    result = sanitize(f'SELECT "value", {literal} FROM public.cart_item')
    assert result == SanitizedQuery(
        'SELECT "value", ? FROM public.cart_item', "SELECT public.cart_item", "SELECT"
    )


def test_comments_are_removed_without_joining_tokens():
    result = sanitize(
        "/* SQL_CANARY */ SELECT/* hidden */value, 1+-- SQL_CANARY\n"
        "2 FROM cart_item -- SQL_CANARY"
    )
    assert result.text == "SELECT value, ? + ? FROM cart_item"
    assert result.summary == "SELECT cart_item"


@pytest.mark.parametrize("placeholder", ["%s", "%b", "%t", "%(name)s", "$1", "%%"])
def test_bind_placeholders_survive_without_reading_parameters(placeholder):
    assert sanitize(f"SELECT {placeholder} FROM cart_item").text == (
        f"SELECT {placeholder} FROM cart_item"
    )


@pytest.mark.parametrize("count", [2, 3, 10, 100])
def test_placeholder_lists_and_repeated_values_rows_have_stable_size(count):
    items = ", ".join("%s" for _ in range(count))
    assert sanitize(f"SELECT id FROM cart_item WHERE id IN ({items})").text == (
        "SELECT id FROM cart_item WHERE id IN (%s, ...)"
    )
    rows = ", ".join("(%s, %s)" for _ in range(count))
    assert sanitize(f"INSERT INTO cart_item VALUES {rows} RETURNING id").text == (
        "INSERT INTO cart_item VALUES (%s, %s), ... RETURNING id"
    )


def test_expression_lists_and_values_columns_are_not_collapsed():
    assert sanitize(
        "SELECT COALESCE(%s, %s) FROM cart_item WHERE id IN (%s, id)"
    ).text == ("SELECT COALESCE(%s, %s) FROM cart_item WHERE id IN (%s, id)")
    assert sanitize("INSERT INTO cart_item VALUES (%s, %s)").text == (
        "INSERT INTO cart_item VALUES (%s, %s)"
    )


@pytest.mark.parametrize(
    "query,summary",
    [
        ('select "id" from "public"."cart_item"', "SELECT public.cart_item"),
        ("INSERT INTO cart_item VALUES (1)", "INSERT cart_item"),
        ('UPDATE "cart_item" SET id=1', "UPDATE cart_item"),
        ("DELETE FROM cart_item WHERE id=1", "DELETE cart_item"),
        ("SELECT 1", "SELECT"),
        ('SELECT 1 FROM "private table"', "SELECT"),
        ('SELECT 1 FROM "a""b"', "SELECT"),
        ('SELECT 1 FROM "a"""', "SELECT"),
        ('SELECT 1 FROM "SELECT"', "SELECT SELECT"),
        ('SELECT 1 FROM "public.cart_item"', "SELECT"),
        ("SELECT 1 FROM " + "a" * 129, "SELECT"),
        ("SELECT 1 FROM (SELECT 1) AS subquery", "SELECT"),
        ("WITH items AS (SELECT 1 FROM cart_item) SELECT * FROM items", "WITH"),
        ("SELECT 'FROM hidden' FROM cart_item", "SELECT cart_item"),
    ],
)
def test_summary_uses_only_a_bounded_identifier(query, summary):
    assert sanitize(query).summary == summary


@pytest.mark.parametrize(
    "query",
    [
        None,
        b"SELECT 1",
        123,
        [],
        "",
        "   ",
        "-- comment",
        "/* comment */",
        "SELECT 'unterminated",
        'SELECT "unterminated',
        "SELECT $tag$unterminated",
        "SELECT 1 /* unterminated",
        "SELECT 1 /* outer /* nested */ */",
        "SELECT 1\x00",
        "SELECT 1\x01",
        "SELECT \\bad",
        "SELECT $invalid",
        "SELECT 1e+",
        "SELECT 0xbroken",
        "SELECT 1__2",
        "SELECT 123abc",
        "SELECT 1 */",
        "SELECT \ud800",
        "SELECT 'ambiguous\\' SQL_CANARY'",
        "SELECT " + "a" * MAX_QUERY_CHARS,
    ],
)
def test_unsupported_or_ambiguous_input_fails_closed(query):
    assert sanitize_query(query, max_text_bytes=2048) is None
    assert sanitize("SELECT 7 FROM cart_item").text == "SELECT ? FROM cart_item"


def test_unknown_objects_are_never_adapted_or_scanned():
    class Hostile:
        def __str__(self):
            pytest.fail("must not adapt a query")

        def __len__(self):
            pytest.fail("must not inspect a query object")

    class HostileString(str):
        def __len__(self):
            pytest.fail("must not call overridden string methods")

    for query in (Hostile(), HostileString("SELECT 1")):
        assert sanitize_query(query, max_text_bytes=2048) is None


def test_unicode_escaped_identifiers_stay_identifiers_without_decoding_values():
    result = sanitize(
        'SELECT U&"na\\006de" FROM U&"cart_item" WHERE value = \'secret\''
    )
    assert result.text == 'SELECT U&"na\\006de" FROM U&"cart_item" WHERE value = ?'
    assert result.summary == "SELECT cart_item"


@pytest.mark.parametrize(
    "function", ["EXTRACT(YEAR", "SUBSTRING(value", "TRIM(value", "OVERLAY(value"]
)
def test_summary_ignores_from_inside_expressions(function):
    result = sanitize(
        f'SELECT {function} FROM "shop_order"."created") AS y FROM "shop_order"'
    )
    assert result.summary == "SELECT shop_order"


@pytest.mark.parametrize(
    "keyword",
    [
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
    ],
)
def test_parenthesized_keyword_expressions_remain_readable(keyword):
    assert sanitize(f"SELECT x {keyword}(value = 1)").text == (
        f"SELECT x {keyword} (value = ?)"
    )
    assert sanitize("SELECT count(*) FROM cart_item").text == (
        "SELECT count(*) FROM cart_item"
    )


def test_null_remains_a_keyword_and_boolean_values_are_removed():
    assert (
        sanitize(
            "SELECT NULL, TRUE FROM cart_item "
            "WHERE value IS NOT NULL AND enabled = FALSE"
        ).text
        == "SELECT NULL, ? FROM cart_item WHERE value IS NOT NULL AND enabled = ?"
    )


def test_parameters_adjacent_to_operators_remain_parameters():
    assert sanitize("SELECT 1+%s, 2*%(value)s").text == "SELECT ? + %s, ? * %(value)s"


def test_cache_admits_many_small_queries_but_does_not_retain_large_queries():
    from django_altimeter.sql import _sanitize

    _sanitize.cache_clear()
    for index in range(1024):
        sanitize(f"SELECT {index}")
    assert _sanitize.cache_info().currsize == 1024
    before = _sanitize.cache_info()
    for index in range(10):
        result = sanitize_query(
            "SELECT 1 /* " + "a" * 4096 + str(index) + " */", max_text_bytes=2048
        )
        assert result is not None and result.text == "SELECT ?"
    assert _sanitize.cache_info() == before


def test_all_truncation_points_remain_fixed_points_at_token_boundaries():
    query = (
        'SELECT "café", %s::text, sum(amount) FROM cart_item '
        "WHERE id IN (1, 2, 3) AND value = 'SQL_CANARY'"
    )
    full = sanitize(query).text
    for maximum in range(1, len(full.encode("utf-8")) + 1):
        result = sanitize_query(query, max_text_bytes=maximum)
        if result is not None:
            assert full.startswith(result.text)
            assert sanitize(result.text, maximum).text == result.text
    assert sanitize_query("SELECT 1", max_text_bytes=0) is None


def test_random_literals_never_escape_and_outputs_are_fixed_points():
    randomizer = random.Random(413)
    for _ in range(500):
        canary = "SQL_CANARY_" + "".join(
            randomizer.choices(string.ascii_letters + string.digits, k=24)
        )
        wrapper = randomizer.choice(
            ["'{}'", "E'{}'", "N'{}'", "U&'{}'", "$tag${}$tag$", "$$ {} $$"]
        )
        items = ", ".join(str(randomizer.randrange(1000)) for _ in range(5))
        query = (
            f"SELECT {wrapper.format(canary)}, %s FROM cart_item "
            f"WHERE id IN ({items}) "
            f"/* {canary} */ -- {canary}"
        )
        result = sanitize(query, randomizer.randrange(8, 160))
        assert canary not in result.text
        assert "SQL_CANARY" not in result.text


def test_random_token_sequences_and_truncation_keep_lexical_boundaries():
    randomizer = random.Random(617)
    tokens = [
        "x",
        "SELECT",
        "IN",
        "VALUES",
        "(",
        ")",
        ",",
        ".",
        "::",
        "?",
        "%s",
        "%b",
        "$1",
        "...",
        "+",
        "-",
        "/",
        "*",
        "->>",
        "??",
        "1",
        "2.3",
        "'SQL_CANARY'",
        '"café"',
    ]
    for _ in range(2000):
        query = "SELECT " + " ".join(
            randomizer.choices(tokens, k=randomizer.randrange(1, 35))
        )
        maximum = randomizer.randrange(6, 200)
        result = sanitize_query(query, max_text_bytes=maximum)
        if result is not None:
            assert sanitize(result.text, maximum).text == result.text
            assert "SQL_CANARY" not in result.text


@pytest.mark.parametrize(
    "value,valid",
    [
        ("cart_item", True),
        ("public.cart_item", True),
        ("a" * 128, True),
        ("a$1", True),
        ("a" * 129, False),
        ("public..item", False),
        ("a.b.c", False),
        ('"item"', False),
        ("item value", False),
        ("", False),
        ("1item", False),
        ("café", False),
        (None, False),
    ],
)
def test_summary_identifier_boundary(value, valid):
    assert is_query_identifier(value) is valid


def test_results_are_immutable():
    result = sanitize("SELECT 1")
    with pytest.raises(FrozenInstanceError):
        result.text = "unsafe"
