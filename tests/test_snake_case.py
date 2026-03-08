"""Tests for CamelCase to snake_case conversion."""

from temporaryname.actor import snake_case


def test_single_word():
    assert snake_case("Request") == "request"


def test_two_words():
    assert snake_case("MathRequest") == "math_request"


def test_three_words():
    assert snake_case("DoSomethingNow") == "do_something_now"


def test_already_lowercase():
    assert snake_case("request") == "request"


def test_acronym_prefix():
    assert snake_case("HTTPRequest") == "http_request"


def test_acronym_suffix():
    assert snake_case("GetHTTP") == "get_http"


def test_acronym_only():
    assert snake_case("HTTP") == "http"


def test_acronym_middle():
    assert snake_case("GetHTTPResponse") == "get_http_response"


def test_two_acronyms():
    assert snake_case("XMLToJSON") == "xml_to_json"


def test_single_char():
    assert snake_case("X") == "x"


def test_two_chars():
    assert snake_case("IO") == "io"


def test_number_in_name():
    assert snake_case("Base64Encode") == "base64_encode"


def test_trailing_number():
    assert snake_case("GetV2") == "get_v2"
