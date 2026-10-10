import json

import pytest

from commerce_etl.transform import InvalidFile, InvalidOrder, normalize, parse_rows

KEY = "test-key-" * 8
ROW = {
    "order_id": "A1",
    "customer_id": "C1",
    "amount": "29000",
    "ordered_at": "2026-01-01 10:00:00",
    "name": "Private Name",
    "email": "raw@example.com",
}


def test_normalization_drops_pii_and_converts_timezone():
    result = normalize("shop_a", ROW, KEY)
    assert result["amount_krw"] == 29000
    assert result["ordered_at"] == "2026-01-01T01:00:00Z"
    assert len(result["customer_token"]) == 64
    assert set(result) == {"source", "order_id", "customer_token", "amount_krw", "ordered_at"}
    assert "raw@example.com" not in json.dumps(result)
    assert result == normalize("shop_a", ROW, KEY)


def test_source_and_secret_separate_customer_tokens():
    b = {
        "orderNo": "A1",
        "buyerId": "C1",
        "totalPrice": 29000,
        "purchasedAt": "2026-01-01T10:00:00+09:00",
    }
    a = normalize("shop_a", ROW, KEY)
    assert a["customer_token"] != normalize("shop_b", b, KEY)["customer_token"]
    assert a["customer_token"] != normalize("shop_a", ROW, KEY + "other")["customer_token"]
    assert a["ordered_at"] == normalize("shop_b", b, KEY)["ordered_at"]


@pytest.mark.parametrize("value", [-1, "-1", "1.5", 1.5, True, None, "", "1e3", "9" * 19])
def test_invalid_amounts(value):
    with pytest.raises(InvalidOrder, match="invalid_amount"):
        normalize("shop_a", dict(ROW, amount=value), KEY)


@pytest.mark.parametrize("value", [None, "", "2026-02-30 10:00:00", "not-a-date"])
def test_invalid_times(value):
    with pytest.raises(InvalidOrder):
        normalize("shop_a", dict(ROW, ordered_at=value), KEY)


def test_b_requires_timezone():
    with pytest.raises(InvalidOrder, match="invalid_timestamp"):
        normalize(
            "shop_b",
            {"orderNo": "1", "buyerId": "1", "totalPrice": 1, "purchasedAt": "2026-01-01T10:00:00"},
            KEY,
        )


@pytest.mark.parametrize(
    "source,data",
    [
        ("shop_b", b'{"secret":"value"}'),
        ("shop_b", b"bad-json"),
        ("shop_a", b"wrong,header\n1,2"),
        ("shop_a", b"\xff"),
    ],
)
def test_bad_file_errors_do_not_echo_input(source, data):
    with pytest.raises(InvalidFile) as error:
        parse_rows(source, data)
    assert "secret" not in str(error.value)


def test_missing_id_is_not_generated_silently():
    with pytest.raises(InvalidOrder, match="invalid_order_id"):
        normalize("shop_a", dict(ROW, order_id=""), KEY)
