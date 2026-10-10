"""Two source contracts -> one allowlisted analysis record. No original PII in errors."""

import csv
import hashlib
import hmac
import io
import json
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from commerce_etl.files import canonical


class InvalidOrder(ValueError):
    """Message is a fixed reason code, never the offending input value."""


class InvalidFile(ValueError):
    pass


def parse_rows(source: str, data: bytes) -> list[dict]:
    try:
        text = data.decode("utf-8-sig")
        if source == "shop_a":
            reader = csv.DictReader(io.StringIO(text), strict=True)
            required = {"order_id", "customer_id", "amount", "ordered_at"}
            if not required <= set(reader.fieldnames or []):
                raise InvalidFile("csv_header")
            return list(reader)
        if source == "shop_b":
            rows = json.loads(text)
            if not isinstance(rows, list):
                raise InvalidFile("json_array_required")
            return rows
        raise InvalidFile("unknown_source")
    except (UnicodeError, csv.Error, json.JSONDecodeError):
        raise InvalidFile("unreadable_file") from None


def required_text(row, name):
    value = row.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise InvalidOrder("invalid_" + name)
    return value.strip()


def normalize(source: str, row: dict, token_key: str) -> dict:
    if not isinstance(row, dict):
        raise InvalidOrder("object_required")
    if source == "shop_a":
        oid, cid, amount, moment = "order_id", "customer_id", "amount", "ordered_at"
    elif source == "shop_b":
        oid, cid, amount, moment = "orderNo", "buyerId", "totalPrice", "purchasedAt"
    else:
        raise InvalidOrder("unknown_source")
    order_id = required_text(row, oid)
    customer = required_text(row, cid)
    value = row.get(amount)
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise InvalidOrder("invalid_amount")
    if not re.fullmatch(r"[0-9]{1,18}", str(value)):
        raise InvalidOrder("invalid_amount")
    try:
        raw_time = required_text(row, moment)
        if source == "shop_a":
            timestamp = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=ZoneInfo("Asia/Seoul")
            )
        else:
            timestamp = datetime.fromisoformat(raw_time)
            if timestamp.tzinfo is None:
                raise ValueError
    except (ValueError, TypeError):
        raise InvalidOrder("invalid_timestamp") from None
    token = hmac.new(token_key.encode(), canonical([source, customer]).encode(), hashlib.sha256)
    return {
        "source": source,
        "order_id": order_id,
        "customer_token": token.hexdigest(),
        "amount_krw": int(value),
        "ordered_at": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
