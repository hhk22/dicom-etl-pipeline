"""Generate deterministic synthetic orders; never overwrite an existing dataset."""

import argparse
import csv
import io
import json
import os
from pathlib import Path

from commerce_etl.files import atomic_write


def generate(root: Path, dataset: str, files_per_shop: int, rows_per_file: int):
    if not dataset or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for c in dataset):
        raise ValueError("dataset must use lowercase letters, digits, _ or -")
    if not 1 <= files_per_shop <= 100 or not 1 <= rows_per_file <= 100000:
        raise ValueError("files/shop: 1..100; rows/file: 1..100000")
    target = root / "input" / dataset
    target.mkdir(parents=True, exist_ok=False)
    for source in ("shop_a", "shop_b"):
        for part in range(files_per_shop):
            rows = []
            for i in range(rows_per_file):
                n = part * rows_per_file + i
                oid, cid = f"{dataset}-{n:08d}", f"C-{n % 1000:04d}"
                if source == "shop_a":
                    rows.append(
                        dict(
                            order_id=oid,
                            customer_id=cid,
                            amount=1000 + n % 90000,
                            ordered_at="2026-01-01 10:00:00",
                            name="Sample User",
                            email=f"sample{n}@example.com",
                            address="Synthetic address",
                        )
                    )
                else:
                    rows.append(
                        dict(
                            orderNo=oid,
                            buyerId=cid,
                            totalPrice=1000 + n % 90000,
                            purchasedAt="2026-01-01T10:00:00+09:00",
                            email=f"sample{n}@example.com",
                        )
                    )
            if source == "shop_a":
                stream = io.StringIO()
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
                data, suffix = stream.getvalue().encode(), "csv"
            else:
                data, suffix = json.dumps(rows).encode(), "json"
            atomic_write(target / source / f"part-{part:03d}.{suffix}", data)
    return {
        "dataset": dataset,
        "files": files_per_shop * 2,
        "orders": files_per_shop * rows_per_file * 2,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset", default="demo")
    p.add_argument("--files-per-shop", type=int, default=4)
    p.add_argument("--rows-per-file", type=int, default=1000)
    p.add_argument(
        "--root", type=Path, default=Path(os.getenv("ETL_DATA_ROOT", "/opt/airflow/data"))
    )
    args = p.parse_args()
    print(json.dumps(generate(args.root, args.dataset, args.files_per_shop, args.rows_per_file)))
