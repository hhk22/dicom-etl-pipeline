import json

import pytest

from commerce_etl.files import run_directory
from commerce_etl.pipeline import discover


def test_discovery_freezes_and_archives_input(settings, generate):
    generate(settings.root, "demo", 2, 3)
    batches = discover(settings, "run-one", "demo")
    assert len(batches) == 4
    assert all(b["raw_path"].startswith("raw/") for b in batches)
    manifest = run_directory(settings.root, "run-one") / "manifest.json"
    assert len(manifest.read_bytes()) < 10000
    assert "customer_id" not in manifest.read_text()
    (settings.root / "input/demo/shop_a/part-000.csv").write_text("changed")
    assert discover(settings, "run-one", "demo") == batches
    assert len(discover(settings, "run-two", "demo")) == 4


def test_empty_input_is_explicit_failure(settings):
    with pytest.raises(ValueError, match="no_input_files"):
        discover(settings, "empty", "empty")


def test_identical_files_get_one_batch(settings, generate):
    generate(settings.root, "demo", 1, 2)
    original = settings.root / "input/demo/shop_a/part-000.csv"
    original.with_name("copy.csv").write_bytes(original.read_bytes())
    assert len(discover(settings, "run", "demo")) == 2


def test_generator_preserves_existing_input(settings, generate):
    generate(settings.root, "demo", 1, 2)
    with pytest.raises(FileExistsError):
        generate(settings.root, "demo", 1, 2)


def test_reject_path_escape(settings):
    with pytest.raises(ValueError, match="invalid_dataset"):
        discover(settings, "run", "../escape")


def test_manifest_does_not_leak_key(settings, generate):
    generate(settings.root, "demo", 1, 1)
    discover(settings, "run", "demo")
    content = (run_directory(settings.root, "run") / "manifest.json").read_text()
    assert settings.token_key not in content
    assert json.loads(content)["transform_version"] == "v1"
