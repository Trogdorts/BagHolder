import io
import zipfile

import pytest

from app.services.data_backup import create_backup_archive, restore_backup_archive


def test_create_backup_archive_captures_nested_files(tmp_path):
    data_dir = tmp_path / "data"
    nested = data_dir / "nested"
    nested.mkdir(parents=True)

    (data_dir / "config.yaml").write_text("config", encoding="utf-8")
    (nested / "notes.txt").write_text("note", encoding="utf-8")

    payload = create_backup_archive(str(data_dir))

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = sorted(archive.namelist())

    assert names == ["config.yaml", "nested/notes.txt"]


def test_restore_backup_archive_replaces_directory_contents(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "old.txt").write_text("old", encoding="utf-8")

    backup_source = tmp_path / "backup_src"
    nested = backup_source / "nested"
    nested.mkdir(parents=True)
    (backup_source / "config.yaml").write_text("new", encoding="utf-8")
    (nested / "notes.txt").write_text("note", encoding="utf-8")

    payload = create_backup_archive(str(backup_source))

    restore_backup_archive(str(data_dir), payload)

    assert not (data_dir / "old.txt").exists()
    assert (data_dir / "config.yaml").read_text(encoding="utf-8") == "new"
    assert (data_dir / "nested/notes.txt").read_text(encoding="utf-8") == "note"


def test_restore_backup_archive_rejects_path_traversal(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("../evil.txt", "bad")

    payload = buffer.getvalue()

    with pytest.raises(ValueError):
        restore_backup_archive(str(data_dir), payload)
