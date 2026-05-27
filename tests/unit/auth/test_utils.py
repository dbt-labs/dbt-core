import stat

from dbt.auth.utils import secure_open


class TestSecureOpen:
    def test_creates_file_with_600_permissions(self, tmp_path):
        target = tmp_path / "token.json"
        with secure_open(target) as f:
            f.write('{"key": "value"}')

        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode == 0o600

    def test_creates_parent_directories(self, tmp_path):
        target = tmp_path / "a" / "b" / "token.json"
        with secure_open(target) as f:
            f.write("hello")

        assert target.read_text() == "hello"

    def test_truncates_existing_file(self, tmp_path):
        target = tmp_path / "token.json"
        with secure_open(target) as f:
            f.write("first-content-is-longer")
        with secure_open(target) as f:
            f.write("second")

        assert target.read_text() == "second"

    def test_preserves_permissions_on_overwrite(self, tmp_path):
        target = tmp_path / "token.json"
        with secure_open(target) as f:
            f.write("v1")
        with secure_open(target) as f:
            f.write("v2")

        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode == 0o600

    def test_custom_mode(self, tmp_path):
        target = tmp_path / "token.json"
        with secure_open(target, mode=0o400) as f:
            f.write("readonly")

        mode = stat.S_IMODE(target.stat().st_mode)
        assert mode == 0o400

    def test_yields_writable_file_handle(self, tmp_path):
        target = tmp_path / "token.json"
        with secure_open(target) as f:
            assert not f.closed
            assert f.writable()
        assert f.closed
