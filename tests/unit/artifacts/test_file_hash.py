from dbt.artifacts.resources.base import FileHash


class TestFileHashFromPathLegacy:
    def test_lf_only_matches_from_path(self, tmp_path):
        """On a file with \\n only, from_path_legacy and from_path produce the same hash."""
        seed = tmp_path / "seed.csv"
        seed.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        legacy = FileHash.from_path_legacy(str(seed))
        new = FileHash.from_path(str(seed))
        assert legacy == new

    def test_crlf_differs_from_from_path(self, tmp_path):
        """On a file with \\r\\n, from_path_legacy preserves \\r\\n while from_path normalizes."""
        seed = tmp_path / "seed.csv"
        seed.write_bytes(b"id,name\r\n1,Alice\r\n2,Bob\r\n")

        legacy = FileHash.from_path_legacy(str(seed))
        new = FileHash.from_path(str(seed))
        # Legacy keeps \r\n, from_path opens in text mode normalizing to \n
        assert legacy.checksum != new.checksum
        assert legacy.name == new.name == "sha256"

    def test_crlf_legacy_matches_from_contents_with_crlf(self, tmp_path):
        """from_path_legacy on a CRLF file matches from_contents called with the CRLF content."""
        content = "id,name\r\n1,Alice\r\n2,Bob"
        seed = tmp_path / "seed.csv"
        seed.write_bytes((content + "\r\n").encode("utf-8"))

        legacy = FileHash.from_path_legacy(str(seed))
        expected = FileHash.from_contents(content)
        assert legacy == expected

    def test_from_path_legacy_strips_whitespace(self, tmp_path):
        """from_path_legacy strips leading/trailing whitespace like the old implementation."""
        seed = tmp_path / "seed.csv"
        seed.write_bytes(b"  id,name\n1,Alice\n  ")

        legacy = FileHash.from_path_legacy(str(seed))
        expected = FileHash.from_contents("id,name\n1,Alice")
        assert legacy == expected
