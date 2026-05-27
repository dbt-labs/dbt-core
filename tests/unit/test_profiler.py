import os
import tempfile

from dbt.profiler import profiler


def test_profiler_writes_stats_file():
    """profiler() context manager should always profile and dump stats."""
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, "stats.prof")
        with profiler(outfile=outfile):
            # do a tiny bit of work to populate stats
            sum(range(1000))
        assert os.path.isfile(outfile)
        assert os.path.getsize(outfile) > 0


def test_profiler_accepts_positional_outfile():
    with tempfile.TemporaryDirectory() as tmpdir:
        outfile = os.path.join(tmpdir, "stats.prof")
        with profiler(outfile):
            pass
        assert os.path.isfile(outfile)
