from dbt.events import AdapterLogger
from dbt.events.types import AdapterEventDebug
from unittest import TestCase


class TestAdapterLogger(TestCase):

    def setUp(self):
        pass

    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_basic_adapter_logging_interface(self):
        logger = AdapterLogger("dbt_tests")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.exception("exception message")
        logger.critical("exception message")
        self.assertTrue(True)

    # python loggers allow deferring string formatting via this signature:
    def test_formatting(self):
        logger = AdapterLogger("dbt_tests")
        # tests that it doesn't throw
        logger.debug("hello {}", 'world')

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="hello {}", args=('world',))
        self.assertTrue("hello world" in event.message())

        # tests that it doesn't throw
        logger.debug("1 2 {}", 3)

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="1 2 {}", args=(3,))
        self.assertTrue("1 2 3" in event.message())

        # tests that it doesn't throw
        logger.debug("boop{x}boop")

        # enters lower in the call stack to test that it formats correctly
        # in this case it's that we didn't attempt to replace anything since there
        # were no args passed after the initial message
        event = AdapterEventDebug(name="dbt_tests", base_msg="boop{x}boop", args=())
        self.assertTrue("boop{x}boop" in event.message())
