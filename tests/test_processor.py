from base import BaseTest

from snuba.processor import SnubaProcessor

class TestProcessor(BaseTest):
    def test(self):
        processor = SnubaProcessor(None)
