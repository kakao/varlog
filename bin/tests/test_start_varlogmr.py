import json
import unittest
from pathlib import Path

TESTDATA = Path(__file__) \
    .resolve() \
    .parent.parent.parent \
    .joinpath('testdata/varlogctl')


class StartVarlogmrTestCase(unittest.TestCase):
    def test_parse_add_metadata_repository_node_response(self):
        path = TESTDATA.joinpath('addmetadatarepositorynode.0.golden.json')
        with open(path) as f:
            mrnode = json.load(f)
            self.assertIn("nodeId", mrnode)


if __name__ == '__main__':
    unittest.main()
