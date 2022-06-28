import json
from pathlib import Path
import unittest

TESTDATA = Path(__file__) \
    .resolve() \
    .parent.parent.parent \
    .joinpath('testdata/varlogctl')


class StartVarlogsnTestCase(unittest.TestCase):

    def test_parse_empty_list_storage_nodes_response(self):
        with open(TESTDATA.joinpath('liststoragenodes.0.golden.json')) as f:
            snms = json.load(f)
            self.assertEqual(len(snms), 0)

    def test_parse_list_storage_nodes_response(self):
        with open(TESTDATA.joinpath('liststoragenodes.1.golden.json')) as f:
            snms = json.load(f)
            self.assertGreater(len(snms), 0)
            self.assertIn("storageNodeId", snms[0])
            self.assertIn("address", snms[0])


if __name__ == '__main__':
    unittest.main()
