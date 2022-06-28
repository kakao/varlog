import json
import unittest
from pathlib import Path

TESTDATA = Path(__file__) \
    .resolve() \
    .parent.parent.parent \
    .joinpath('testdata/varlogctl')


class VmrCase(unittest.TestCase):
    def test_parse_list_metadata_repository_nodes_response(self):
        path = TESTDATA.joinpath('listmetadatarepositorynodes.0.golden.json')
        with open(path) as f:
            nodes = json.load(f)
            members = [node["raftURL"] for node in nodes]
            self.assertIsNotNone(members)
            self.assertEqual(len(members), 1)

    def test_parse_delete_metadata_repository_node_response(self):
        path = TESTDATA.joinpath('deletemetadatarepositorynode.0.golden.json')
        with open(path) as f:
            rsp = json.load(f)
            self.assertIsNotNone(rsp)


if __name__ == '__main__':
    unittest.main()
