import unittest
from RelationshipProvider  import RelationshipProvider


class test_queries(unittest.TestCase):


    def test_funded_collections_record(self):
        r = RelationshipProvider()
        r.getFundedCollections("www.bioplatforms.com/party-1", "ro_key")

    def test_get_fundeder_for_collection(self):
        r = RelationshipProvider()
        r.getFundedCollections("www.bioplatforms.com/collection-1", "ro_key")

    if __name__ == '__main__':
        unittest.main()