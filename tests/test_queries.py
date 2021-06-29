import unittest
from RelationshipProvider  import RelationshipProvider


class test_queries(unittest.TestCase):


    def test_funded_collections_record(self):
        r = RelationshipProvider()
        r.getFundedCollections("http://purl.org/au-research/grants/arc/DP0987351", "ro_key")

    def test_get_funder_for_collection(self):
        r = RelationshipProvider()
        r.getFundedCollections('501100000923', "fundref")

    def test_get_funder_for_collection(self):
        r = RelationshipProvider()
        r.getFunder("scu.edu.au/collgeo-001", "ro_key")



    def test_get_path_between_two_identifiers(self):
        r = RelationshipProvider()
        r.getRelationshipsBetween('501100000923', "fundref", "scu.edu.au/collgeo-001", "ro_key")

    def test_get_path_between_two_identifiers(self):
        r = RelationshipProvider()
        r.getRelationshipsBetween("scu.edu.au/collgeo-001", "ro_key",'501100000923', "fundref")

    if __name__ == '__main__':
        unittest.main()