import unittest
from RelationshipProvider  import RelationshipProvider


class test_queries(unittest.TestCase):


    def test_funded_collections_record(self):
        r = RelationshipProvider()
        r.getFundedCollections("http://purl.org/au-research/grants/arc/DP0987351", "ro_key")

    def test_get_funder_for_collection(self):
        r = RelationshipProvider()
        r.getFundedCollections('501100000923', "fundref")

    def test_funded_collections_record(self):
        r = RelationshipProvider()
        r.getFundedCollections("http://dx.doi.org/10.13039/501100003531", "doi")

    def test_get_funder_for_collection_scu(self):
        r = RelationshipProvider()
        r.getFunder("scu.edu.au/collgeo-001", "ro_key")


    def test_get_funder_for_collection_vic_u(self):
        r = RelationshipProvider()
        r.getFunder("vu/collection/ISI_02", "ro_key")

    def test_get_grant_network(self):
        r = RelationshipProvider()
        r.getGrantGraph("vu/collection/ISI_02", "ro_key")


    def test_get_path_between_two_identifiers(self):
        r = RelationshipProvider()
        r.getRelationshipsBetween('501100000923', "fundref", "scu.edu.au/collgeo-001", "ro_key")
        r.getRelationshipsBetween("scu.edu.au/collgeo-001", "ro_key",'501100000923', "fundref")

    def test_get_path_between_two_identifiers_vic(self):
        r = RelationshipProvider()
        r.getRelationshipsBetween("http://vuir.vu.edu.au/id/eprint/30481", "uri", '501100003531', "fundref")

    if __name__ == '__main__':
        unittest.main()