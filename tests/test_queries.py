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

    def test_get_nested_collections(self):
        r = RelationshipProvider()
        r.get_nested_collections("vu/collection/ISI_02", "ro_key")

    def test_get_nested_collections_from_top(self):
        r = RelationshipProvider()
        result = r.get_nested_collections("AODN:440cd6dc-eb6e-46a1-9574-33b58a739db4", "ro_key")
        print(result)

    def test_get_nested_collections_from_bottom(self):
        r = RelationshipProvider()
        result = r.get_nested_collections("http://vivo.curtin.edu.au/vivo/individual/tm66442326", "ro_key")
        print(result)



    def test_get_path_between_two_identifiers(self):
        r = RelationshipProvider()
        relationship = r.getRelationshipsBetween_shortest('501100000923', "fundref", "scu.edu.au/collgeo-001", "ro_key")
        self.assertEqual("isFunderOf", relationship)
        relationship = r.getRelationshipsBetween_shortest("scu.edu.au/collgeo-001", "ro_key",'501100000923', "fundref")
        self.assertEqual("isFundedBy", relationship)


    def test_get_grant_network_2(self):
        r = RelationshipProvider()
        r.getGrantGraph("Activity1", "ro_key")



    if __name__ == '__main__':
        unittest.main()