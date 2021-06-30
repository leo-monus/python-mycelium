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
        r.get_nested_collections("10378.1/1548442", "handle")

    def test_get_nested_collections_from_bottom(self):
        r = RelationshipProvider()
        r.get_nested_collections("griffith.edu.au/individual:n361ce8d6cea67950ab9a12edf4c83060", "ro_key")




    def test_get_path_between_two_identifiers(self):
        r = RelationshipProvider()
        relationship = r.getRelationshipsBetween_shortest('501100000923', "fundref", "scu.edu.au/collgeo-001", "ro_key")
        self.assertEqual("isFunderOf", relationship)
        relationship = r.getRelationshipsBetween_shortest("scu.edu.au/collgeo-001", "ro_key",'501100000923', "fundref")
        self.assertEqual("isFundedBy", relationship)


    def test_get_grant_network_2(self):
        r = RelationshipProvider()
        r.getGrantGraph("https://docs.opengeospatial.org/is/15-078r6/15-078r6.html", "uri")



    if __name__ == '__main__':
        unittest.main()