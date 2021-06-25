from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import myconfig
import logging


class RelationshipProvider:

    def __init__(self):
        scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
        host_name = myconfig.neo4j_url
        port = myconfig.neo4j_port
        uri = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
        user = myconfig.neo4j_user
        password = myconfig.neo4j_password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()


    def getFundedCollections(self, identifier_value, identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_funded_collections, identifier_value, identifier_type)
            for record in result:
                print(record)
                # print(record['subject'])
                # print("...............")
                # print(record['object'])
                # print("...............")
                # print(record['predicate'])
                # print("####################")


    @staticmethod
    def _get_funded_collections(tx, identifier_value, identifier_type):
        print(identifier_value + "  " + identifier_type)
        path_def = "sameAs|isFunderOf|isFundedBy|isPartOf|hasPart|isProducedBy|isOutptOf|isOwnedBy"
        query = (
            "MATCH(p:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.path.expandConfig(p, {relationshipFilter: $path_def, minLevel: 1, maxLevel: 10}) "
            "YIELD path "
            "WITH apoc.path.elements(path) as pathElements "
            "WITH [idx in range(0, size(pathElements) - 1) | "
            "CASE WHEN idx % 2 = 0 AND pathElements[idx].identifier_value <> '' "
            "THEN pathElements[idx].identifier_value "  # Identifier
            "WHEN idx % 2 = 0 AND pathElements[idx].ro_id <> '' THEN pathElements[idx].ro_id " # RegistryObject
            "ELSE type(pathElements[idx]) END] as pathElements "  # relationship
            "WITH [idx in range(0, size(pathElements) - 2, 2) | pathElements[idx..idx+3]] as triplets "
            "RETURN triplets"
        )
        result = tx.run(query,  identifier_value=identifier_value, identifier_type=identifier_type,  path_def=path_def)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise





