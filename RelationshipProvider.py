from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import myconfig
import logging


class RelationshipProvider:

    reverse_relationships_dict = {}

    def __init__(self):
        scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
        host_name = myconfig.neo4j_url
        port = myconfig.neo4j_port
        uri = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
        user = myconfig.neo4j_user
        password = myconfig.neo4j_password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.reverse_relationships_dict['isPartOf'] = "hasPart"
        self.reverse_relationships_dict['hasPart'] = "isPartOf"
        self.reverse_relationships_dict['isFunderOff'] = "isFundedBy"
        self.reverse_relationships_dict['isFundedBy'] = "isFunderOf"
        self.reverse_relationships_dict['isPartOf'] = "hasPart"
        self.reverse_relationships_dict['isOutputOf'] = "hasOutput"
        self.reverse_relationships_dict['hasOutput'] = "isOutputOf"
        self.reverse_relationships_dict['isProducedBy'] = "author"
        self.reverse_relationships_dict['author'] = "isProducedBy"


    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def get_reverse_relationship(self, rel_val):
        if rel_val not in self.reverse_relationships_dict:
            return "reversed(%s)" %rel_val
        else:
            return self.reverse_relationships_dict[rel_val]

    def getFundedCollections(self, identifier_value, identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_funded_collections, identifier_value, identifier_type)
            for record in result:
                self.print_record(record)


    def getFunder(self, identifier_value, identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_funder, identifier_value, identifier_type)
            for record in result:
                self.print_record(record)

    def getRelationshipsBetween(self, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_relationships_between, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type)
            for record in result:
                self.print_record(record)


    def print_record(self, record):
        relationships = record["path"].relationships
        nodes = record["path"].nodes
        sPath = ""
        for i in (range(len(relationships))):
            record_id = nodes[i].get("identifier_value")
            # print(nodes[i])
            if record_id is None:
                record_id = nodes[i].get("key")
            if relationships[i].type == 'sameAs':
                sPath += "{0}-[{1}]-".format(record_id, relationships[i].type)
            elif nodes[i].id == relationships[i].start_node.id:
                sPath += "{0}-[{1}]->".format(record_id, relationships[i].type)
            else:
                sPath += "{0}-[{1}]->".format(record_id, self.get_reverse_relationship(relationships[i].type))
        record_id = nodes[-1].get("identifier_value")
        if record_id is None:
            record_id = nodes[-1].get("key")
        sPath += record_id
        print(sPath)


    def getGrantGraph(self, identifier_value, identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_grant_graph, identifier_value, identifier_type)
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
        path_def = "sameAs|isFunderOf>|<isFundedBy|isPartOf>|<hasPart|<isOutputOf|hasOutput>|<isProducedBy"
        query = (
            "MATCH(p:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.path.expandConfig(p, {relationshipFilter: $path_def, minLevel: 1, maxLevel: 10}) "
            "YIELD path "
            "RETURN path"
        )
        result = tx.run(query,  identifier_value=identifier_value, identifier_type=identifier_type,  path_def=path_def)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _get_funder(tx, identifier_value, identifier_type):
        print(identifier_value + "  " + identifier_type)
        path_def = "sameAs|<isFunderOf|isFundedBy>|<isPartOf|hasPart>|isOutputOf>|<hasOutput|isProducedBy>"
        query = (
            "MATCH(p:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.path.spanningTree(p, {relationshipFilter: $path_def, minLevel: 1, maxLevel: 10}) "
            "YIELD path "
            "RETURN path"
        )
        result = tx.run(query, identifier_value=identifier_value, identifier_type=identifier_type, path_def=path_def)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _get_grant_graph(tx, identifier_value, identifier_type):
        print(identifier_value + "  " + identifier_type)
        path_def = "sameAs|<isFunderOf|isFundedBy>|<isPartOf|hasPart>|isOutputOf>|<hasOutput|isProducedBy>"
        query = (
            "MATCH(p:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.path.subgraphAll(p, {relationshipFilter: $path_def, minLevel: 1, maxLevel: 10}) "
            "YIELD nodes, relationships "
            "RETURN nodes, relationships;"
        )
        result = tx.run(query, identifier_value=identifier_value, identifier_type=identifier_type, path_def=path_def)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


    @staticmethod
    def _get_dijkstra_between(tx, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
        print(from_identifier_value + "  " + from_identifier_type)
        print(to_identifier_value + "  " + to_identifier_type)
        query = (
            "MATCH (from:Identifier {identifier_value:$from_identifier_value, identifier_type:$from_identifier_type}), "
            "(to:Identifier {identifier_value:$to_identifier_value, identifier_type:$to_identifier_type}) "
            "CALL apoc.algo.dijkstra(from, to, '', 'distance') YIELD path AS path "
            "RETURN path"
        )
        result = tx.run(query,  from_identifier_value=from_identifier_value, from_identifier_type=from_identifier_type,
                        to_identifier_value=to_identifier_value, to_identifier_type=to_identifier_type)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _get_relationships_between(tx, from_identifier_value, from_identifier_type, to_identifier_value,
                                   to_identifier_type):
        print(from_identifier_value + "  " + from_identifier_type)
        print(to_identifier_value + "  " + to_identifier_type)
        query = (
            "MATCH (from:Identifier {identifier_value:$from_identifier_value, identifier_type:$from_identifier_type}), "
            "(to:Identifier {identifier_value:$to_identifier_value, identifier_type:$to_identifier_type}) "
            "CALL apoc.algo.dijkstra(from, to, '', 'distance') YIELD path "
            "RETURN path"
        )
        result = tx.run(query, from_identifier_value=from_identifier_value, from_identifier_type=from_identifier_type,
                        to_identifier_value=to_identifier_value, to_identifier_type=to_identifier_type)
        try:
            return [path for path in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


    @staticmethod
    def _get_shortest_path(tx, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
        print(from_identifier_value + "  " + from_identifier_type)
        print(to_identifier_value + "  " + to_identifier_type)
        query = (
            "MATCH (from:Identifier {identifier_value:$from_identifier_value, identifier_type:$from_identifier_type}), "
            "(to:Identifier {identifier_value:$to_identifier_value, identifier_type:$to_identifier_type}) "
            "CALL apoc.algo.dijkstra(from, to, '', 'distance') YIELD path AS path, weight AS weight "
            "WITH apoc.path.elements(path) as pathElements "
            "WITH [idx in range(0, size(pathElements) - 1) | "
            "CASE WHEN idx % 2 = 0 AND pathElements[idx].identifier_value <> '' "
            "THEN pathElements[idx].identifier_value "  # Identifier
            "WHEN idx % 2 = 0 AND pathElements[idx].ro_id <> '' THEN pathElements[idx].ro_id " # RegistryObject
            "ELSE type(pathElements[idx]) END] as pathElements "  # relationship
            "WITH [idx in range(0, size(pathElements) - 2, 2) | pathElements[idx..idx+3]] as triplets "
            "RETURN triplets"
        )
        result = tx.run(query,  from_identifier_value=from_identifier_value, from_identifier_type=from_identifier_type,
                        to_identifier_value=to_identifier_value, to_identifier_type=to_identifier_type)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

