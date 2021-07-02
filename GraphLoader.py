from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import myconfig
import logging


class GraphLoader:

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


    def create_registry_object(self, row):
        # name=row[0],
        # title=row[5],
        # ro_id=row[0],
        # key=row[2],
        # status=row[6],
        # ro_class=row[3],
        # slug=row[4]
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors

            result = session.write_transaction(
                self._create_registry_object, row)
            for record in result:
                print("Created RegistryObject Vertex: {p}".format(
                    p=record['p']))
            result = session.write_transaction(
                self._create_identifier, identifier_type="ro_key", identifier_value=row[2], ro_class=row[3])
            for record in result:
                print("Created Identifier for roKey Vertex: {n}".format(
                    n=record['n']))
            result = session.write_transaction(
                self._create_same_as_relationship_to_identifier_vertex, "ro_key", row[2], row[0])
            for record in result:
                print("Created sameAs relationship between key and Identifier: {r}".format(
                    r=record['r']))



    @staticmethod
    def _create_registry_object(tx, row):
        query = (
            "MERGE (p:RegistryObject {name:$name, title:$title, ro_id:$ro_id, "
            "key:$key, status:$status, ro_class:$ro_class, slug:$slug}) "
            "RETURN p"
        )
        #print(row)
        #exit(0)
        result = tx.run(query, name=row[0], title=row[5], ro_id=row[0],
                        key=row[2], status=row[6], ro_class=row[3], slug=row[4])
        try:
            return [{"p": record["p"]["title"]}
                    for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


    def create_identifier(self, row):
        #print(row)
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_identifier, row[3], row[2], row[4])
            for record in result:
                print("Created Identifier Vertex: {n}".format(
                    n=record['n']))
            result = session.write_transaction(
                self._create_same_as_relationship_to_identifier_vertex, row[3], row[2], row[1])
            for record in result:
                print("Created sameAs to Identifier: {r}".format(
                    r=record['r']))

    @staticmethod
    def _create_identifier(tx, identifier_type, identifier_value, ro_class):

        if ro_class is None:
            ro_class = "unknown"
        if ro_class != "unknown":
            query = (
                "MERGE (n:Identifier {identifier_type:$identifier_type, identifier_value:$identifier_value}) "
                "ON CREATE SET n.ro_class = $ro_class "
                "RETURN n"
            )
        else:
            query = (
                "MERGE (n:Identifier {identifier_type:$identifier_type, identifier_value:$identifier_value}) "
                "ON CREATE SET n.ro_class = $ro_class "
                "ON MATCH SET n.ro_class = $ro_class "
                "RETURN n"
                )
        #print(row)
        #exit(0)
        result = tx.run(query, identifier_type=identifier_type, identifier_value=identifier_value, ro_class=ro_class)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _create_same_as_relationship_to_identifier_vertex(tx, identifier_type, identifier_value, ro_id):
        query = (
            "MATCH (p:RegistryObject {ro_id: $ro_id}) "
            "MATCH (i:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "MERGE (p)-[r:sameAs]->(i)"
            "RETURN r"
        )
        #print(row)
        #exit(0)
        result = tx.run(query, identifier_type=identifier_type, identifier_value=identifier_value, ro_id=ro_id)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_registry_object_relationship(self, row):
        if row[1] is None or row[1].strip() == '':
            return
        if row[4] is None or row[4].strip() == '':
            return
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_identifier, "ro_key", row[1], "unknown")
            for record in result:
                print("Created Identifier for roKey Vertex: {n}".format(
                    n=record['n']))
            #print(row[1], row[0], row[2], row[4])
            result = session.write_transaction(
                self._create_relationship_to_identifier_vertex, "ro_key", row[1], row[0], row[2], row[4])
            for record in result:
                print("Created relationship between key and Identifier: {rel}".format(
                    rel=record['rel']))


    def create_identifier_relationship(self, row):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_identifier, row[4], row[2], row[3])
            for record in result:
                print("Created Identifier Vertex: {n}".format(
                    n=record['n']))
            #print(row[4], row[2], row[1], row[3], row[5])
            # identifier_type, identifier_value, ro_id, to_class, relation_type
            result = session.write_transaction(
                self._create_relationship_to_identifier_vertex, row[4], row[2], row[1], row[3], row[5])
            for record in result:
                print("Created relationship between Identifier: {rel}".format(
                    rel=record['rel']))


    @staticmethod
    def _create_relationship_to_identifier_vertex(tx, identifier_type, identifier_value, ro_id, to_class, relation_type):
        if to_class is None:
            to_class = "collection"
        query = (
            "MATCH (p:RegistryObject {ro_id: $ro_id}) "
            "MATCH (i:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.merge.relationship(p, $relation_type, {to_class:$to_class}, {to_class:$to_class}, i, {}) "
            "YIELD rel "
            "RETURN rel;"
        )
        #print(row)
        #exit(0)
        result = tx.run(query, identifier_type=identifier_type, identifier_value=identifier_value, ro_id=ro_id,
                        relation_type=relation_type, to_class=to_class)
        try:
            return [record for record in result]

        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


    @staticmethod
    def _find_and_return_registry_object_by_id(tx, ro_id):
        query = (
            "MATCH (p:RegistryObject) "
            "WHERE p.ro_id = $ro_id "
            "RETURN p"
        )
        result = tx.run(query, ro_id=ro_id)
        return [record for record in result]


    @staticmethod
    def _find_and_return_identifier_by_value_and_type(tx, value, type):
        query = (
            "MATCH (i:Identifier) "
            "WHERE i.identifier_value = $value AND i.identifier_type = $type"
            "RETURN i"
        )
        result = tx.run(query, value=value, type=type)
        return [record for record in result]


