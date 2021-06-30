from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import myconfig
import logging
import webservice
import threading
import time

class RelationshipProvider:

    reverse_relationships_dict = {}
    relation_hierarchy = {}

    def __init__(self):
        print("started")
        scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" URI scheme
        host_name = myconfig.neo4j_url
        port = myconfig.neo4j_port
        uri = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
        user = myconfig.neo4j_user
        password = myconfig.neo4j_password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.reverse_relationships_dict['isPartOf'] = "hasPart"
        self.reverse_relationships_dict['sameAs'] = "sameAs"
        self.reverse_relationships_dict['hasPart'] = "isPartOf"
        self.reverse_relationships_dict['isFunderOff'] = "isFundedBy"
        self.reverse_relationships_dict['isFundedBy'] = "isFunderOf"
        self.reverse_relationships_dict['isPartOf'] = "hasPart"
        self.reverse_relationships_dict['isOutputOf'] = "hasOutput"
        self.reverse_relationships_dict['hasOutput'] = "isOutputOf"
        self.reverse_relationships_dict['isProducedBy'] = "author"
        self.reverse_relationships_dict['author'] = "isProducedBy"
        self.reverse_relationships_dict['isManagedBy'] = "manages"
        self.reverse_relationships_dict['manages'] = "isManagedBy"
        self.reverse_relationships_dict['hasPrincipalInvestigator'] = "isPrincipalInvestigatorOf"
        self.reverse_relationships_dict['isPrincipalInvestigatorOf'] = "hasPrincipalInvestigator"
        self.relation_hierarchy["collection"] = 1
        self.relation_hierarchy["service"] = 2
        self.relation_hierarchy["activity"] = 3
        self.relation_hierarchy["party"] = 4

    def info(self):
        return self.reverse_relationships_dict

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

    def getRelationshipsBetween_shortest(self, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_relationships_between, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type)
            relationship = "sameAs"
            for record in result:
                print(record)
                relationships = record["path"].relationships
                nodes = record["path"].nodes
                sPath = ""

                current_type = "sameAs"
                high_class = 0
                start_class = "collection"
                end_class = "collection"
                for i in (range(len(relationships))):
                    # get the class of the registryObjects
                    # start or end Vertices can be Identifiers without a class mostrly the "sameAs" relationships
                    # in that case just keep chugging along until we get to an RO
                    #
                    if nodes[i].get("ro_class") is not None:
                        start_class = nodes[i].get("ro_class")
                    if nodes[i+1].get("ro_class") is not None:
                        end_class = nodes[i+1].get("ro_class")
                    # if the Node on the "left" is the start_node of the relationship we are good
                    # otherwise we need to "reverse" the type
                    if nodes[i].id == relationships[i].start_node.id:
                        current_type = relationships[i].type
                    else:
                        current_type = self.get_reverse_relationship(relationships[i].type)

                    # We've got a relationship that is changing from the previous one
                    # if it's not the "sameAs" which is not important here
                    # and one of the nodes are larger or equal to the last registryObjects in our hierarchy
                    # Party = 4 , Activity = 3, Service = 2, and Collection = 1
                    if current_type != "sameAs" and (self.relation_hierarchy[start_class] >= high_class
                                                     or self.relation_hierarchy[end_class] >= high_class):
                        # which way are we going?
                        # only upwards change is allowed !!! so store the highest
                        # in a variable
                        if self.relation_hierarchy[start_class] > high_class:
                            high_class = self.relation_hierarchy[start_class]
                        if self.relation_hierarchy[end_class] > high_class:
                            high_class = self.relation_hierarchy[end_class]
                        relationship = current_type
            print(relationship)
            return relationship


    def getRelationshipsBetween_all(self, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_paths_between, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type)
            the_relationships = {}
            for record in result:
                print(record)
                relationships = record["path"].relationships
                nodes = record["path"].nodes
                sPath = ""
                relationship = "sameAs"
                current_type = "sameAs"
                high_class = 0
                start_class = "collection"
                end_class = "collection"
                for i in (range(len(relationships))):
                    # get the class of the registryObjects
                    # start or end Vertices can be Identifiers without a class mostrly the "sameAs" relationships
                    # in that case just keep chugging along until we get to an RO
                    #

                    if nodes[i].get("ro_class") is not None:
                        start_class = nodes[i].get("ro_class")
                    if nodes[i+1].get("ro_class") is not None:
                        end_class = nodes[i+1].get("ro_class")
                    # if the Node on the "left" is the start_node of the relationship we are good
                    # otherwise we need to "reverse" the type
                    if nodes[i].id == relationships[i].start_node.id:
                        current_type = relationships[i].type
                    else:
                        current_type = self.get_reverse_relationship(relationships[i].type)

                    # We've got a relationship that is changing from the previous one
                    # if it's not the "sameAs" which is not important here
                    # and one of the nodes are larger or equal to the last registryObjects in our hierarchy
                    # Party = 4 , Activity = 3, Service = 2, and Collection = 1
                    if current_type != "sameAs" and (self.relation_hierarchy[start_class] >= high_class
                                                     or self.relation_hierarchy[end_class] >= high_class):
                        # which way are we going?
                        # only upwards change is allowed !!! so store the highest
                        # in a variable
                        if self.relation_hierarchy[start_class] > high_class:
                            high_class = self.relation_hierarchy[start_class]
                        if self.relation_hierarchy[end_class] > high_class:
                            high_class = self.relation_hierarchy[end_class]
                        relationship = current_type
                    print(start_class, end_class,current_type, high_class)
                print(relationship)
                print("###############")
                if not(relationship in the_relationships.keys()):
                    the_relationships[relationship] = 1
                else:
                    the_relationships[relationship] += 1
            return the_relationships


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
        return sPath


    def get_nested_collections(self, identifier_value, identifier_type):
        with self.driver.session() as session:
            response = ""
            result = session.read_transaction(self._get_top_level_collection, identifier_value, identifier_type)
            last_length = 0
            top_node = None
            for record in result:
                if len(record["path"].relationships) > last_length:
                    top_node = record["path"].nodes[-1]
                    last_length = len(record["path"].relationships)
            print(top_node)
            if top_node.get("identifier_value") is not None:
                result = session.read_transaction(self._get_nested_collections, top_node.get("identifier_value"), top_node.get("identifier_type"))
            elif top_node.get("key") is not None:
                result = session.read_transaction(self._get_nested_collections, top_node.get("ikey"), "ro_key")
            for record in result:
                response += self.print_record(record)
                response += "<br/>"
            return response



    def getGrantGraph(self, identifier_value, identifier_type):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.read_transaction(self._get_grant_graph, identifier_value, identifier_type)
            for record in result:
                self.print_record(record)


    @staticmethod
    def _get_top_level_collection(tx, identifier_value, identifier_type):
        path_def = "sameAs|isPartOf>|<hasPart"
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
    def _get_nested_collections(tx, identifier_value, identifier_type):
        path_def = "sameAs|<isPartOf|hasPart>"
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
    def _get_funded_collections(tx, identifier_value, identifier_type):
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
        path_def = "sameAs|<isFunderOf|isFundedBy>|<isPartOf|hasPart>|isOutputOf>|<hasOutput|isProducedBy>"
        query = (
            "MATCH(p:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.path.expandConfig(p, {relationshipFilter: $path_def, minLevel: 1, maxLevel: 10}) "
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
        path_def = "sameAs|isFunderOf|isFundedBy|isPartOf|hasPart|isOutputOf|hasOutput|isProducedBy"
        query = (
            "MATCH(p:Identifier {identifier_value: $identifier_value, identifier_type: $identifier_type}) "
            "CALL apoc.path.expandConfig(p, {relationshipFilter: $path_def, minLevel: 1, maxLevel: 5}) "
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
    def _get_dijkstra_between(tx, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
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
    def _get_paths_between(tx, from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type):
        query = (
            "MATCH (from:Identifier {identifier_value:$from_identifier_value, identifier_type:$from_identifier_type}), "
            "(to:Identifier {identifier_value:$to_identifier_value, identifier_type:$to_identifier_type}) "
            "CALL apoc.algo.allSimplePaths(from, to, '', 3) YIELD path AS path "
            "RETURN path"
        )
        result = tx.run(query, from_identifier_value=from_identifier_value, from_identifier_type=from_identifier_type,
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


    def run(self):


        # Starting the web interface as a different thread
        try:
            web_port = getattr(myconfig, 'web_port', 7001)
            web_host = getattr(myconfig, 'web_host', '0.0.0.0')
            http = webservice.new(daemon=self)
            threading.Thread(
                target=http.run,
                kwargs={
                    'host': web_host,
                    'port': web_port,
                    'debug': False
                },
                daemon=True
            ).start()
            logging.debug("\n\nWeb Thread started at port %s \n\n" % web_port)
        except Exception as e:
            logging.error("error %r" % e)
            pass
        try:
            while True:
                self.info()
                time.sleep(1000)
        except (KeyboardInterrupt, SystemExit):
            logging.info("\n\nSTOPPING...")
            #self.shutDown()
        except Exception as e:
            logging.error("error %r" %(e))
            pass

if __name__ == "__main__":
    rp = RelationshipProvider()
    rp.run()