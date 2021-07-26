import uuid

class MyceliumNode:
    identifiers = {}
    properties = {}
    parent_node_id = None
    child_node_ids = []
    relationships = None
    id = None

    def __init__(self):
        self.id = uuid.uuid4()
        self.identifiers = {}
        self.properties = {}
        self.parent_node_id = None
        self.relationships = relationsShipByType()

    def getid(self):
        return self.id

    def getIdentifiers(self):
        return self.identifiers

    def addChildId(self, child_id):
        if child_id not in self.child_node_ids:
            self.child_node_ids.append(child_id)

    def getParentNodeId(self):
        return self.parent_node_id

    def setParentNodeId(self, parent_node_id):
        self.parent_node_id = parent_node_id

    def addIdentifier(self, identifier_type, identifier_value):
        self.identifiers[identifier_type] = identifier_value

    def getIdentifier(self, identifier_type):
        if identifier_type in self.identifiers.keys():
            return self.identifiers[identifier_type]
        else:
            return None

    def addProperty(self, property_value, property_type):
        self.properties[property_type] = property_value

    def getProperty(self, property_type):
        if property_type in self.properties.keys():
            return self.properties[property_type]
        else:
            return None

    def addRelated(self,realtionship_type, related_id):
        self.relationships.addRelated(realtionship_type, related_id)


    def print(self):
        print("UUID: %s Identifier: %s" %(format(self.id), format(self.identifiers)))
        self.relationships.print()


class relationsShipByType:
    relationshipdict = {}

    def __init__(self):
        self.relationshipdict = {}

    def getRelatedByList(self, realtionship_type):
        if realtionship_type in self.relationshipdict:
            return self.relationshipdict[realtionship_type]
        return None

    def addRelated(self, realtionship_type, related_id):
        if realtionship_type in self.relationshipdict:
            related_ids = self.relationshipdict[realtionship_type]
            if related_id not in related_ids:
                related_ids.append(related_id)
                self.relationshipdict[realtionship_type] = related_ids
        else:
            related_ids = []
            related_ids.append(related_id)
            self.relationshipdict[realtionship_type] = related_ids

    def print(self):
        for k in self.relationshipdict.keys():
            print("%s, %s" %(k, format(self.relationshipdict[k])))



class MyceliumNodeList:
    nodeList = None

    def __init__(self):
        self.nodeList = {}

    def addNode(self, identifier_type, identifier_value):
        #print("add %s,%s" %(identifier_type, identifier_value))
        node_id = self.findNode(identifier_type, identifier_value)
        if node_id is None:
            node = MyceliumNode()
            node.addIdentifier(identifier_type, identifier_value)
            self.nodeList[node.getid()] = node
        else:
            node = self.nodeList[node_id]
            node.addIdentifier(identifier_type, identifier_value)
            self.nodeList[node.getid()] = node
        return node.getid()


    def updateNode(self, identifier_type, identifier_value, some_other_identifier_type, some_other_identifier_value):
        #print("add other Identifier %s,%s ____ %s,%s" %(identifier_type, identifier_value, some_other_identifier_type, some_other_identifier_value))
        node_id = self.findNode(identifier_type, identifier_value)

        same_node_id = self.findNode(some_other_identifier_type, some_other_identifier_value)
        #print(node_id, same_node_id)
        if node_id is None and same_node_id is None:
            node = MyceliumNode()
            node.addIdentifier(identifier_type, identifier_value)
            node.addIdentifier(some_other_identifier_type, some_other_identifier_value)
            self.nodeList[node.getid()] = node
            return node.getid()
        elif same_node_id is None:
            node = self.nodeList.get(node_id)
            node.addIdentifier(some_other_identifier_type, some_other_identifier_value)
            self.nodeList[node.getid()] = node
            return node.getid()
        elif node_id is None:
            node = self.nodeList.get(same_node_id)
            node.addIdentifier(identifier_type, identifier_value)
            self.nodeList[node.getid()] = node
            return node.getid()



    def addRelatedNode(self, identifier_type, identifier_value, relationship_type, child_identifier_type, child_identifier_value):
        #print("add Related Node %s,%s __[%s]__ %s,%s" %(identifier_type, identifier_value, relationship_type, child_identifier_type, child_identifier_value))
        node_id = self.findNode(identifier_type, identifier_value)
        child_node_id = self.findNode(child_identifier_type, child_identifier_value)
        node = self.nodeList[node_id]
        node.addRelated(relationship_type,  child_node_id)
        self.nodeList[node_id] = node



    def findNode(self, identifier_type, identifier_value):
        if len(self.nodeList) == 0:
            return None
        else:
            for n in self.nodeList.keys():
                node = self.nodeList.get(n)
                if node.getIdentifier(identifier_type) == identifier_value:
                    return n
        return None
