from utils.Database import DataBase
from GraphLoader import GraphLoader
import logging

class RegistryImporter:

    batch_size = 50

    def __init__(self):
        self.database = DataBase()
        self.driver = GraphLoader()


    def import_all(self):
        self.import_registry_objects("")
        self.import_registry_object_relationships("")
        self.import_identifiers("")
        self.import_identifier_relationships("")


    def import_datasource(self, datasource_id):
        #self.import_registry_objects(datasource_id)
        self.import_registry_object_relationships(datasource_id)
        self.import_identifiers(datasource_id)
        self.import_identifier_relationships(datasource_id)


    def import_single(self, ro_id):
        #self.import_single_registry_objects(ro_id)
        self.import_single_registry_object_relationships(ro_id)
        #self.import_single_registry_object_identifiers(ro_id)
        #self.import_single_registry_objec_identifier_relationships(ro_id)


    def import_single_registry_object_relationships(self, ro_id):
        tablename = "dbs_registry.registry_object_relationships"
        where = "WHERE registry_object_id = %s" % ro_id
        query = "SELECT * from %s %s" % (tablename, where)
        print(query)
        try:
            conn = self.database.getConnection()
            cur = conn.cursor()
            cur.execute(query)
            if cur.rowcount > 0:
                for r in cur:
                    self.import_registry_object_relationship(r)
            cur.close()
            del cur
            conn.close()
        except Exception as exception:
            print(exception)
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))

    def import_registry_objects(self, datasource_id):
        tablename = "dbs_registry.registry_objects"
        where = ""
        if datasource_id != "":
            where = "WHERE registry_object_id in (SELECT registry_object_id FROM dbs_registry.registry_objects where data_source_id = %d)" % datasource_id
        size = self.getsizeof(tablename + " " + where)
        page = 0
        while (page * self.batch_size) < size:
            query = "SELECT * from %s %s order by 1 limit %d offset %d" % (tablename, where, self.batch_size, (page * self.batch_size))
            print(query)
            try:
                print(page)
                conn = self.database.getConnection()
                cur = conn.cursor()
                cur.execute(query)
                if cur.rowcount > 0:
                    for r in cur:
                        self.import_registry_object(r)
                cur.close()
                del cur
                conn.close()
                page = page + 1
            except Exception as exception:
                print(exception)
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))

    def import_registry_object_relationships(self, datasource_id):
        tablename = "dbs_registry.registry_object_relationships"
        where = ""
        if datasource_id != "":
            where = "WHERE registry_object_id in (SELECT registry_object_id FROM dbs_registry.registry_objects where data_source_id = %d)" % datasource_id
        size = self.getsizeof(tablename + " " + where)

        page = 0
        while (page * self.batch_size) < size:
            query = "SELECT * from %s %s order by 1 limit %d offset %d" % (tablename, where, self.batch_size, (page * self.batch_size))
            print(query)
            try:
                print(page)
                conn = self.database.getConnection()
                cur = conn.cursor()
                cur.execute(query)
                if cur.rowcount > 0:
                    for r in cur:
                        self.import_registry_object_relationship(r)
                cur.close()
                del cur
                conn.close()
                page = page + 1
            except Exception as exception:
                print(exception)
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))


    def import_identifiers(self, datasource_id):
        tablename = "dbs_registry.registry_object_identifiers"
        where = ""
        if datasource_id != "":
            where = "WHERE registry_object_id in (SELECT registry_object_id FROM dbs_registry.registry_objects where data_source_id = %d)" % datasource_id
        size = self.getsizeof(tablename + " " + where)

        page = 0
        while (page * self.batch_size) < size:
            query = "SELECT * from %s %s order by 2 limit %d offset %d" % (tablename, where, self.batch_size, (page * self.batch_size))
            print(query)
            try:
                print(page)
                conn = self.database.getConnection()
                cur = conn.cursor()
                cur.execute(query)
                if cur.rowcount > 0:
                    for r in cur:
                        self.import_identifier(r)
                cur.close()
                del cur
                conn.close()
                page = page + 1
            except Exception as exception:
                print(exception)
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))


    def import_identifier_relationships(self, datasource_id):
        tablename = "dbs_registry.registry_object_identifier_relationships"
        where = ""
        if datasource_id != "":
            where = "WHERE registry_object_id in (SELECT registry_object_id FROM dbs_registry.registry_objects where data_source_id = %d)" % datasource_id
        size = self.getsizeof(tablename + " " + where)

        page = 0
        while (page * self.batch_size) < size:
            query = "SELECT * from %s %s order by 2 limit %d offset %d" % (tablename, where, self.batch_size, (page * self.batch_size))
            print(query)
            try:
                print(page)
                conn = self.database.getConnection()
                cur = conn.cursor()
                cur.execute(query)
                if cur.rowcount > 0:
                    for r in cur:
                        self.import_identifier_relationship(r)
                cur.close()
                del cur
                conn.close()
                page = page + 1
            except Exception as exception:
                print(exception)
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))



    def getsizeof(self, tablename):
        query = "SELECT count(*) from %s" % tablename
        number_of_entries = 0
        try:
            conn = self.database.getConnection()
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()
            number_of_entries = result[0]
            print("number of entries %s" %number_of_entries)
            cur.close()
            del cur
            conn.close()
        except Exception as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
        return int(number_of_entries)

        #return 100


    def import_identifier_relationship(self, row):
        self.driver.create_identifier_relationship(row)
        #print(row[1],row[2],row[4],row[3],row[5])

    def import_identifier(self, row):
        self.driver.create_identifier(row)
        #print(row[1], row[2], row[3])

    def import_registry_object(self, row):
        self.driver.create_registry_object(row)
        #print(row[0], row[5], row[2], row[3])


    def import_registry_object_relationship(self, row):
        print(row[0], row[1], row[2], row[4])
        self.driver.create_registry_object_relationship(row)

