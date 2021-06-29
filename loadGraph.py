from RegistryImporter import RegistryImporter

class App:

    def __init__(self):
        self.importer = RegistryImporter()


    def processRegistry(self):
        self.importer.import_all()

    def load_datasource(self, datasource_id):
        self.importer.import_datasource(datasource_id)

    def import_single(self, ro_id):
        self.importer.import_single(ro_id)

if __name__ == "__main__":
    app = App()
    # 20 is ARC
    # 117 is Southern Cross University
    #datasource_id = 20
    #app.load_datasource(datasource_id)
    ro_id = 68038
    app.import_single(ro_id)
