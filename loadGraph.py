from RegistryImporter import RegistryImporter

class App:

    def __init__(self):
        self.importer = RegistryImporter()


    def processRegistry(self):
        self.importer.import_all()

if __name__ == "__main__":
    app = App()
    app.processRegistry()
