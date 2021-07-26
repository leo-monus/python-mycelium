from flask import Flask, jsonify, request

def new(daemon):
    """
    Returns a new Flask instance for a Daemon
    Daemon must implements the info function

    :param daemon:
    :return: Flask
    """
    app = Flask(__name__)

    @app.route('/', methods=['GET'])
    def info():
        print("info")
        return jsonify(daemon.info())

    @app.route('/get_nested_collections', methods=['GET'])
    def getNestedCollectiomns():
        try:
            identifier_value = request.args.get('identifier_value', type=str)
            identifier_type = request.args.get('identifier_type', type=str)
            if identifier_value is None or identifier_value == "" :
                return "identifier_value is required"
            elif identifier_type is None or identifier_type == "":
                return "identifier_type is required"
            else:
                return daemon.get_nested_collections(identifier_value, identifier_type)
        except Exception as e:
            print(e)
            return

    @app.route('/get_collection_tree', methods=['GET'])
    def getNestedCollectiomn_tree():
        try:
            identifier_value = request.args.get('identifier_value', type=str)
            identifier_type = request.args.get('identifier_type', type=str)
            if identifier_value is None or identifier_value == "" :
                return "identifier_value is required"
            elif identifier_type is None or identifier_type == "":
                return "identifier_type is required"
            else:
                return daemon.get_nested_collections_subtree(identifier_value, identifier_type)
        except Exception as e:
            print(e)
            return


    @app.route('/get_implicit_relationship_between', methods=['GET'])
    def getImplicitRelationships():
        try:
            from_identifier_value = request.args.get('from_identifier_value', type=str)
            from_identifier_type = request.args.get('from_identifier_type', type=str)
            to_identifier_value = request.args.get('to_identifier_value', type=str)
            to_identifier_type = request.args.get('to_identifier_type', type=str)
            if from_identifier_value is None or from_identifier_value == "" :
                return "from_identifier_value is required"
            elif from_identifier_type is None or from_identifier_type == "":
                return "from_identifier_type is required"
            elif to_identifier_value is None or to_identifier_value == "":
                return "to_identifier_type is required"
            elif to_identifier_type is None or to_identifier_type == "":
                return "to_identifier_type is required"
            else:
                return daemon.getRelationshipsBetween_shortest(from_identifier_value, from_identifier_type, to_identifier_value, to_identifier_type)
        except Exception as e:
            print(e)
            return



    return app