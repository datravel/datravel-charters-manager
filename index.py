#coding: utf-8
from flask import Flask, Response
import simplejson
import config


app = Flask('index')
app.config.from_object(config)


@app.route('/api/v1/ping', methods = ['GET'])
def ping():
    return '', 200


@app.route('/api/v1/sources', methods = ['GET'])
def get_sources():
    source_list = []
    for source in config.sources:
        source_list.append({'name': source['name']})

    return Response(
        simplejson.dumps(source_list, ensure_ascii=False),
        mimetype='application/json'
    )


@app.route('/api/v1/source/<source_name>', methods = ['PUT', 'DELETE'])
def update_source(source_name):
    from importer import import_charter_tickets

    import_charter_tickets(source_name)
    return '', 204


if __name__ == '__main__':
    app.run()