#coding: utf-8
from flask import Flask
import config


app = Flask('index')
app.config.from_object(config)


@app.route('/api/v1/ping', methods = ['GET'])
def ping():
    return '', 200


if __name__ == '__main__':
    app.run()