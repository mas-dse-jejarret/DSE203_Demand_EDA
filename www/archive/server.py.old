from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"



@app.route("/histogram/<string:name>/")
def histogram(name):
    return name


@app.route('/api/add_message/<uuid>', methods=['GET', 'POST'])
def add_message(uuid):
        content = request.get_json(silent=True)
        print content
        return jsonify('{"h" : "ok"}')

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=80)


