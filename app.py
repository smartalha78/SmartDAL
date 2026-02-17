from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/api/test")
def test():
    return jsonify(status="ok Test")

@app.route("/api/health")
def health():
    return jsonify(status="ok")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
