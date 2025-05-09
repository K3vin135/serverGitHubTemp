from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def index():
    return "Servidor activo 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("✅ Webhook recibido:")
    print(data)
    return {"status": "ok"}, 200
