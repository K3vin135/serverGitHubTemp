from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def home():
    return "Servidor Flask activo 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("✅ Webhook recibido:")
    print(data)
    return {"status": "ok"}, 200
