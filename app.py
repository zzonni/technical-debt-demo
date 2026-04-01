
import os
from flask import Flask, render_template, request, redirect, url_for, session
import models
from auth import bp as auth_bp
from services import email as email_service

app = Flask(__name__)

app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

app.register_blueprint(auth_bp)

models.create_user("demo", "demo")

def require_login():
    if "user" not in session:
        return redirect(url_for("index"))

@app.route("/")
def index():
    tasks = models.list_tasks(session.get("user"))
    return render_template("index.html", tasks=tasks)

@app.route("/add", methods=["POST"])
def add():
    text = request.form["text"]
    models.create_task(session.get("user"), text)
    return redirect(url_for("index"))

@app.route("/toggle/<int:task_id>", methods=["POST"])
def toggle(task_id):
    t = models.find_task(task_id)
    if t:
        t["status"] = "done" if t["status"] == "open" else "open"
    return redirect(url_for("index"))

@app.route("/mail_report")
def mail_report():
    tasks = models.list_tasks(session.get("user"))
    # Safely parse/validate recipient email
    to_addr = request.args.get("to", "admin@example.com")
    # Basic validation: ensure there's an '@' and no suspicious characters [Inference]
    if "@" not in to_addr or any(c in to_addr for c in [';', '\\n', '\\r']):
        return "invalid recipient", 400
    email_service.send_email(to_addr, "Todo Report", str(tasks))
    return "sent"


if __name__ == "__main__":
    app.run(debug=True)
