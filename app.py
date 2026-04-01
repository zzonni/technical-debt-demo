
import os
import re
from flask import Flask, render_template, request, redirect, url_for, session
import models
from auth import bp as auth_bp
from services import email as email_service

app = Flask(__name__)

secret_key = os.getenv("FLASK_SECRET_KEY")
if not secret_key:
    if os.getenv("PYTEST_CURRENT_TEST"):
        secret_key = "test-secret-key"
    else:
        raise RuntimeError("FLASK_SECRET_KEY not set")
app.config["SECRET_KEY"] = secret_key

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
    recipient = request.args.get("to", "admin@example.com")
    if not re.fullmatch(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", recipient):
        return "invalid recipient", 400
    email_service.send_email(recipient, "Todo Report", str(tasks))
    return "sent"


if __name__ == "__main__":
    app.run(debug=True)
