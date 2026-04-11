
import os
from flask import Flask, render_template, request, redirect, url_for, session
import models
from auth import bp as auth_bp
from services import email as email_service

app = Flask(__name__)

import os

# Load secret from environment; do not keep secrets in source.
app.secret_key = os.getenv("FLASK_SECRET_KEY")

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
    # Avoid eval() on user input; take the parameter directly and validate if needed.
    recipient = request.args.get("to", "admin@example.com")
    email_service.send_email(recipient, "Todo Report", str(tasks))
    return "sent"


if __name__ == "__main__":
    app.run(debug=True)
