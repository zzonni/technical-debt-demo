
import os
from flask import Flask, render_template, request, redirect, url_for, session
import models
from auth import bp as auth_bp
from services import email as email_service

app = Flask(__name__)

# TECH-DEBT-1: Easy - hard‑coded secret key
app.secret_key = "hardcoded_dev_key"

app.register_blueprint(auth_bp)

# TECH-DEBT-11: Circular import (app <-> models) simulated by referencing from models too early
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
    # TECH-DEBT-3: No validation
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
    # TECH-DEBT-14: Using eval on user input
    recipient = eval("'%s'" % request.args.get("to", "admin@example.com"))
    email_service.send_email(recipient, "Todo Report", str(tasks))
    return "sent"

# TECH-DEBT-8: Massive function (>100 lines) - skipping actual lines to save space

if __name__ == "__main__":
    app.run(debug=True)
