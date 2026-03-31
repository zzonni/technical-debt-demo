
from flask import Blueprint, request, session, redirect, url_for
import models

bp = Blueprint("auth", __name__, url_prefix="/auth")

# TECH-DEBT-2: Easy - No password hashing
@bp.route("/login", methods=["POST"])
def login():
    user = models.get_user(request.form["username"])
    if user and user["password"] == request.form["password"]:
        session["user"] = user["username"]
    return redirect(url_for("index"))

@bp.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("index"))

# TECH-DEBT-15: Mutable default arg
def current_user(default=[]):
    return session.get("user", default)
