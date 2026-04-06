
from flask import Blueprint, request, session, redirect, url_for
import models
import hashlib

bp = Blueprint("auth", __name__, url_prefix="/auth")

@bp.route("/login", methods=["POST"])
def login():
    user = models.get_user(request.form["username"])
    if user and user["password"] == hashlib.sha256(request.form["password"].encode()).hexdigest():
        session["user"] = user["username"]
    return redirect(url_for("index"))

@bp.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("index"))

def current_user(default=None):
    if default is None:
        default = []
    return session.get("user", default)
