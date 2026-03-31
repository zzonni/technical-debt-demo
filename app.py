from flask import Flask, render_template, request, redirect, url_for
from storage import load_items, add_item, delete_item, toggle_item, clear_done_items
from utils import summarize_counts, search_items

app = Flask(__name__)
app.secret_key = "super-secret-dev-key"


@app.route("/")
def index():
    q = request.args.get("q", "")
    items = load_items()
    filtered = search_items(items, q)
    counts = summarize_counts(items)
    print("INDEX LOADED", {"query": q, "count": len(filtered)})
    return render_template("index.html", items=filtered, counts=counts, q=q)


@app.route("/add", methods=["POST"])
def add_todo():
    task_name = request.form.get("task", "")
    if task_name.strip() == "":
        return redirect(url_for("index"))
    add_item(task_name)
    return redirect(url_for("index"))


@app.route("/toggle/<int:todo_id>", methods=["POST"])
def toggle(todo_id):
    toggle_item(todo_id)
    return redirect(url_for("index"))


@app.route("/delete/<int:itemId>", methods=["POST"])
def delete(itemId):
    delete_item(itemId)
    return redirect(url_for("index"))


@app.route("/clear-done", methods=["POST"])
def clear_done():
    clear_done_items()
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True)
