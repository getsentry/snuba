from flask import Flask

application = Flask(__name__, static_url_path="")


@application.route("/")
def root() -> str:
    return "snuba admin"
