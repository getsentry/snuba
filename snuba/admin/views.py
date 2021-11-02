from flask import Flask, Response

application = Flask(__name__, static_url_path="", static_folder="dist")


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")
