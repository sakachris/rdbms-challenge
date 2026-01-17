"""SimplDB Blog - Flask Web Application"""

from flask import Flask

from simpldb import Database
from webapp.config import Config
from webapp.routes.api import register_api_routes
from webapp.routes.main import register_main_routes
from webapp.routes.posts import register_post_routes
from webapp.utils.db_init import init_database


def create_app(config_class=Config):
    """Application factory"""
    app = Flask(__name__)
    app.config.from_object(config_class)
    app.secret_key = config_class.SECRET_KEY

    # Initialize database
    db = Database(
        name=config_class.DATABASE_NAME, data_dir=config_class.DATABASE_DIR
    )

    # Initialize schema (idempotent)
    init_database(db)

    # Register routes
    register_main_routes(app, db)
    register_post_routes(app, db)
    register_api_routes(app, db)

    # Health endpoint (for Docker & Kubernetes)
    @app.route("/health")
    def health():
        return {"status": "ok"}, 200

    return app


app = create_app()

# Local dev only
if __name__ == "__main__":
    app = create_app()
    app.run(debug=Config.DEBUG, host=Config.HOST, port=Config.PORT)
