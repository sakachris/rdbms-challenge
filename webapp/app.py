"""SimplDB Blog - Flask Web Application"""
from flask import Flask

from webapp.config import Config
from simpldb import Database
from webapp.utils.db_init import init_database
from webapp.routes.main import register_main_routes
from webapp.routes.posts import register_post_routes
from webapp.routes.api import register_api_routes


def create_app(config_class=Config):
    """Application factory"""
    app = Flask(__name__)
    app.config.from_object(config_class)
    app.secret_key = config_class.SECRET_KEY

    # Initialize database
    db = Database(
        name=config_class.DATABASE_NAME,
        data_dir=config_class.DATABASE_DIR
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


# Local dev only
if __name__ == "__main__":
    app = create_app()
    app.run(
        debug=Config.DEBUG,
        host=Config.HOST,
        port=Config.PORT
    )



# """SimplDB Blog - Flask Web Application"""
# import os
# import sys
# from flask import Flask

# # Allow local imports when not installed as a package
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

# if PROJECT_ROOT not in sys.path:
#     sys.path.insert(0, PROJECT_ROOT)

# from simpldb import Database
# from config import Config
# from utils.db_init import init_database
# from routes.main import register_main_routes
# from routes.posts import register_post_routes
# from routes.api import register_api_routes


# def create_app(config_class=Config):
#     """Application factory"""
#     app = Flask(__name__)
#     app.config.from_object(config_class)
#     app.secret_key = config_class.SECRET_KEY

#     # Initialize database
#     db = Database(
#         name=config_class.DATABASE_NAME,
#         data_dir=config_class.DATABASE_DIR
#     )

#     # Initialize schema (idempotent)
#     init_database(db)

#     # Register routes
#     register_main_routes(app, db)
#     register_post_routes(app, db)
#     register_api_routes(app, db)

#     # Health endpoint (for Docker & Kubernetes)
#     @app.route("/health")
#     def health():
#         return {"status": "ok"}, 200

#     return app


# # Local dev only
# if __name__ == "__main__":
#     app = create_app()
#     app.run(
#         debug=Config.DEBUG,
#         host=Config.HOST,
#         port=Config.PORT
#     )