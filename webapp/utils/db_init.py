"""Database initialization utilities"""

from webapp.utils.sample_data import insert_sample_data


def init_database(db):
    """Initialize database schema"""
    if "users" not in db.list_tables():
        create_schema(db)
        insert_sample_data(db)
        print("✓ Database schema created")
    else:
        print("✓ Database already initialized")


def create_schema(db):
    """Create database tables and indexes"""
    # Create users table
    db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            full_name VARCHAR(100),
            created_at DATE
        )
    """)

    # Create posts table
    db.execute("""
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            title VARCHAR(200) NOT NULL,
            content TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            published BOOLEAN DEFAULT FALSE,
            created_at DATE,
            updated_at DATE
        )
    """)

    # Create comments table
    db.execute("""
        CREATE TABLE comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at DATE
        )
    """)

    # Create indexes
    db.execute("CREATE INDEX idx_post_author ON posts(author_id)")
    db.execute("CREATE INDEX idx_comment_post ON comments(post_id)")
    db.execute("CREATE INDEX idx_comment_user ON comments(user_id)")
