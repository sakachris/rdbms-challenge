"""Database initialization utilities"""
from datetime import datetime


def init_database(db):
    """Initialize database schema"""
    if 'users' not in db.list_tables():
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


def insert_sample_data(db):
    """Insert sample data for demonstration"""
    today = datetime.now().date().isoformat()
    
    # Sample users
    users = [
        (1, 'alice', 'alice@example.com', 'Alice Smith', '2025-01-01'),
        (2, 'bob', 'bob@example.com', 'Bob Johnson', '2025-01-02'),
        (3, 'charlie', 'charlie@example.com', 'Charlie Brown', '2025-01-03'),
    ]
    
    for user_id, username, email, full_name, created in users:
        db.execute(f"""
            INSERT INTO users (id, username, email, full_name, created_at)
            VALUES ({user_id}, '{username}', '{email}', '{full_name}', '{created}')
        """)
    
    # Sample posts
    posts = [
        (1, 'Welcome to SimplDB Blog', 'This is our first blog post using SimplDB, a custom-built relational database!', 1, True, '2025-01-10', '2025-01-10'),
        (2, 'Building Your Own Database', 'Learn how to build a simple RDBMS from scratch using Python.', 1, True, '2025-01-11', '2025-01-11'),
        (3, 'Getting Started with SQL', 'A beginner\'s guide to SQL queries and database design.', 2, True, '2025-01-12', '2025-01-12'),
        (4, 'Draft Post', 'This post is not yet published.', 2, False, today, today),
    ]
    
    for post_id, title, content, author_id, published, created, updated in posts:
        db.execute(f"""
            INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
            VALUES ({post_id}, '{title}', '{content}', {author_id}, {published}, '{created}', '{updated}')
        """)
    
    # Sample comments
    comments = [
        (1, 1, 2, 'Great post! Looking forward to more content.', '2025-01-10'),
        (2, 1, 3, 'Very informative, thank you!', '2025-01-10'),
        (3, 2, 3, 'This is exactly what I was looking for!', '2025-01-11'),
    ]
    
    for comment_id, post_id, user_id, content, created in comments:
        db.execute(f"""
            INSERT INTO comments (id, post_id, user_id, content, created_at)
            VALUES ({comment_id}, {post_id}, {user_id}, '{content}', '{created}')
        """)
    
    print("✓ Sample data inserted")