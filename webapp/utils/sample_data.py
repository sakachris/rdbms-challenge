# ruff: noqa
"""Sample data insertion for demonstration purposes"""

from datetime import datetime


def insert_sample_data(db):
    """Insert sample data for demonstration"""
    today = datetime.now().date().isoformat()

    # Sample users
    users = [
        (1, "alice", "alice@example.com", "Alice Smith", "2026-01-01"),
        (2, "bob", "bob@example.com", "Bob Johnson", "2026-01-02"),
        (3, "charlie", "charlie@example.com", "Charlie Brown", "2026-01-03"),
    ]

    for user_id, username, email, full_name, created in users:
        db.execute(f"""
            INSERT INTO users (id, username, email, full_name, created_at)
            VALUES ({user_id}, '{username}', '{email}', '{full_name}', '{created}')
        """)

    # Sample posts
    posts = [
        (
            1,
            "Welcome to SimplDB Blog",
            "This is our first blog post using SimplDB, a custom-built relational database!",
            1,
            True,
            "2026-01-10",
            "2026-01-10",
        ),
        (
            2,
            "Building Your Own Database",
            "Learn how to build a simple RDBMS from scratch using Python.",
            1,
            True,
            "2026-01-11",
            "2026-01-11",
        ),
        (
            3,
            "Getting Started with SQL",
            "A beginner's guide to SQL queries and database design.",
            2,
            True,
            "2026-01-12",
            "2026-01-12",
        ),
        (
            4,
            "Draft Post",
            "This post is not yet published.",
            2,
            False,
            today,
            today,
        ),
    ]

    for (
        post_id,
        title,
        content,
        author_id,
        published,
        created,
        updated,
    ) in posts:
        db.execute(f"""
            INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
            VALUES ({post_id}, '{title}', '{content}', {author_id}, {published}, '{created}', '{updated}')
        """)

    # Sample comments
    comments = [
        (
            1,
            1,
            2,
            "Great post! Looking forward to more content.",
            "2026-01-10",
        ),
        (2, 1, 3, "Very informative, thank you!", "2026-01-10"),
        (3, 2, 3, "This is exactly what I was looking for!", "2026-01-11"),
    ]

    for comment_id, post_id, user_id, content, created in comments:
        db.execute(f"""
            INSERT INTO comments (id, post_id, user_id, content, created_at)
            VALUES ({comment_id}, {post_id}, {user_id}, '{content}', '{created}')
        """)

    print("✓ Sample data inserted")
