"""
SimplDB Database API Demo
Demonstrates the high-level Database interface
"""

import sys
import shutil
from pathlib import Path

sys.path.insert(0, '.')

from simpldb import Database


def cleanup():
    demo_dir = Path("demo_database")
    if demo_dir.exists():
        shutil.rmtree(demo_dir)


def main():
    print("=" * 70)
    print("SimplDB Database API Demo")
    print("=" * 70)
    
    cleanup()
    
    # 1. Create database
    print("\n1. Creating database...")
    db = Database(name="BlogDB", data_dir="demo_database")
    print(f"   ✓ Database '{db.name}' created at {db.data_dir}")
    
    # 2. Create tables
    print("\n2. Creating tables...")
    
    # Users table
    result = db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            full_name VARCHAR(100),
            created_at DATE
        )
    """)
    print(f"   Users table: {result.message}")
    
    # Posts table
    result = db.execute("""
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            title VARCHAR(200) NOT NULL,
            content TEXT,
            author_id INTEGER NOT NULL,
            published BOOLEAN DEFAULT FALSE,
            created_at DATE
        )
    """)
    print(f"   Posts table: {result.message}")
    
    # Comments table
    result = db.execute("""
        CREATE TABLE comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at DATE
        )
    """)
    print(f"   Comments table: {result.message}")
    
    # 3. Create indexes
    print("\n3. Creating indexes...")
    db.execute("CREATE INDEX idx_post_author ON posts(author_id)")
    db.execute("CREATE INDEX idx_comment_post ON comments(post_id)")
    db.execute("CREATE INDEX idx_comment_user ON comments(user_id)")
    print("   ✓ Indexes created")
    
    # 4. Insert users
    print("\n4. Inserting users...")
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
    print(f"   ✓ Inserted {len(users)} users")
    
    # 5. Insert posts
    print("\n5. Inserting posts...")
    posts = [
        (1, 'Getting Started with SimplDB', 'This is a great database!', 1, True, '2025-01-10'),
        (2, 'Building Web Apps', 'Learn to build apps with SimplDB', 1, True, '2025-01-11'),
        (3, 'Database Design Tips', 'Best practices for schemas', 2, False, '2025-01-12'),
    ]
    
    for post_id, title, content, author_id, published, created in posts:
        db.execute(f"""
            INSERT INTO posts (id, title, content, author_id, published, created_at)
            VALUES ({post_id}, '{title}', '{content}', {author_id}, {published}, '{created}')
        """)
    print(f"   ✓ Inserted {len(posts)} posts")
    
    # 6. Insert comments
    print("\n6. Inserting comments...")
    comments = [
        (1, 1, 2, 'Great post!', '2025-01-10'),
        (2, 1, 3, 'Very helpful, thanks!', '2025-01-10'),
        (3, 2, 2, 'Looking forward to more content', '2025-01-11'),
    ]
    
    for comment_id, post_id, user_id, content, created in comments:
        db.execute(f"""
            INSERT INTO comments (id, post_id, user_id, content, created_at)
            VALUES ({comment_id}, {post_id}, {user_id}, '{content}', '{created}')
        """)
    print(f"   ✓ Inserted {len(comments)} comments")
    
    # 7. Query data
    print("\n7. Querying data...")
    
    # All published posts
    result = db.execute("SELECT title, author_id FROM posts WHERE published = TRUE")
    print(f"\n   Published posts ({len(result.rows)}):")
    for row in result.rows:
        print(f"      - {row['title']} (by user {row['author_id']})")
    
    # Comments on first post
    result = db.execute("SELECT user_id, content FROM comments WHERE post_id = 1")
    print(f"\n   Comments on post 1 ({len(result.rows)}):")
    for row in result.rows:
        print(f"      - User {row['user_id']}: {row['content']}")
    
    # Users with most posts (simulated - would need GROUP BY)
    result = db.execute("SELECT * FROM posts WHERE author_id = 1")
    print(f"\n   Posts by user 1: {len(result.rows)}")
    
    # 8. Update data
    print("\n8. Updating data...")
    result = db.execute("UPDATE posts SET published = TRUE WHERE id = 3")
    print(f"   {result.message}")
    
    # 9. Delete data
    print("\n9. Deleting data...")
    result = db.execute("DELETE FROM comments WHERE id = 3")
    print(f"   {result.message}")
    
    # 10. Database statistics
    print("\n10. Database statistics:")
    
    db_info = db.get_database_info()
    print(f"   Database: {db_info['name']}")
    print(f"   Tables: {db_info['total_tables']}")
    print(f"   Total rows: {db_info['total_rows']}")
    print(f"   Indexes: {db_info['total_indexes']}")
    
    # 11. Table details
    print("\n11. Table details:")
    
    for table_name in db.list_tables():
        stats = db.get_table_stats(table_name)
        print(f"\n   {table_name}:")
        print(f"      Rows: {stats['row_count']}")
        print(f"      Indexes: {len(stats['indexes'])}")
    
    # 12. Describe a table
    print("\n12. Describing 'users' table:")
    info = db.describe_table('users')
    print(f"   Columns:")
    for col in info['columns']:
        constraints = ', '.join(col['constraints']) if col['constraints'] else 'None'
        print(f"      - {col['name']}: {col['type']} ({constraints})")
    
    # 13. Export schema
    print("\n13. Exporting schema...")
    db.export_schema("demo_database/schema_backup.json")
    print("   ✓ Schema exported to schema_backup.json")
    
    # 14. Execute multiple queries
    print("\n14. Executing batch queries...")
    batch = [
        "INSERT INTO users (id, username, email, full_name, created_at) VALUES (4, 'diana', 'diana@example.com', 'Diana Prince', '2025-01-15')",
        "INSERT INTO posts (id, title, content, author_id, published, created_at) VALUES (4, 'New Post', 'Content here', 4, FALSE, '2025-01-15')"
    ]
    
    results = db.execute_many(batch)
    print(f"   ✓ Executed {len(results)} queries")
    print(f"   ✓ All successful: {all(r.success for r in results)}")
    
    # 15. Final database state
    print("\n15. Final database state:")
    
    result = db.execute("SELECT COUNT(*) as count FROM users")
    # Note: We don't have COUNT() yet, so just count manually
    result = db.execute("SELECT * FROM users")
    print(f"   Total users: {len(result.rows)}")
    
    result = db.execute("SELECT * FROM posts")
    print(f"   Total posts: {len(result.rows)}")
    
    result = db.execute("SELECT * FROM comments")
    print(f"   Total comments: {len(result.rows)}")
    
    # 16. Close database
    print("\n16. Closing database...")
    db.close()
    print("   ✓ Database closed successfully")
    
    print("\n" + "=" * 70)
    print("✅ Demo complete!")
    print("=" * 70)
    print("\nGenerated files in 'demo_database/':")
    print("  - users.json (user data)")
    print("  - posts.json (post data)")
    print("  - comments.json (comment data)")
    print("  - *_indexes.json (index files)")
    print("  - catalog.json (database metadata)")
    print("  - schema_backup.json (schema export)")
    
    # Optional: Don't cleanup so user can inspect files
    # cleanup()


if __name__ == "__main__":
    main()