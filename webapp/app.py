"""
SimplDB Blog - Flask Web Application
A simple blog application using SimplDB as the backend
"""

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime
import os
import sys

# Add parent directory to path to import simpldb
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb import Database

app = Flask(__name__)
app.secret_key = 'simpldb-secret-key-change-in-production'

# Initialize database
db = Database(name="blogdb", data_dir="webapp/data")


def init_database():
    """Initialize database schema"""
    # Check if tables exist
    if 'users' not in db.list_tables():
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
        
        print("✓ Database schema created")
        
        # Insert sample data
        insert_sample_data()
    else:
        print("✓ Database already initialized")


def insert_sample_data():
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


# Routes

@app.route('/')
def index():
    """Home page - list all published posts"""
    result = db.execute("SELECT * FROM posts WHERE published = TRUE ORDER BY created_at DESC")
    
    posts = []
    if result.success and result.rows:
        for post in result.rows:
            # Get author info
            author_result = db.execute(f"SELECT username, full_name FROM users WHERE id = {post['author_id']}")
            author = author_result.rows[0] if author_result.rows else {'username': 'Unknown', 'full_name': 'Unknown'}
            
            # Get comment count
            comment_result = db.execute(f"SELECT * FROM comments WHERE post_id = {post['id']}")
            comment_count = len(comment_result.rows) if comment_result.rows else 0
            
            posts.append({
                'id': post['id'],
                'title': post['title'],
                'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                'author_username': author['username'],
                'author_name': author['full_name'],
                'created_at': post['created_at'],
                'comment_count': comment_count
            })
    
    return render_template('index.html', posts=posts)


@app.route('/post/<int:post_id>')
def view_post(post_id):
    """View a single post with comments"""
    # Get post
    result = db.execute(f"SELECT * FROM posts WHERE id = {post_id}")
    
    if not result.success or not result.rows:
        flash('Post not found', 'error')
        return redirect(url_for('index'))
    
    post = result.rows[0]
    
    # Get author
    author_result = db.execute(f"SELECT username, full_name FROM users WHERE id = {post['author_id']}")
    author = author_result.rows[0] if author_result.rows else {'username': 'Unknown', 'full_name': 'Unknown'}
    
    # Get comments with user info
    comments_result = db.execute(f"SELECT * FROM comments WHERE post_id = {post_id} ORDER BY created_at DESC")
    
    comments = []
    if comments_result.rows:
        for comment in comments_result.rows:
            user_result = db.execute(f"SELECT username, full_name FROM users WHERE id = {comment['user_id']}")
            user = user_result.rows[0] if user_result.rows else {'username': 'Unknown', 'full_name': 'Unknown'}
            
            comments.append({
                'id': comment['id'],
                'content': comment['content'],
                'username': user['username'],
                'full_name': user['full_name'],
                'created_at': comment['created_at']
            })
    
    return render_template('post.html', 
                         post=post, 
                         author=author, 
                         comments=comments)


@app.route('/create', methods=['GET', 'POST'])
def create_post():
    """Create a new post"""
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        content = request.form.get('content', '').strip()
        author_id = int(request.form.get('author_id', 1))
        published = request.form.get('published') == 'on'
        
        if not title or not content:
            flash('Title and content are required', 'error')
            return redirect(url_for('create_post'))
        
        # Get next ID
        result = db.execute("SELECT * FROM posts")
        next_id = max([p['id'] for p in result.rows], default=0) + 1
        
        today = datetime.now().date().isoformat()
        
        # Escape single quotes
        title = title.replace("'", "''")
        content = content.replace("'", "''")
        
        result = db.execute(f"""
            INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
            VALUES ({next_id}, '{title}', '{content}', {author_id}, {published}, '{today}', '{today}')
        """)
        
        if result.success:
            flash('Post created successfully!', 'success')
            return redirect(url_for('view_post', post_id=next_id))
        else:
            flash(f'Error creating post: {result.message}', 'error')
    
    # Get users for author selection
    users_result = db.execute("SELECT id, username, full_name FROM users")
    users = users_result.rows if users_result.rows else []
    
    return render_template('create.html', users=users)


@app.route('/edit/<int:post_id>', methods=['GET', 'POST'])
def edit_post(post_id):
    """Edit an existing post"""
    # Get post
    result = db.execute(f"SELECT * FROM posts WHERE id = {post_id}")
    
    if not result.success or not result.rows:
        flash('Post not found', 'error')
        return redirect(url_for('index'))
    
    post = result.rows[0]
    
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        content = request.form.get('content', '').strip()
        published = request.form.get('published') == 'on'
        
        if not title or not content:
            flash('Title and content are required', 'error')
            return redirect(url_for('edit_post', post_id=post_id))
        
        today = datetime.now().date().isoformat()
        
        # Escape single quotes
        title = title.replace("'", "''")
        content = content.replace("'", "''")
        
        result = db.execute(f"""
            UPDATE posts 
            SET title = '{title}', 
                content = '{content}', 
                published = {published},
                updated_at = '{today}'
            WHERE id = {post_id}
        """)
        
        if result.success:
            flash('Post updated successfully!', 'success')
            return redirect(url_for('view_post', post_id=post_id))
        else:
            flash(f'Error updating post: {result.message}', 'error')
    
    return render_template('edit.html', post=post)


@app.route('/delete/<int:post_id>', methods=['POST'])
def delete_post(post_id):
    """Delete a post"""
    # Delete comments first
    db.execute(f"DELETE FROM comments WHERE post_id = {post_id}")
    
    # Delete post
    result = db.execute(f"DELETE FROM posts WHERE id = {post_id}")
    
    if result.success:
        flash('Post deleted successfully!', 'success')
    else:
        flash(f'Error deleting post: {result.message}', 'error')
    
    return redirect(url_for('index'))


@app.route('/search')
def search():
    """Search posts"""
    query = request.args.get('q', '').strip()
    
    if not query:
        return redirect(url_for('index'))
    
    # Search in title and content (simple LIKE simulation)
    result = db.execute("SELECT * FROM posts WHERE published = TRUE")
    
    posts = []
    if result.rows:
        for post in result.rows:
            # Simple search - check if query is in title or content
            if query.lower() in post['title'].lower() or query.lower() in post['content'].lower():
                # Get author
                author_result = db.execute(f"SELECT username, full_name FROM users WHERE id = {post['author_id']}")
                author = author_result.rows[0] if author_result.rows else {'username': 'Unknown', 'full_name': 'Unknown'}
                
                posts.append({
                    'id': post['id'],
                    'title': post['title'],
                    'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                    'author_username': author['username'],
                    'author_name': author['full_name'],
                    'created_at': post['created_at']
                })
    
    return render_template('search.html', posts=posts, query=query)


@app.route('/author/<username>')
def author_posts(username):
    """View all posts by an author"""
    # Get user
    user_result = db.execute(f"SELECT * FROM users WHERE username = '{username}'")
    
    if not user_result.rows:
        flash('Author not found', 'error')
        return redirect(url_for('index'))
    
    user = user_result.rows[0]
    
    # Get posts
    result = db.execute(f"SELECT * FROM posts WHERE author_id = {user['id']} AND published = TRUE ORDER BY created_at DESC")
    
    posts = []
    if result.rows:
        for post in result.rows:
            posts.append({
                'id': post['id'],
                'title': post['title'],
                'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                'created_at': post['created_at']
            })
    
    return render_template('author.html', author=user, posts=posts)


@app.route('/stats')
def stats():
    """Database statistics"""
    db_info = db.get_database_info()
    
    # Get detailed stats
    table_stats = []
    for table_name in db.list_tables():
        stats = db.get_table_stats(table_name)
        table_stats.append(stats)
    
    return render_template('stats.html', db_info=db_info, table_stats=table_stats)


@app.route('/api/posts')
def api_posts():
    """API endpoint - get all posts as JSON"""
    result = db.execute("SELECT * FROM posts WHERE published = TRUE")
    
    posts = []
    if result.rows:
        for post in result.rows:
            author_result = db.execute(f"SELECT username FROM users WHERE id = {post['author_id']}")
            author = author_result.rows[0]['username'] if author_result.rows else 'Unknown'
            
            posts.append({
                'id': post['id'],
                'title': post['title'],
                'content': post['content'],
                'author': author,
                'created_at': post['created_at']
            })
    
    return jsonify(posts)


# Error handlers

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def server_error(e):
    return render_template('500.html'), 500


if __name__ == '__main__':
    # Initialize database
    init_database()
    
    # Run app
    print("\n" + "=" * 70)
    print("SimplDB Blog Application")
    print("=" * 70)
    print("\nStarting server at http://127.0.0.1:5000")
    print("\nAvailable routes:")
    print("  /              - Home page (list posts)")
    print("  /post/<id>     - View single post")
    print("  /create        - Create new post")
    print("  /edit/<id>     - Edit post")
    print("  /search?q=...  - Search posts")
    print("  /author/<name> - View author's posts")
    print("  /stats         - Database statistics")
    print("  /api/posts     - JSON API endpoint")
    print("\nPress Ctrl+C to stop")
    print("=" * 70 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)