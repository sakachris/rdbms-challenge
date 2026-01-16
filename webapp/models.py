"""Database models and query functions for SimplDB Blog"""
from datetime import datetime


class Post:
    """Post model with database operations"""
    
    @staticmethod
    def get_all_published(db):
        """Get all published posts"""
        result = db.execute("SELECT * FROM posts WHERE published = TRUE ORDER BY created_at DESC")
        if not result.success or not result.rows:
            return []
        
        posts = []
        for post in result.rows:
            author = User.get_by_id(db, post['author_id'])
            comment_count = Comment.count_by_post(db, post['id'])
            
            posts.append({
                'id': post['id'],
                'title': post['title'],
                'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                'author_username': author.get('username', 'Unknown'),
                'author_name': author.get('full_name', 'Unknown'),
                'created_at': post['created_at'],
                'comment_count': comment_count
            })
        return posts
    
    @staticmethod
    def get_by_id(db, post_id):
        """Get post by ID"""
        result = db.execute(f"SELECT * FROM posts WHERE id = {post_id}")
        if result.success and result.rows:
            return result.rows[0]
        return None
    
    @staticmethod
    def get_by_author(db, author_id):
        """Get all published posts by author"""
        result = db.execute(f"SELECT * FROM posts WHERE author_id = {author_id} AND published = TRUE ORDER BY created_at DESC")
        if not result.success or not result.rows:
            return []
        
        posts = []
        for post in result.rows:
            posts.append({
                'id': post['id'],
                'title': post['title'],
                'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                'created_at': post['created_at']
            })
        return posts
    
    @staticmethod
    def create(db, title, content, author_id, published=False):
        """Create a new post"""
        # Get next ID
        result = db.execute("SELECT * FROM posts")
        next_id = max([p['id'] for p in result.rows], default=0) + 1 if result.rows else 1
        
        today = datetime.now().date().isoformat()
        
        # Escape single quotes
        title = title.replace("'", "''")
        content = content.replace("'", "''")
        
        result = db.execute(f"""
            INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
            VALUES ({next_id}, '{title}', '{content}', {author_id}, {published}, '{today}', '{today}')
        """)
        
        if result.success:
            return next_id
        return None
    
    @staticmethod
    def update(db, post_id, title, content, published):
        """Update an existing post"""
        today = datetime.now().date().isoformat()
        
        # Escape single quotes
        title = title.replace("'", "''")
        content = content.replace("'", "''")
        
        result = db.execute(f"""
            UPDATE posts 
            SET title = '{title}', content = '{content}', published = {published}, updated_at = '{today}'
            WHERE id = {post_id}
        """)
        
        return result.success
    
    @staticmethod
    def delete(db, post_id):
        """Delete a post"""
        # Delete comments first
        Comment.delete_by_post(db, post_id)
        
        # Delete post
        result = db.execute(f"DELETE FROM posts WHERE id = {post_id}")
        return result.success
    
    @staticmethod
    def search(db, query):
        """Search posts by title or content"""
        result = db.execute("SELECT * FROM posts WHERE published = TRUE")
        if not result.rows:
            return []
        
        posts = []
        for post in result.rows:
            if query.lower() in post['title'].lower() or query.lower() in post['content'].lower():
                author = User.get_by_id(db, post['author_id'])
                posts.append({
                    'id': post['id'],
                    'title': post['title'],
                    'content': post['content'][:200] + '...' if len(post['content']) > 200 else post['content'],
                    'author_username': author.get('username', 'Unknown'),
                    'author_name': author.get('full_name', 'Unknown'),
                    'created_at': post['created_at']
                })
        return posts


class User:
    """User model with database operations"""
    
    @staticmethod
    def get_all(db):
        """Get all users"""
        result = db.execute("SELECT id, username, full_name FROM users")
        return result.rows if result.rows else []
    
    @staticmethod
    def get_by_id(db, user_id):
        """Get user by ID"""
        result = db.execute(f"SELECT username, full_name FROM users WHERE id = {user_id}")
        if result.rows:
            return result.rows[0]
        return {'username': 'Unknown', 'full_name': 'Unknown'}
    
    @staticmethod
    def get_by_username(db, username):
        """Get user by username"""
        result = db.execute(f"SELECT * FROM users WHERE username = '{username}'")
        if result.rows:
            return result.rows[0]
        return None


class Comment:
    """Comment model with database operations"""
    
    @staticmethod
    def get_by_post(db, post_id):
        """Get all comments for a post with user info"""
        result = db.execute(f"SELECT * FROM comments WHERE post_id = {post_id} ORDER BY created_at DESC")
        if not result.rows:
            return []
        
        comments = []
        for comment in result.rows:
            user = User.get_by_id(db, comment['user_id'])
            comments.append({
                'id': comment['id'],
                'content': comment['content'],
                'username': user.get('username', 'Unknown'),
                'full_name': user.get('full_name', 'Unknown'),
                'created_at': comment['created_at']
            })
        return comments
    
    @staticmethod
    def count_by_post(db, post_id):
        """Count comments for a post"""
        result = db.execute(f"SELECT * FROM comments WHERE post_id = {post_id}")
        return len(result.rows) if result.rows else 0
    
    @staticmethod
    def delete_by_post(db, post_id):
        """Delete all comments for a post"""
        db.execute(f"DELETE FROM comments WHERE post_id = {post_id}")
