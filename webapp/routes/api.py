"""API routes"""
from flask import jsonify
from models import Post, User


def register_api_routes(app, db):
    """Register API routes"""
    
    @app.route('/api/posts')
    def api_posts():
        """API endpoint - get all posts as JSON"""
        result = db.execute("SELECT * FROM posts WHERE published = TRUE")
        posts = []
        
        if result.rows:
            for post in result.rows:
                author = User.get_by_id(db, post['author_id'])
                posts.append({
                    'id': post['id'],
                    'title': post['title'],
                    'content': post['content'],
                    'author': author.get('username', 'Unknown'),
                    'created_at': post['created_at']
                })
        
        return jsonify(posts)