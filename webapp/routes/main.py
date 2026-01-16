"""Main application routes"""
from flask import render_template, request, redirect, url_for, flash
from models import Post, User


def register_main_routes(app, db):
    """Register main application routes"""
    
    @app.route('/')
    def index():
        """Home page - list all published posts"""
        posts = Post.get_all_published(db)
        return render_template('index.html', posts=posts)
    
    @app.route('/search')
    def search():
        """Search posts"""
        query = request.args.get('q', '').strip()
        if not query:
            return redirect(url_for('index'))
        
        posts = Post.search(db, query)
        return render_template('search.html', posts=posts, query=query)
    
    @app.route('/author/<username>')
    def author_posts(username):
        """View all posts by an author"""
        user = User.get_by_username(db, username)
        if not user:
            flash('Author not found', 'error')
            return redirect(url_for('index'))
        
        posts = Post.get_by_author(db, user['id'])
        return render_template('author.html', author=user, posts=posts)
    
    @app.route('/stats')
    def stats():
        """Database statistics"""
        db_info = db.get_database_info()
        
        table_stats = []
        for table_name in db.list_tables():
            stats = db.get_table_stats(table_name)
            table_stats.append(stats)
        
        return render_template('stats.html', db_info=db_info, table_stats=table_stats)
    
    # @app.route("/health")
    # def health():
    #     return {"status": "ok"}, 200
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(e):
        return render_template('404.html'), 404
    
    @app.errorhandler(500)
    def server_error(e):
        return render_template('500.html'), 500