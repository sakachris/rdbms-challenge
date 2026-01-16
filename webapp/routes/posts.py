"""Post-related routes"""
from flask import render_template, request, redirect, url_for, flash
from models import Post, User, Comment


def register_post_routes(app, db):
    """Register post-related routes"""
    
    @app.route('/post/<int:post_id>')
    def view_post(post_id):
        """View a single post with comments"""
        post = Post.get_by_id(db, post_id)
        if not post:
            flash('Post not found', 'error')
            return redirect(url_for('index'))
        
        author = User.get_by_id(db, post['author_id'])
        comments = Comment.get_by_post(db, post_id)
        
        return render_template('post.html', post=post, author=author, comments=comments)
    
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
            
            post_id = Post.create(db, title, content, author_id, published)
            
            if post_id:
                flash('Post created successfully!', 'success')
                return redirect(url_for('view_post', post_id=post_id))
            else:
                flash('Error creating post', 'error')
        
        users = User.get_all(db)
        return render_template('create.html', users=users)
    
    @app.route('/edit/<int:post_id>', methods=['GET', 'POST'])
    def edit_post(post_id):
        """Edit an existing post"""
        post = Post.get_by_id(db, post_id)
        if not post:
            flash('Post not found', 'error')
            return redirect(url_for('index'))
        
        if request.method == 'POST':
            title = request.form.get('title', '').strip()
            content = request.form.get('content', '').strip()
            published = request.form.get('published') == 'on'
            
            if not title or not content:
                flash('Title and content are required', 'error')
                return redirect(url_for('edit_post', post_id=post_id))
            
            if Post.update(db, post_id, title, content, published):
                flash('Post updated successfully!', 'success')
                return redirect(url_for('view_post', post_id=post_id))
            else:
                flash('Error updating post', 'error')
        
        return render_template('edit.html', post=post)
    
    @app.route('/delete/<int:post_id>', methods=['POST'])
    def delete_post(post_id):
        """Delete a post"""
        if Post.delete(db, post_id):
            flash('Post deleted successfully!', 'success')
        else:
            flash('Error deleting post', 'error')
        
        return redirect(url_for('index'))