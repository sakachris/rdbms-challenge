# ruff: noqa: E501
# webapp/routes/main.py

"""Main application routes"""

from flask import flash, redirect, render_template, request, url_for

from webapp.models import Post, User


def register_main_routes(app, db):
    """Register main application routes"""

    # @app.route("/")
    # def index():
    #     """Home page - list all published posts"""
    #     posts = Post.get_all_published(db)
    #     return render_template("index.html", posts=posts)

    @app.route("/")
    @app.route("/page/<int:page>")
    def index(page=1):
        """Home page - list all published posts with pagination"""
        per_page = 5  # Posts per page

        # Get all published posts
        result = db.execute(
            "SELECT * FROM posts WHERE published = TRUE ORDER BY created_at DESC"
        )

        all_posts = []
        if result.success and result.rows:
            for post in result.rows:
                # Get author info
                author_result = db.execute(
                    f"SELECT username, full_name FROM users WHERE id = {post['author_id']}"
                )
                author = (
                    author_result.rows[0]
                    if author_result.rows
                    else {"username": "Unknown", "full_name": "Unknown"}
                )

                # Get comment count
                comment_result = db.execute(
                    f"SELECT * FROM comments WHERE post_id = {post['id']}"
                )
                comment_count = (
                    len(comment_result.rows) if comment_result.rows else 0
                )

                all_posts.append(
                    {
                        "id": post["id"],
                        "title": post["title"],
                        "content": (
                            post["content"][:200] + "..."
                            if len(post["content"]) > 200
                            else post["content"]
                        ),
                        "author_username": author["username"],
                        "author_name": author["full_name"],
                        "created_at": post["created_at"],
                        "comment_count": comment_count,
                    }
                )

        # Calculate pagination
        total_posts = len(all_posts)
        total_pages = (
            total_posts + per_page - 1
        ) // per_page  # Ceiling division

        # Ensure page is within valid range
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages

        # Get posts for current page
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        posts = all_posts[start_idx:end_idx]

        # Create pagination info
        pagination = {
            "page": page,
            "per_page": per_page,
            "total_posts": total_posts,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": page < total_pages,
            "prev_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if page < total_pages else None,
            "pages": list(range(1, total_pages + 1)),
        }

        return render_template(
            "index.html", posts=posts, pagination=pagination
        )

    @app.route("/search")
    def search():
        """Search posts"""
        query = request.args.get("q", "").strip()
        if not query:
            return redirect(url_for("index"))

        posts = Post.search(db, query)
        return render_template("search.html", posts=posts, query=query)

    @app.route("/author/<username>")
    def author_posts(username):
        """View all posts by an author"""
        user = User.get_by_username(db, username)
        if not user:
            flash("Author not found", "error")
            return redirect(url_for("index"))

        posts = Post.get_by_author(db, user["id"])
        return render_template("author.html", author=user, posts=posts)

    @app.route("/stats")
    def stats():
        """Database statistics"""
        db_info = db.get_database_info()

        table_stats = []
        for table_name in db.list_tables():
            stats = db.get_table_stats(table_name)
            table_stats.append(stats)

        return render_template(
            "stats.html", db_info=db_info, table_stats=table_stats
        )

    # @app.route("/health")
    # def health():
    #     return {"status": "ok"}, 200

    # Error handlers
    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404

    @app.errorhandler(500)
    def server_error(e):
        return render_template("500.html"), 500
