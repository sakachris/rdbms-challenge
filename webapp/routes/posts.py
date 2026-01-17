"""Post-related routes"""

from flask import flash, redirect, render_template, request, url_for

from webapp.models import Comment, Post, User


def register_post_routes(app, db):
    """Register post-related routes"""

    @app.route("/post/<int:post_id>")
    def view_post(post_id):
        """View a single post with comments"""
        post = Post.get_by_id(db, post_id)
        if not post:
            flash("Post not found", "error")
            return redirect(url_for("index"))

        author = User.get_by_id(db, post["author_id"])
        comments = Comment.get_by_post(db, post_id)

        return render_template(
            "post.html", post=post, author=author, comments=comments
        )

    @app.route("/create", methods=["GET", "POST"])
    def create_post():
        """Create a new post"""
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            content = request.form.get("content", "").strip()
            author_id = int(request.form.get("author_id", 1))
            published = request.form.get("published") == "on"

            if not title or not content:
                flash("Title and content are required", "error")
                return redirect(url_for("create_post"))

            post_id = Post.create(db, title, content, author_id, published)

            if post_id:
                flash("Post created successfully!", "success")
                return redirect(url_for("view_post", post_id=post_id))
            else:
                flash("Error creating post", "error")

        users = User.get_all(db)
        return render_template("create.html", users=users)

    @app.route("/edit/<int:post_id>", methods=["GET", "POST"])
    def edit_post(post_id):
        """Edit an existing post"""
        post = Post.get_by_id(db, post_id)
        if not post:
            flash("Post not found", "error")
            return redirect(url_for("index"))

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            content = request.form.get("content", "").strip()
            published = request.form.get("published") == "on"

            if not title or not content:
                flash("Title and content are required", "error")
                return redirect(url_for("edit_post", post_id=post_id))

            if Post.update(db, post_id, title, content, published):
                flash("Post updated successfully!", "success")
                return redirect(url_for("view_post", post_id=post_id))
            else:
                flash("Error updating post", "error")

        return render_template("edit.html", post=post)

    # @app.route("/edit/<int:post_id>", methods=["GET", "POST"])
    # def edit_post(post_id):
    #     """Edit an existing post"""
    #     # Get post
    #     result = db.execute(f"SELECT * FROM posts WHERE id = {post_id}")

    #     if not result.success or not result.rows:
    #         flash("Post not found", "error")
    #         return redirect(url_for("index"))

    #     post = result.rows[0]

    #     if request.method == "POST":
    #         title = request.form.get("title", "").strip()
    #         content = request.form.get("content", "").strip()
    #         published = request.form.get("published") == "on"

    #         if not title or not content:
    #             flash("Title and content are required", "error")
    #             return redirect(url_for("edit_post", post_id=post_id))

    #         today = datetime.now().date().isoformat()

    #         # Use direct API instead of SQL to avoid parser issues
    #         try:
    #             # Get the storage table
    #             posts_table = db.storage.get_table("posts")

    #             # Build update data
    #             update_data = {
    #                 "title": title,
    #                 "content": content,
    #                 "published": published,
    #                 "updated_at": today,
    #             }

    #             # Validate with schema
    #             schema = db.schema_manager.get_schema("posts")
    #             full_data = {**post, **update_data}
    #             is_valid, errors = schema.validate_row(full_data)

    #             if not is_valid:
    #                 flash(f"Validation error: {', '.join(errors)}", "error")
    #                 return redirect(url_for("edit_post", post_id=post_id))

    #             # Convert types
    #             converted_update = schema.convert_row(update_data)

    #             # Get old data for index update
    #             old_row = posts_table.select_by_id(post_id)
    #             old_data = old_row.data.copy()

    #             # Update storage
    #             posts_table.update_by_id(post_id, converted_update)

    #             # Update indexes
    #             new_data = {**old_data, **converted_update}
    #             success, error = db.index_manager.update_indexes(
    #                 "posts", post_id, old_data, new_data
    #             )

    #             if not success:
    #                 # Rollback storage update
    #                 posts_table.update_by_id(post_id, old_data)
    #                 flash(f"Index error: {error}", "error")
    #                 return redirect(url_for("edit_post", post_id=post_id))

    #             flash("Post updated successfully!", "success")
    #             return redirect(url_for("view_post", post_id=post_id))

    #         except Exception as e:
    #             flash(f"Error updating post: {str(e)}", "error")
    #             return redirect(url_for("edit_post", post_id=post_id))

    #     return render_template("edit.html", post=post)

    @app.route("/delete/<int:post_id>", methods=["POST"])
    def delete_post(post_id):
        """Delete a post"""
        if Post.delete(db, post_id):
            flash("Post deleted successfully!", "success")
        else:
            flash("Error deleting post", "error")

        return redirect(url_for("index"))
