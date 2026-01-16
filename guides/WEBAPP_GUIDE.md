# SimplDB Blog - Web Application Guide

## 🚀 Quick Start

### 1. Setup Project Structure

```bash
cd simpldb-project

# Create webapp directory structure
mkdir -p webapp/templates
mkdir -p webapp/static
mkdir -p webapp/data

# The app.py file goes in the webapp directory
```

### 2. Create Template Files

Create the following files in `webapp/templates/`:

1. `base.html` - Base template (from artifact 2)
2. `index.html` - Home page (from artifact 3)
3. Create a single file with all remaining templates (from artifact 4):
   - `post.html`
   - `create.html`
   - `edit.html`
   - `search.html`
   - `author.html`
   - `stats.html`
   - `404.html`
   - `500.html`

Split the content from artifact 4 into separate files.

### 3. Install Dependencies

Your `requirements.txt` already has Flask and tabulate. Verify:

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Run the Application

```bash
# From project root
cd webapp
python app.py
```

You should see:

```
======================================================================
SimplDB Blog Application
======================================================================

Starting server at http://127.0.0.1:5000

Available routes:
  /              - Home page (list posts)
  /post/<id>     - View single post
  /create        - Create new post
  /edit/<id>     - Edit post
  /search?q=...  - Search posts
  /author/<n>    - View author's posts
  /stats         - Database statistics
  /api/posts     - JSON API endpoint

Press Ctrl+C to stop
======================================================================

✓ Database schema created
✓ Sample data inserted
 * Serving Flask app 'app'
 * Debug mode: on
 * Running on http://127.0.0.1:5000
```

### 5. Access the Application

Open your browser and navigate to:

- **Home:** http://127.0.0.1:5000
- **Stats:** http://127.0.0.1:5000/stats
- **Create Post:** http://127.0.0.1:5000/create

## 📋 Features

### 1. **View Posts**

- Home page lists all published posts
- Click on post title to view full content
- See author, date, and comment count

### 2. **Create Posts**

- Navigate to "New Post" in navbar
- Fill in title, select author, write content
- Choose to publish immediately or save as draft

### 3. **Edit Posts**

- Click "Edit" button on any post
- Update title, content, or publication status
- Changes tracked with updated timestamp

### 4. **Delete Posts**

- Click "Delete" button with confirmation
- Automatically removes associated comments

### 5. **Search**

- Use search box in navbar
- Searches in both title and content
- Case-insensitive matching

### 6. **Author Pages**

- Click on author name to see all their posts
- View author profile information
- See publication history

### 7. **Database Statistics**

- View total tables, rows, and indexes
- See detailed table information
- Monitor database performance

### 8. **REST API**

- GET `/api/posts` - Returns all posts as JSON
- Useful for building mobile apps or integrations

## 🏗️ Architecture

### Database Schema

**Users Table:**

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at DATE
)
```

**Posts Table:**

```sql
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    published BOOLEAN DEFAULT FALSE,
    created_at DATE,
    updated_at DATE
)
```

**Comments Table:**

```sql
CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at DATE
)
```

### Indexes

```sql
CREATE INDEX idx_post_author ON posts(author_id);
CREATE INDEX idx_comment_post ON comments(post_id);
CREATE INDEX idx_comment_user ON comments(user_id);
```

## 🎨 UI Components

### Bootstrap 5

- Responsive grid system
- Card components for posts
- Forms with validation
- Navigation and alerts

### Bootstrap Icons

- Used throughout for visual enhancement
- Database, person, calendar, etc.

### Custom Styling

- Gradient badge for SimplDB branding
- Card shadows for depth
- Responsive design for mobile

## 📊 Sample Data

The application automatically creates:

**3 Users:**

- Alice Smith (@alice)
- Bob Johnson (@bob)
- Charlie Brown (@charlie)

**4 Posts:**

- "Welcome to SimplDB Blog"
- "Building Your Own Database"
- "Getting Started with SQL"
- "Draft Post" (unpublished)

**3 Comments:**

- Various comments on posts

## 🔧 Customization

### Change Database Location

```python
# In app.py, modify:
db = Database(name="blogdb", data_dir="path/to/data")
```

### Add New Routes

```python
@app.route('/your-route')
def your_function():
    # Your logic here
    return render_template('your_template.html')
```

### Modify Styling

Edit the `<style>` section in `base.html` or add external CSS file.

### Add Features

Common additions:

- User authentication
- Comment posting form
- Image uploads
- Tags/categories
- Pagination

## 🐛 Troubleshooting

### Issue: Database already exists error

**Solution:** Delete the data directory:

```bash
rm -rf webapp/data
python app.py  # Will recreate fresh
```

### Issue: Template not found

**Solution:** Verify structure:

```bash
webapp/
├── app.py
└── templates/
    ├── base.html
    ├── index.html
    └── ... (other templates)
```

### Issue: Port 5000 already in use

**Solution:** Change port in app.py:

```python
app.run(debug=True, host='0.0.0.0', port=5001)
```

### Issue: Module not found

**Solution:**

```bash
# Make sure you're in the right directory
cd simpldb-project
source venv/bin/activate
pip install -r requirements.txt
```

## 📸 Screenshots

Expected views:

1. **Home Page** - List of blog posts with sidebar
2. **Post View** - Full post with comments
3. **Create/Edit** - Form with fields
4. **Search Results** - Filtered posts
5. **Statistics** - Database metrics

## 🚀 Production Deployment

For production use:

1. **Change secret key:**

```python
app.secret_key = os.environ.get('SECRET_KEY', 'fallback-secret')
```

2. **Disable debug mode:**

```python
app.run(debug=False)
```

3. **Use production server:**

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

4. **Add security headers**
5. **Setup HTTPS**
6. **Configure proper database backup**

## 📝 API Usage

### Get All Posts

```bash
curl http://127.0.0.1:5000/api/posts
```

Response:

```json
[
  {
    "id": 1,
    "title": "Welcome to SimplDB Blog",
    "content": "This is our first blog post...",
    "author": "alice",
    "created_at": "2025-01-10"
  }
]
```

## 🎯 Next Steps

1. Add user authentication
2. Implement comment posting
3. Add post categories/tags
4. Create admin dashboard
5. Add image upload functionality
6. Implement pagination
7. Add RSS feed
8. Create email notifications

## 🏆 Achievement Unlocked!

You've successfully built a complete web application using your custom RDBMS! 🎉

The application demonstrates:

- ✅ Full CRUD operations
- ✅ Database relationships
- ✅ Indexing for performance
- ✅ Real-world SQL usage
- ✅ Modern web development
- ✅ RESTful API

Congratulations on completing the SimplDB project!
