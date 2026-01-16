# SimplDB REPL - Interactive Shell Guide

## 🚀 Quick Start

### Starting the REPL

```bash
# From project root
cd simpldb-project
source venv/bin/activate

# Start interactive shell
python -m simpldb.repl

# Or with custom database
python -m simpldb.repl --name mydb --data-dir mydata
```

### Execute Single Command

```bash
# Execute SQL without entering interactive mode
python -m simpldb.repl --execute "SELECT * FROM users;"

# Or use -e shorthand
python -m simpldb.repl -e "SELECT * FROM users;"
```

## 📋 Features

### 1. **SQL Execution**

Write SQL queries ending with semicolon:

```sql
simpldb> CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));
Table 'users' created successfully
Query executed in 0.003s

simpldb> INSERT INTO users (id, name) VALUES (1, 'Alice');
1 row inserted (ID: 1)
1 row(s) affected in 0.002s

simpldb> SELECT * FROM users;

+------+-------+
|   id | name  |
|------+-------|
|    1 | Alice |
+------+-------+

1 row(s) returned in 0.001s
```

### 2. **Multi-line Queries**

Press Enter without semicolon to continue on next line:

```sql
simpldb> CREATE TABLE posts (
      ->   id INTEGER PRIMARY KEY,
      ->   title VARCHAR(200),
      ->   content TEXT
      -> );
Table 'posts' created successfully
```

### 3. **Meta Commands**

Commands starting with `.` provide database operations:

```bash
simpldb> .tables
Tables in database (2):
  1. posts (0 rows)
  2. users (1 rows)

simpldb> .describe users
Table: users
============================================================

Created: 2025-01-16T10:30:00
Last Modified: 2025-01-16T10:31:00
Row Count: 1

Columns (2):
+--------+--------------+-------------+---------+
| Name   | Type         | Constraints | Default |
|--------+--------------+-------------+---------|
| id     | INTEGER      | PRIMARY_KEY | -       |
| name   | VARCHAR(100) | -           | -       |
+--------+--------------+-------------+---------+
```

### 4. **Tab Completion**

Press Tab to auto-complete:

- SQL keywords (SELECT, FROM, WHERE...)
- Table names
- Meta commands (.help, .tables...)

```sql
simpldb> SEL<TAB>     → SELECT
simpldb> .ta<TAB>     → .tables
```

### 5. **Command History**

- **Up/Down arrows**: Navigate through previous commands
- **Ctrl+R**: Reverse search history
- History saved to `~/.simpldb_history`

### 6. **Pretty-Printed Output**

Results displayed in clean, formatted tables using `psql` style:

```sql
simpldb> SELECT id, username, age FROM users;

+------+----------+-------+
|   id | username |   age |
|------+----------+-------|
|    1 | alice    |    30 |
|    2 | bob      |    25 |
|    3 | charlie  |    35 |
+------+----------+-------+

3 row(s) returned in 0.002s
```

## 🎯 Meta Commands Reference

### Information Commands

| Command             | Description                        | Example           |
| ------------------- | ---------------------------------- | ----------------- |
| `.help`             | Show help message                  | `.help`           |
| `.tables`           | List all tables                    | `.tables`         |
| `.schema [table]`   | Show schema for all/specific table | `.schema users`   |
| `.describe <table>` | Detailed table info                | `.describe users` |
| `.stats [table]`    | Show statistics                    | `.stats users`    |
| `.dbinfo`           | Database information               | `.dbinfo`         |

### Management Commands

| Command            | Description             | Example               |
| ------------------ | ----------------------- | --------------------- |
| `.export <file>`   | Export schema to file   | `.export backup.json` |
| `.import <file>`   | Import schema from file | `.import backup.json` |
| `.clear`           | Clear screen            | `.clear`              |
| `.exit` or `.quit` | Exit REPL               | `.exit`               |

## 💡 Usage Examples

### Example 1: Create Blog Database

```sql
simpldb> CREATE TABLE users (
      ->   id INTEGER PRIMARY KEY,
      ->   username VARCHAR(50) UNIQUE NOT NULL,
      ->   email VARCHAR(100) UNIQUE
      -> );
Table 'users' created successfully

simpldb> CREATE TABLE posts (
      ->   id INTEGER PRIMARY KEY,
      ->   title VARCHAR(200) NOT NULL,
      ->   content TEXT,
      ->   author_id INTEGER NOT NULL,
      ->   created_at DATE
      -> );
Table 'posts' created successfully

simpldb> CREATE INDEX idx_author ON posts(author_id);
Index 'idx_author' created successfully

simpldb> .tables
Tables in database (2):
  1. posts (0 rows)
  2. users (0 rows)
```

### Example 2: CRUD Operations

```sql
-- Insert
simpldb> INSERT INTO users (id, username, email)
      -> VALUES (1, 'alice', 'alice@example.com');
1 row inserted (ID: 1)

-- Read
simpldb> SELECT * FROM users WHERE username = 'alice';
+------+----------+-------------------+
|   id | username | email             |
|------+----------+-------------------|
|    1 | alice    | alice@example.com |
+------+----------+-------------------+

-- Update
simpldb> UPDATE users SET email = 'alice.new@example.com' WHERE id = 1;
1 rows updated

-- Delete
simpldb> DELETE FROM users WHERE id = 1;
1 rows deleted
```

### Example 3: Complex Queries

```sql
-- Filtering and sorting
simpldb> SELECT username, age FROM users
      -> WHERE age >= 25
      -> ORDER BY age DESC
      -> LIMIT 5;

-- Joins (if implemented)
simpldb> SELECT u.username, p.title
      -> FROM users u
      -> INNER JOIN posts p ON u.id = p.author_id;
```

### Example 4: Schema Management

```sql
-- Export schema
simpldb> .export schema_backup.json
Schema exported to: schema_backup.json

-- View table details
simpldb> .describe posts
Table: posts
============================================================
Created: 2025-01-16T10:30:00
Row Count: 3

Columns (5):
+------------+--------------+-------------+---------+
| Name       | Type         | Constraints | Default |
|------------+--------------+-------------+---------|
| id         | INTEGER      | PRIMARY_KEY | -       |
| title      | VARCHAR(200) | NOT_NULL    | -       |
| content    | TEXT         | -           | -       |
| author_id  | INTEGER      | NOT_NULL    | -       |
| created_at | DATE         | -           | -       |
+------------+--------------+-------------+---------+

Indexes (2):
+--------------+-----------+--------+
| Name         | Column    | Unique |
|--------------+-----------+--------|
| idx_author   | author_id | No     |
| PRIMARY      | id        | Yes    |
+--------------+-----------+--------+

-- View statistics
simpldb> .stats posts
Statistics for table: posts
============================================================
Row Count: 3
Created: 2025-01-16T10:30:00
Last Modified: 2025-01-16T10:35:00

Indexes (2):
+-----------+--------+----------------+---------------+
| Column    | Unique | Distinct Keys  | Total Entries |
|-----------+--------+----------------+---------------|
| id        | Yes    | 3              | 3             |
| author_id | No     | 2              | 3             |
+-----------+--------+----------------+---------------+
```

## ⌨️ Keyboard Shortcuts

| Shortcut  | Action                         |
| --------- | ------------------------------ |
| `Tab`     | Auto-complete                  |
| `Up/Down` | Command history                |
| `Ctrl+C`  | Cancel current input           |
| `Ctrl+D`  | Exit REPL                      |
| `Ctrl+L`  | Clear screen (or use `.clear`) |
| `Ctrl+R`  | Reverse search history         |

## 🎨 Tips & Tricks

### 1. Use Meta Commands for Quick Info

```sql
-- Instead of SELECT * FROM users;
simpldb> .describe users  -- Faster for schema info

-- Instead of manually counting
simpldb> .stats users     -- Shows row count instantly
```

### 2. Multi-line for Readability

```sql
simpldb> SELECT
      ->   u.username,
      ->   p.title
      -> FROM users u
      -> INNER JOIN posts p
      ->   ON u.id = p.author_id
      -> WHERE p.published = TRUE;
```

### 3. Export Before Major Changes

```sql
simpldb> .export backup_$(date +%Y%m%d).json
simpldb> DROP TABLE old_table;
```

### 4. Use History for Iteration

```bash
# Press Up arrow to recall and edit previous queries
simpldb> SELECT * FROM users WHERE age > 25;  # Run
# Press Up, edit, run again
simpldb> SELECT * FROM users WHERE age > 30;  # Modified
```

## 🐛 Troubleshooting

### Issue: Tab completion not working

**Solution**: Make sure `readline` is installed:

```bash
pip install readline
```

### Issue: History not saving

**Solution**: Check permissions on `~/.simpldb_history`:

```bash
chmod 644 ~/.simpldb_history
```

### Issue: Tables not aligned

**Solution**: Ensure `tabulate` is installed:

```bash
pip install tabulate
```

## 📚 Next Steps

After mastering the REPL:

1. Build the web application (Step 7)
2. Explore advanced queries (JOINs, subqueries)
3. Create complex database schemas
4. Experiment with indexing strategies

Happy querying! 🚀
