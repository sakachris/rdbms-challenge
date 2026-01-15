#!/bin/bash
# SimplDB REPL Demo Script
# This script demonstrates REPL features by executing commands

echo "SimplDB REPL Demo"
echo "================="
echo ""
echo "This demo shows various REPL features."
echo "Press Enter to continue..."
read

# Create a sample database
echo ""
echo "1. Creating sample database..."
python -m simpldb.repl --data-dir demo_repl --execute "CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR(50) UNIQUE NOT NULL, email VARCHAR(100), age INTEGER);"

echo ""
echo "2. Inserting sample data..."
python -m simpldb.repl --data-dir demo_repl --execute "INSERT INTO users (id, username, email, age) VALUES (1, 'alice', 'alice@example.com', 30);"
python -m simpldb.repl --data-dir demo_repl --execute "INSERT INTO users (id, username, email, age) VALUES (2, 'bob', 'bob@example.com', 25);"
python -m simpldb.repl --data-dir demo_repl --execute "INSERT INTO users (id, username, email, age) VALUES (3, 'charlie', 'charlie@example.com', 35);"

echo ""
echo "3. Creating an index..."
python -m simpldb.repl --data-dir demo_repl --execute "CREATE INDEX idx_age ON users(age);"

echo ""
echo "4. Querying data..."
python -m simpldb.repl --data-dir demo_repl --execute "SELECT * FROM users WHERE age > 25;"

echo ""
echo "5. Creating another table..."
python -m simpldb.repl --data-dir demo_repl --execute "CREATE TABLE posts (id INTEGER PRIMARY KEY, title VARCHAR(200), author_id INTEGER, published BOOLEAN DEFAULT FALSE);"

echo ""
echo "6. Inserting posts..."
python -m simpldb.repl --data-dir demo_repl --execute "INSERT INTO posts (id, title, author_id, published) VALUES (1, 'First Post', 1, TRUE);"
python -m simpldb.repl --data-dir demo_repl --execute "INSERT INTO posts (id, title, author_id, published) VALUES (2, 'Second Post', 2, FALSE);"

echo ""
echo "================="
echo "Demo complete!"
echo ""
echo "Now starting interactive REPL with the demo database..."
echo "Try these commands:"
echo "  .tables                    - List all tables"
echo "  .describe users            - Describe users table"
echo "  .stats                     - Show statistics"
echo "  SELECT * FROM users;       - Query users"
echo "  .help                      - Show all commands"
echo "  .exit                      - Exit REPL"
echo ""
echo "Press Enter to start interactive mode..."
read

# Start interactive REPL
python -m simpldb.repl --data-dir demo_repl