# ruff: noqa

from datetime import datetime


def insert_sample_data(db):
    """Insert comprehensive sample data for demonstration"""
    today = datetime.now().date().isoformat()

    # Sample users - diverse team of contributors
    users = [
        (1, "alice", "alice@simpldb.dev", "Alice Smith", "2026-01-14"),
        (2, "bob", "bob@simpldb.dev", "Bob Johnson", "2026-01-14"),
        (3, "charlie", "charlie@simpldb.dev", "Charlie Brown", "2026-01-15"),
        (4, "diana", "diana@simpldb.dev", "Diana Prince", "2026-01-15"),
        (5, "eve", "eve@simpldb.dev", "Eve Martinez", "2026-01-16"),
    ]

    for user_id, username, email, full_name, created in users:
        db.execute(
            f"""
            INSERT INTO users (id, username, email, full_name, created_at)
            VALUES ({user_id}, '{username}', '{email}', '{full_name}', '{created}')
            """
        )

    # Sample posts - comprehensive blog content about SimplDB
    posts = [
        (
            1,
            "Introducing SimplDB: A Custom RDBMS Built in 2 Days",
            """Welcome to SimplDB! Over the past two days, we've built a fully functional relational database
            management system from scratch using Python. This project demonstrates core database concepts
            including storage layers, schema validation, B-Tree indexing, SQL parsing, and query execution.
            What started as an educational experiment has evolved into a production-ready system capable of
            powering real applications. SimplDB supports all standard SQL operations (CREATE, INSERT, SELECT,
            UPDATE, DELETE), enforces schema constraints, maintains indexes for performance, and provides both
            a command-line REPL and a web interface. The entire system is containerized with Docker and deployed
            on Kubernetes, showcasing modern DevOps practices. In this blog series, we'll explore every aspect
            of building a database from the ground up, sharing insights, challenges, and solutions we
            discovered along the way.""",
            1,
            True,
            "2026-01-14",
            "2026-01-14",
        ),
        (
            2,
            "The Storage Layer: Persistence with JSON",
            """Every database needs a storage layer. For SimplDB, we chose JSON as our serialization format
            for its simplicity and human-readability. The storage layer consists of three key components:
            Row (representing individual records), TableStorage (managing single tables), and Storage (the
            main coordinator). Each table is stored as a separate JSON file containing rows, metadata, and
            auto-incrementing IDs. We implemented thread-safe operations using Python's threading module to
            ensure data integrity during concurrent access. The storage layer provides CRUD operations,
            bulk inserts, and metadata management. While JSON isn't the most efficient format for large
            datasets, it offers excellent debugging capabilities and makes the database internals
            transparent. Future iterations could swap JSON for binary formats like MessagePack or Protocol
            Buffers for better performance.""",
            1,
            True,
            "2026-01-14",
            "2026-01-14",
        ),
        (
            3,
            "Schema Validation: Type Safety and Constraints",
            """Schema management is critical for data integrity. SimplDB supports multiple data types:
            INTEGER, VARCHAR, FLOAT, BOOLEAN, DATE, and TEXT. Each column definition includes type
            information, optional length constraints, and various flags like PRIMARY KEY, UNIQUE, and
            NOT NULL. The schema manager validates every insert and update operation, automatically
            converting types when possible (e.g., string '25' to integer 25) and rejecting invalid data.
            We implemented default values, allowing columns to specify fallback data when no value is
            provided. The PRIMARY KEY constraint automatically implies UNIQUE and NOT NULL, simplifying
            table definitions. Schema definitions persist in the database catalog, ensuring they survive
            restarts. This validation layer prevents garbage data from entering the database and catches
            errors early in the development cycle.""",
            2,
            True,
            "2026-01-15",
            "2026-01-15",
        ),
        (
            4,
            "B-Tree Indexes: Making Queries Fast",
            """Without indexes, every query requires a full table scan - examining every single row. This is
            prohibitively slow for large datasets. SimplDB implements B-Tree-like indexes that reduce search
            complexity from O(n) to O(log n). Our index structure uses Python's bisect module for efficient
            binary searches on sorted keys. Each index maps values to row IDs, supporting both unique and
            non-unique indexes. When you create a PRIMARY KEY, SimplDB automatically creates a unique index
            on that column. You can manually create indexes on any column using CREATE INDEX. The index
            manager maintains all indexes automatically, updating them during INSERT, UPDATE, and DELETE
            operations. Range queries benefit especially from indexes - searching for 'age BETWEEN 25 AND
            35' becomes dramatically faster. In our benchmarks, indexed queries ran 50x faster than full
            table scans on 10,000 rows.""",
            3,
            True,
            "2026-01-15",
            "2026-01-15",
        ),
        (
            5,
            "SQL Parser: From Text to Structured Queries",
            """Parsing SQL is surprisingly complex. SimplDB's parser uses regular expressions to break down
            SQL statements into structured query objects. We support DDL commands like CREATE TABLE and
            DROP TABLE, and DML commands like INSERT, SELECT, UPDATE, and DELETE. The parser handles WHERE
            clauses with multiple conditions, ORDER BY for sorting, LIMIT and OFFSET for pagination, and
            even basic JOINs. Each SQL statement is transformed into a query object containing all
            necessary information: table names, columns, conditions, and operators. The parser performs
            basic validation, checking for syntax errors before execution. Multi-line queries are
            supported, making complex statements more readable. While we use regex for simplicity,
            production databases typically employ more sophisticated parsing techniques.""",
            2,
            True,
            "2026-01-15",
            "2026-01-16",
        ),
        (
            6,
            "Query Execution: Bringing It All Together",
            """The query executor is where everything converges. It takes parsed query objects and
            orchestrates their execution using the storage layer, schema manager, and index manager.
            INSERT operations validate data against the schema, check unique constraints via indexes,
            write to storage, and update all relevant indexes. SELECT queries leverage indexes when
            possible, falling back to full table scans when necessary. UPDATE and DELETE operations find
            matching rows, modify them, and maintain index consistency. The executor implements rollback
            behavior to keep data and indexes synchronized.""",
            4,
            True,
            "2026-01-16",
            "2026-01-16",
        ),
        (
            7,
            "The Interactive REPL: A Developer's Best Friend",
            """SimplDB includes a full-featured REPL (Read-Eval-Print Loop) for interactive database
            exploration. Built with Python's readline library, it offers tab completion, command history,
            multi-line queries, meta commands like .tables and .describe, and pretty-printed results.
            The REPL supports scripting, keyboard shortcuts, and persistent history storage.""",
            5,
            True,
            "2026-01-16",
            "2026-01-16",
        ),
        (
            8,
            "Building the Blog: SimplDB in Production",
            """To demonstrate SimplDB's capabilities, we built a complete blog application using Flask.
            The blog showcases CRUD operations, relationships, search functionality, REST APIs, and
            Kubernetes deployment with persistent volumes.""",
            1,
            True,
            "2026-01-16",
            "2026-01-16",
        ),
        (
            9,
            "Docker and Kubernetes: Deploying SimplDB",
            """Modern applications demand containerization. We packaged SimplDB using Docker and deployed
            it on Kubernetes with persistent storage, health checks, and ingress configuration.""",
            3,
            True,
            "2026-01-17",
            "2026-01-17",
        ),
        (
            10,
            "Performance Analysis: Benchmarking SimplDB",
            """We conducted extensive performance testing using datasets up to 100,000 rows. Indexed
            queries showed dramatic speed improvements, validating SimplDB's design.""",
            4,
            True,
            "2026-01-17",
            "2026-01-17",
        ),
        (
            11,
            "Lessons Learned: Building a Database in 48 Hours",
            """Creating SimplDB was an intense learning experience that reinforced core database concepts,
            trade-offs, testing discipline, and documentation importance.""",
            1,
            True,
            "2026-01-17",
            "2026-01-17",
        ),
        (
            12,
            "Future Roadmap: Where SimplDB Goes Next",
            """Planned features include aggregation, transactions, query optimization, security,
            replication, and sharding.""",
            5,
            True,
            today,
            today,
        ),
        (
            13,
            "Understanding B-Trees: The Secret to Fast Searches",
            """B-Trees are fundamental to database indexing and explain why indexes drastically improve
            query performance.""",
            3,
            True,
            "2026-01-16",
            "2026-01-16",
        ),
        (
            14,
            "Draft: Advanced Query Optimization Techniques",
            """This draft explores future optimization strategies such as cost-based planning and query
            caching.""",
            2,
            False,
            today,
            today,
        ),
    ]

    for (
        post_id,
        title,
        content,
        author_id,
        published,
        created,
        updated,
    ) in posts:
        safe_title = title.replace("'", "''")
        safe_content = content.replace("'", "''")
        db.execute(
            f"""
            INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
            VALUES ({post_id}, '{safe_title}', '{safe_content}', {author_id}, {published}, '{created}', '{updated}')
            """
        )

    # Sample comments - realistic engagement
    comments = [
        (
            1,
            1,
            2,
            "Incredible work! Building a database in 2 days is impressive.",
            "2026-01-14",
        ),
        (
            2,
            1,
            3,
            "Can't wait to dive into the code. This is a great learning resource!",
            "2026-01-14",
        ),
        (
            3,
            1,
            4,
            "The architecture diagram really helped me understand the design.",
            "2026-01-15",
        ),
        (
            4,
            2,
            3,
            "JSON for storage is brilliant for debugging. Great choice!",
            "2026-01-15",
        ),
        (
            5,
            2,
            5,
            "Have you considered using SQLite's file format for better performance?",
            "2026-01-15",
        ),
        (
            6,
            3,
            1,
            "The automatic type conversion is really handy. Saves so much boilerplate!",
            "2026-01-15",
        ),
        (7, 4, 2, "50x speedup with indexes! That's amazing.", "2026-01-16"),
        (
            8,
            4,
            4,
            "I'm curious about the B-Tree implementation.",
            "2026-01-16",
        ),
        (9, 5, 1, "Regex-based parsing is clever.", "2026-01-16"),
        (10, 6, 5, "The rollback mechanism is crucial.", "2026-01-16"),
        (11, 7, 3, "The REPL is so polished!", "2026-01-16"),
        (12, 7, 4, "Meta commands are genius.", "2026-01-17"),
        (
            13,
            8,
            2,
            "The blog demo really shows SimplDB's capabilities.",
            "2026-01-17",
        ),
        (
            14,
            9,
            1,
            "Kubernetes deployment guide worked perfectly.",
            "2026-01-17",
        ),
        (15, 9, 5, "Persistent volumes saved my data.", "2026-01-17"),
        (16, 10, 3, "Benchmark numbers are solid.", "2026-01-17"),
        (17, 11, 4, "Lessons learned section is gold.", "2026-01-17"),
        (18, 11, 2, "AI-assisted development is the future.", "2026-01-17"),
        (19, 12, 1, "Excited about the roadmap!", "2026-01-17"),
        (20, 12, 5, "Replication would be huge.", "2026-01-17"),
        (21, 13, 2, "Finally understand B-Trees.", "2026-01-17"),
    ]

    for comment_id, post_id, user_id, content, created in comments:
        safe_content = content.replace("'", "''")
        db.execute(
            f"""
            INSERT INTO comments (id, post_id, user_id, content, created_at)
            VALUES ({comment_id}, {post_id}, {user_id}, '{safe_content}', '{created}')
            """
        )

    print("✓ Sample data inserted")


# """Sample data insertion for demonstration purposes"""

# from datetime import datetime


# def insert_sample_data(db):
#     """Insert sample data for demonstration"""
#     today = datetime.now().date().isoformat()

#     # Sample users
#     users = [
#         (1, "alice", "alice@example.com", "Alice Smith", "2026-01-01"),
#         (2, "bob", "bob@example.com", "Bob Johnson", "2026-01-02"),
#         (3, "charlie", "charlie@example.com", "Charlie Brown", "2026-01-03"),
#     ]

#     for user_id, username, email, full_name, created in users:
#         db.execute(f"""
#             INSERT INTO users (id, username, email, full_name, created_at)
#             VALUES ({user_id}, '{username}', '{email}', '{full_name}', '{created}')
#         """)

#     # Sample posts
#     posts = [
#         (
#             1,
#             "Welcome to SimplDB Blog",
#             "This is our first blog post using SimplDB, a custom-built relational database!",
#             1,
#             True,
#             "2026-01-10",
#             "2026-01-10",
#         ),
#         (
#             2,
#             "Building Your Own Database",
#             "Learn how to build a simple RDBMS from scratch using Python.",
#             1,
#             True,
#             "2026-01-11",
#             "2026-01-11",
#         ),
#         (
#             3,
#             "Getting Started with SQL",
#             "A beginner's guide to SQL queries and database design.",
#             2,
#             True,
#             "2026-01-12",
#             "2026-01-12",
#         ),
#         (
#             4,
#             "Draft Post",
#             "This post is not yet published.",
#             2,
#             False,
#             today,
#             today,
#         ),
#     ]

#     for (
#         post_id,
#         title,
#         content,
#         author_id,
#         published,
#         created,
#         updated,
#     ) in posts:
#         db.execute(f"""
#             INSERT INTO posts (id, title, content, author_id, published, created_at, updated_at)
#             VALUES ({post_id}, '{title}', '{content}', {author_id}, {published}, '{created}', '{updated}')
#         """)

#     # Sample comments
#     comments = [
#         (
#             1,
#             1,
#             2,
#             "Great post! Looking forward to more content.",
#             "2026-01-10",
#         ),
#         (2, 1, 3, "Very informative, thank you!", "2026-01-10"),
#         (3, 2, 3, "This is exactly what I was looking for!", "2026-01-11"),
#     ]

#     for comment_id, post_id, user_id, content, created in comments:
#         db.execute(f"""
#             INSERT INTO comments (id, post_id, user_id, content, created_at)
#             VALUES ({comment_id}, {post_id}, {user_id}, '{content}', '{created}')
#         """)

#     print("✓ Sample data inserted")
