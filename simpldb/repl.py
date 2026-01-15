"""
SimplDB REPL - Interactive SQL Shell
Provides a command-line interface for executing SQL queries
"""

import sys
import os
import readline
import atexit
from pathlib import Path
from typing import Optional
from tabulate import tabulate
from datetime import datetime

from simpldb import Database


class SimplDBREPL:
    """Interactive REPL for SimplDB"""
    
    def __init__(self, db_name: str = "simpldb", data_dir: str = "data"):
        self.db = Database(name=db_name, data_dir=data_dir)
        self.running = True
        self.history_file = Path.home() / ".simpldb_history"
        self.command_buffer = []
        
        # Setup readline for command history
        self._setup_readline()
        
        # Command shortcuts
        self.meta_commands = {
            '.help': self.show_help,
            '.tables': self.show_tables,
            '.schema': self.show_schema,
            '.describe': self.describe_table,
            '.stats': self.show_stats,
            '.exit': self.exit_repl,
            '.quit': self.exit_repl,
            '.clear': self.clear_screen,
            '.export': self.export_schema,
            '.import': self.import_schema,
            '.dbinfo': self.show_db_info,
        }
    
    def _setup_readline(self):
        """Setup readline for command history and completion"""
        # Enable tab completion
        readline.parse_and_bind('tab: complete')
        readline.set_completer(self._completer)
        
        # Load history
        if self.history_file.exists():
            try:
                readline.read_history_file(self.history_file)
            except Exception:
                pass
        
        # Save history on exit
        atexit.register(self._save_history)
        
        # Set history length
        readline.set_history_length(1000)
    
    def _save_history(self):
        """Save command history to file"""
        try:
            readline.write_history_file(self.history_file)
        except Exception:
            pass
    
    def _completer(self, text, state):
        """Tab completion for SQL keywords and meta commands"""
        sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
            'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
            'INDEX', 'ON', 'PRIMARY', 'KEY', 'UNIQUE', 'NOT', 'NULL',
            'INTEGER', 'VARCHAR', 'FLOAT', 'BOOLEAN', 'DATE', 'TEXT',
            'AND', 'OR', 'ORDER', 'BY', 'LIMIT', 'OFFSET',
            'ASC', 'DESC', 'JOIN', 'INNER', 'LEFT', 'RIGHT'
        ]
        
        meta_commands = list(self.meta_commands.keys())
        
        # Get all possible completions
        options = []
        
        # Add SQL keywords
        options.extend([kw for kw in sql_keywords if kw.startswith(text.upper())])
        
        # Add meta commands
        options.extend([cmd for cmd in meta_commands if cmd.startswith(text.lower())])
        
        # Add table names
        try:
            tables = self.db.list_tables()
            options.extend([tbl for tbl in tables if tbl.startswith(text.lower())])
        except Exception:
            pass
        
        if state < len(options):
            return options[state]
        return None
    
    def run(self):
        """Run the REPL"""
        self.print_banner()
        
        while self.running:
            try:
                # Get input
                if not self.command_buffer:
                    prompt = "simpldb> "
                else:
                    prompt = "      -> "
                
                line = input(prompt).strip()
                
                if not line:
                    continue
                
                # Handle meta commands
                if line.startswith('.'):
                    self.handle_meta_command(line)
                    continue
                
                # Build multi-line SQL
                self.command_buffer.append(line)
                
                # Check if statement is complete (ends with semicolon)
                if line.endswith(';'):
                    sql = ' '.join(self.command_buffer)
                    self.command_buffer = []
                    self.execute_sql(sql)
                
            except KeyboardInterrupt:
                print("\nUse .exit or .quit to exit")
                self.command_buffer = []
            except EOFError:
                print()
                self.exit_repl()
            except Exception as e:
                print(f"Error: {e}")
                self.command_buffer = []
    
    def handle_meta_command(self, command: str):
        """Handle meta commands (starting with .)"""
        parts = command.split()
        cmd = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        
        if cmd in self.meta_commands:
            self.meta_commands[cmd](args)
        else:
            print(f"Unknown command: {cmd}")
            print("Type .help for available commands")
    
    def execute_sql(self, sql: str):
        """Execute SQL query and display results"""
        start_time = datetime.now()
        
        try:
            result = self.db.execute(sql)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if result.success:
                if result.rows:
                    # Display results as table
                    self.display_results(result.rows)
                    print(f"\n{len(result.rows)} row(s) returned in {elapsed:.3f}s")
                else:
                    print(f"{result.message}")
                    if result.rows_affected > 0:
                        print(f"{result.rows_affected} row(s) affected in {elapsed:.3f}s")
                    else:
                        print(f"Query executed in {elapsed:.3f}s")
            else:
                print(f"Error: {result.message}")
        
        except Exception as e:
            print(f"Error: {e}")
    
    def display_results(self, rows):
        """Display query results in a formatted table"""
        if not rows:
            print("No results")
            return
        
        # Get all unique keys from all rows (in case of joins with different columns)
        headers = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    headers.append(key)
                    seen.add(key)
        
        # Build table data
        table_data = []
        for row in rows:
            table_data.append([row.get(key, '') for key in headers])
        
        # Print table
        print()
        print(tabulate(table_data, headers=headers, tablefmt='psql'))
    
    def show_help(self, args):
        """Show help information"""
        help_text = """
SimplDB Interactive Shell - Help

SQL COMMANDS:
  Write SQL queries ending with semicolon (;)
  Multi-line queries are supported
  
  Examples:
    CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));
    INSERT INTO users (id, name) VALUES (1, 'Alice');
    SELECT * FROM users;
    UPDATE users SET name = 'Bob' WHERE id = 1;
    DELETE FROM users WHERE id = 1;

META COMMANDS:
  .help              Show this help message
  .tables            List all tables
  .schema [table]    Show schema for all tables or specific table
  .describe <table>  Show detailed information about a table
  .stats [table]     Show statistics for all or specific table
  .dbinfo            Show database information
  .export <file>     Export database schema to file
  .import <file>     Import database schema from file
  .clear             Clear screen
  .exit, .quit       Exit the shell

KEYBOARD SHORTCUTS:
  Tab                Auto-complete SQL keywords and table names
  Ctrl+C             Cancel current input
  Ctrl+D             Exit shell
  Up/Down arrows     Navigate command history

SUPPORTED SQL:
  - CREATE TABLE / DROP TABLE
  - CREATE INDEX / DROP INDEX  
  - INSERT INTO
  - SELECT (with WHERE, ORDER BY, LIMIT, JOIN)
  - UPDATE (with WHERE)
  - DELETE (with WHERE)

DATA TYPES:
  INTEGER, VARCHAR(n), FLOAT, BOOLEAN, DATE, TEXT

CONSTRAINTS:
  PRIMARY KEY, UNIQUE, NOT NULL, DEFAULT

For more information, visit: https://github.com/yourusername/simpldb
"""
        print(help_text)
    
    def show_tables(self, args):
        """List all tables"""
        tables = self.db.list_tables()
        
        if not tables:
            print("No tables in database")
            return
        
        print(f"\nTables in database ({len(tables)}):")
        for i, table in enumerate(sorted(tables), 1):
            stats = self.db.get_table_stats(table)
            row_count = stats['row_count'] if stats else 0
            print(f"  {i}. {table} ({row_count} rows)")
    
    def show_schema(self, args):
        """Show schema for tables"""
        if args:
            # Show schema for specific table
            table_name = args[0]
            self._show_table_schema(table_name)
        else:
            # Show schema for all tables
            tables = self.db.list_tables()
            if not tables:
                print("No tables in database")
                return
            
            for table_name in sorted(tables):
                self._show_table_schema(table_name)
                print()
    
    def _show_table_schema(self, table_name: str):
        """Show schema for a specific table"""
        info = self.db.describe_table(table_name)
        
        if not info:
            print(f"Table '{table_name}' not found")
            return
        
        print(f"\nTable: {table_name}")
        print("=" * 60)
        
        if 'columns' in info:
            print("\nColumns:")
            for col in info['columns']:
                constraints = ', '.join(col['constraints']) if col['constraints'] else ''
                length = f"({col['max_length']})" if col.get('max_length') else ''
                default = f" DEFAULT {col['default']}" if col.get('default') is not None else ''
                
                print(f"  {col['name']:20} {col['type']}{length:10} {constraints}{default}")
        
        if info.get('indexes'):
            print("\nIndexes:")
            for idx in info['indexes']:
                unique = "UNIQUE " if idx.get('unique') else ""
                print(f"  {unique}INDEX {idx['name']} ON {idx['column']}")
    
    def describe_table(self, args):
        """Show detailed table information"""
        if not args:
            print("Usage: .describe <table_name>")
            return
        
        table_name = args[0]
        info = self.db.describe_table(table_name)
        
        if not info:
            print(f"Table '{table_name}' not found")
            return
        
        print(f"\nTable: {table_name}")
        print("=" * 60)
        
        print(f"\nCreated: {info.get('created_at', 'Unknown')}")
        print(f"Last Modified: {info.get('last_modified', 'Unknown')}")
        print(f"Row Count: {info.get('row_count', 0)}")
        
        if 'columns' in info:
            print(f"\nColumns ({len(info['columns'])}):")
            
            col_data = []
            for col in info['columns']:
                constraints = ', '.join(col['constraints']) if col['constraints'] else '-'
                dtype = col['type']
                if col.get('max_length'):
                    dtype += f"({col['max_length']})"
                default = col['default'] if col.get('default') is not None else '-'
                
                col_data.append([col['name'], dtype, constraints, default])
            
            print(tabulate(col_data, headers=['Name', 'Type', 'Constraints', 'Default'], tablefmt='psql'))
        
        if info.get('indexes'):
            print(f"\nIndexes ({len(info['indexes'])}):")
            idx_data = []
            for idx in info['indexes']:
                unique = "Yes" if idx.get('unique') else "No"
                idx_data.append([idx['name'], idx['column'], unique])
            
            print(tabulate(idx_data, headers=['Name', 'Column', 'Unique'], tablefmt='psql'))
    
    def show_stats(self, args):
        """Show table statistics"""
        if args:
            # Stats for specific table
            table_name = args[0]
            self._show_table_stats(table_name)
        else:
            # Stats for all tables
            tables = self.db.list_tables()
            if not tables:
                print("No tables in database")
                return
            
            print(f"\nDatabase Statistics")
            print("=" * 60)
            
            stats_data = []
            total_rows = 0
            total_indexes = 0
            
            for table_name in sorted(tables):
                stats = self.db.get_table_stats(table_name)
                if stats:
                    rows = stats['row_count']
                    indexes = len(stats['indexes'])
                    total_rows += rows
                    total_indexes += indexes
                    stats_data.append([table_name, rows, indexes])
            
            print(tabulate(stats_data, headers=['Table', 'Rows', 'Indexes'], tablefmt='psql'))
            print(f"\nTotal: {len(tables)} tables, {total_rows} rows, {total_indexes} indexes")
    
    def _show_table_stats(self, table_name: str):
        """Show stats for specific table"""
        stats = self.db.get_table_stats(table_name)
        
        if not stats:
            print(f"Table '{table_name}' not found")
            return
        
        print(f"\nStatistics for table: {table_name}")
        print("=" * 60)
        print(f"Row Count: {stats['row_count']}")
        print(f"Created: {stats.get('created_at', 'Unknown')}")
        print(f"Last Modified: {stats.get('last_modified', 'Unknown')}")
        
        if stats['indexes']:
            print(f"\nIndexes ({len(stats['indexes'])}):")
            idx_data = []
            for idx in stats['indexes']:
                idx_data.append([
                    idx['column'],
                    "Yes" if idx['unique'] else "No",
                    idx['distinct_keys'],
                    idx['total_entries']
                ])
            
            print(tabulate(idx_data, 
                         headers=['Column', 'Unique', 'Distinct Keys', 'Total Entries'],
                         tablefmt='psql'))
    
    def show_db_info(self, args):
        """Show database information"""
        info = self.db.get_database_info()
        
        print(f"\nDatabase Information")
        print("=" * 60)
        print(f"Name: {info['name']}")
        print(f"Version: {info['version']}")
        print(f"Created: {info.get('created_at', 'Unknown')}")
        print(f"Data Directory: {info['data_directory']}")
        print(f"\nStatistics:")
        print(f"  Tables: {info['total_tables']}")
        print(f"  Total Rows: {info['total_rows']}")
        print(f"  Total Indexes: {info['total_indexes']}")
        
        if info['tables']:
            print(f"\nTables: {', '.join(sorted(info['tables']))}")
    
    def export_schema(self, args):
        """Export database schema"""
        if not args:
            print("Usage: .export <filename>")
            return
        
        filename = args[0]
        
        try:
            self.db.export_schema(filename)
            print(f"Schema exported to: {filename}")
        except Exception as e:
            print(f"Error exporting schema: {e}")
    
    def import_schema(self, args):
        """Import database schema"""
        if not args:
            print("Usage: .import <filename>")
            return
        
        filename = args[0]
        
        if not Path(filename).exists():
            print(f"File not found: {filename}")
            return
        
        try:
            results = self.db.import_schema(filename)
            success_count = sum(1 for r in results if r.success)
            print(f"Imported {success_count}/{len(results)} tables successfully")
        except Exception as e:
            print(f"Error importing schema: {e}")
    
    def clear_screen(self, args):
        """Clear the screen"""
        os.system('clear' if os.name != 'nt' else 'cls')
        self.print_banner()
    
    def exit_repl(self, args=None):
        """Exit the REPL"""
        print("Closing database...")
        self.db.close()
        print("Goodbye!")
        self.running = False
    
    def print_banner(self):
        """Print welcome banner"""
        banner = """
╔═══════════════════════════════════════════════════════════════════╗
║                        SimplDB v0.1.0                             ║
║              Simple Relational Database Management System         ║
╚═══════════════════════════════════════════════════════════════════╝

Type .help for help, .exit to quit
"""
        print(banner)


def main():
    """Main entry point for REPL"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SimplDB Interactive Shell')
    parser.add_argument('--name', default='simpldb', help='Database name')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    parser.add_argument('--execute', '-e', help='Execute SQL and exit')
    
    args = parser.parse_args()
    
    repl = SimplDBREPL(db_name=args.name, data_dir=args.data_dir)
    
    if args.execute:
        # Execute single command and exit
        repl.execute_sql(args.execute)
        repl.db.close()
    else:
        # Run interactive REPL
        try:
            repl.run()
        except Exception as e:
            print(f"Fatal error: {e}")
            repl.db.close()
            sys.exit(1)


if __name__ == "__main__":
    main()