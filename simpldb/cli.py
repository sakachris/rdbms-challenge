"""
Command-line interface entry point
"""

from simpldb.repl import SimplDBREPL
import sys

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='SimplDB Interactive Shell')
    parser.add_argument('--name', default='simpldb', help='Database name')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    parser.add_argument('--execute', '-e', help='Execute SQL and exit')
    
    args = parser.parse_args()
    
    repl = SimplDBREPL(db_name=args.name, data_dir=args.data_dir)
    
    if args.execute:
        repl.execute_sql(args.execute)
        repl.db.close()
    else:
        try:
            repl.run()
        except Exception as e:
            print(f"Fatal error: {e}")
            repl.db.close()
            sys.exit(1)

if __name__ == "__main__":
    main()