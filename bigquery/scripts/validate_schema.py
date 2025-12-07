import argparse
import sys
from sqlglot import parse_one, exp

# --- DEFINITIONS ---
# Mandatory: Missing these will cause the workflow to fail (Straight Rejection).
REQUIRED_AUDIT_COLUMNS = ['created_at', 'updated_at'] 

# Optimization Check: Missing these will print a warning message (Email Alert).
REQUIRED_PARTITIONING = True
REQUIRED_CLUSTERING = True

def check_audit_columns(table_exp):
    """Checks the DDL columns for required audit fields."""
    defined_columns = {col.name.upper() for col in table_exp.args.get('this').args['expressions']}
    
    missing_audit = [col for col in REQUIRED_AUDIT_COLUMNS if col.upper() not in defined_columns]
    
    if missing_audit:
        # GitHub Action logging syntax for failure
        print(f"::error::REJECTION: Missing mandatory audit columns in DDL: {', '.join(missing_audit)}")
        sys.exit(1) # Straight rejection!
        
def check_optimization(sql_content):
    """Checks the SQL content for required partitioning and clustering keywords."""
    sql_content_upper = sql_content.upper()
    
    # Check Partitioning
    if REQUIRED_PARTITIONING and 'PARTITION BY' not in sql_content_upper:
        print("::warning::ALERT: Table is NOT partitioned. Triggering notification email.")
        
    # Check Clustering
    if REQUIRED_CLUSTERING and 'CLUSTER BY' not in sql_content_upper:
        print("::warning::ALERT: Table is NOT clustered. Triggering notification email.")

def parse_and_validate(sql_file_path):
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    try:
        table_exp = parse_one(sql_content, read='bigquery').find(exp.CreateTable)
        if not table_exp:
             print("::error::REJECTION: SQL file does not contain a valid CREATE TABLE statement.")
             sys.exit(1)
    except Exception as e:
        print(f"::error::REJECTION: Failed to parse SQL DDL with sqlglot: {e}")
        sys.exit(1)

    check_audit_columns(table_exp)
    check_optimization(sql_content)
    sys.exit(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sql-file', required=True, help='Path to the SQL DDL file.')
    args = parser.parse_args()
    
    parse_and_validate(args.sql_file)