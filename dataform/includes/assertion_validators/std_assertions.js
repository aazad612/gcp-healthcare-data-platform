/**
 * Checks for actual NULL values only.
 */
function not_null_check(columns, target_ref) {
  const where_clauses = columns.map(col => `${col} IS NULL`);
  return `SELECT "null_check" as validation_type, * FROM ${target_ref} WHERE ${where_clauses.join(' OR ')}`;
}

/**
 * Checks for strings that are either NULL, empty, or just whitespace.
 * Use this for high-priority data integrity.
 */
function no_whitespace_check(columns, target_ref) {
  // r'\s' in BigQuery catches spaces, tabs, and newlines
  const where_clauses = columns.map(col => `( ${col} IS NULL OR LENGTH(REGEXP_REPLACE(${col}, r'\\s', '')) = 0 )`);
  
  return `
SELECT 
  "strict_empty_check" as validation_type,
  * FROM ${target_ref} 
WHERE ${where_clauses.join(' OR ')}
`;
}
/**
 * Checks for duplicate records based on a composite key.
 */
function unique_check(key_columns, target_ref) {
  const columns_sql = key_columns.join(', ');
  return `
SELECT 
  "unique_check" as validation_type,
  ${columns_sql}, 
  COUNT(1) AS n_records
FROM ${target_ref}
GROUP BY ${columns_sql}
HAVING n_records > 1
`;
}

module.exports = { 
  not_null_check,
  no_whitespace_check,
  unique_check 
};