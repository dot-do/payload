/**
 * Validate that a table name is safe to use in SQL
 */
export function validateTableName(tableName: string): void {
  if (!/^[a-z_]\w*$/i.test(tableName)) {
    throw new Error(
      `Invalid table name '${tableName}'. Table names must start with a letter or underscore and contain only alphanumeric characters and underscores.`,
    )
  }
}

/**
 * Generate SQL to create the hybrid data table
 * Core columns for fast filtering + JSON for flexibility
 */
export function getCreateTableSQL(tableName: string): string {
  validateTableName(tableName)
  return `
CREATE TABLE IF NOT EXISTS ${tableName} (
    -- Core columns (indexed, fast filtering)
    ns String,
    tenant String DEFAULT '',
    type String,
    id String,
    v DateTime64(3),

    -- Common indexed fields
    createdAt DateTime64(3),
    createdBy Nullable(String),
    updatedAt DateTime64(3),
    updatedBy Nullable(String),

    -- Soft delete support
    deletedAt Nullable(DateTime64(3)),
    deletedBy Nullable(String),

    -- Flexible JSON for all field data
    data JSON,

    -- Searchable title
    title String DEFAULT ''

) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, tenant, type, id)
`
}

/**
 * Generate SQL to create the database if it doesn't exist
 */
export function getCreateDatabaseSQL(database: string): string {
  // Validate database name
  if (!/^[a-z_]\w*$/i.test(database)) {
    throw new Error(
      `Invalid database name '${database}'. Database names must start with a letter or underscore and contain only alphanumeric characters and underscores.`,
    )
  }
  return `CREATE DATABASE IF NOT EXISTS ${database}`
}
