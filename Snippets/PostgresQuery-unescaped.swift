import PostgresNIO

// snippet.unescaped
let id = 10000
let tableName = "users"
let query: PostgresQuery = """
    SELECT id, username, birthday FROM \(unescaped: tableName) WHERE id < \(id);
    """
// snippet.end
