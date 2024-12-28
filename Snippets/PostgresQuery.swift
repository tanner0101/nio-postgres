import PostgresNIO

// snippet.select1
let id = 10000
let query: PostgresQuery = """
    SELECT id, username, birthday FROM users WHERE id < \(id);
    """
// snippet.end

