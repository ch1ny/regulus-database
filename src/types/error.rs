use crate::types::schema::SchemaError;
use std::fmt;

/// 数据库错误类型
#[derive(Debug)]
pub enum DbError {
    /// 表不存在
    TableNotFound(String),
    /// 表已存在
    TableAlreadyExists(String),
    /// 行不存在
    RowNotFound,
    /// Schema 错误
    SchemaError(SchemaError),
    /// 事务错误
    TransactionError(String),
    /// 索引错误
    IndexError(String),
    /// 其他错误
    Other(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            DbError::TableAlreadyExists(name) => write!(f, "Table already exists: {}", name),
            DbError::RowNotFound => write!(f, "Row not found"),
            DbError::SchemaError(err) => write!(f, "Schema error: {}", err),
            DbError::TransactionError(msg) => write!(f, "Transaction error: {}", msg),
            DbError::IndexError(msg) => write!(f, "Index error: {}", msg),
            DbError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for DbError {}

impl From<SchemaError> for DbError {
    fn from(err: SchemaError) -> Self {
        DbError::SchemaError(err)
    }
}

/// 数据库操作结果
pub type DbResult<T> = Result<T, DbError>;
