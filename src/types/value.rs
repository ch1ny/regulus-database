use std::fmt;

/// 数据库值类型 - 支持 8 种基本类型
#[derive(Debug, Clone, PartialEq)]
pub enum DbValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
    Date(i64),      // Unix 时间戳（天）
    Datetime(i64),  // Unix 时间戳（毫秒）
}

impl DbValue {
    // 构造函数
    pub fn null() -> Self {
        DbValue::Null
    }

    pub fn integer(v: i64) -> Self {
        DbValue::Integer(v)
    }

    pub fn real(v: f64) -> Self {
        DbValue::Real(v)
    }

    pub fn text(v: impl Into<String>) -> Self {
        DbValue::Text(v.into())
    }

    pub fn blob(v: Vec<u8>) -> Self {
        DbValue::Blob(v)
    }

    pub fn boolean(v: bool) -> Self {
        DbValue::Boolean(v)
    }

    pub fn date(v: i64) -> Self {
        DbValue::Date(v)
    }

    pub fn datetime(v: i64) -> Self {
        DbValue::Datetime(v)
    }

    // 类型转换方法
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            DbValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_real(&self) -> Option<f64> {
        match self {
            DbValue::Real(v) => Some(*v),
            DbValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            DbValue::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            DbValue::Blob(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            DbValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<i64> {
        match self {
            DbValue::Date(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            DbValue::Datetime(v) => Some(*v),
            _ => None,
        }
    }

    // 判断是否为 Null
    pub fn is_null(&self) -> bool {
        matches!(self, DbValue::Null)
    }

    // 获取类型名称
    pub fn type_name(&self) -> &'static str {
        match self {
            DbValue::Null => "NULL",
            DbValue::Integer(_) => "INTEGER",
            DbValue::Real(_) => "REAL",
            DbValue::Text(_) => "TEXT",
            DbValue::Blob(_) => "BLOB",
            DbValue::Boolean(_) => "BOOLEAN",
            DbValue::Date(_) => "DATE",
            DbValue::Datetime(_) => "DATETIME",
        }
    }
}

impl fmt::Display for DbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbValue::Null => write!(f, "NULL"),
            DbValue::Integer(v) => write!(f, "{}", v),
            DbValue::Real(v) => write!(f, "{}", v),
            DbValue::Text(s) => write!(f, "'{}'", s),
            DbValue::Blob(b) => write!(f, "X'{}'", hex::encode(b)),
            DbValue::Boolean(v) => write!(f, "{}", if *v { "TRUE" } else { "FALSE" }),
            DbValue::Date(v) => write!(f, "DATE({})", v),
            DbValue::Datetime(v) => write!(f, "DATETIME({})", v),
        }
    }
}

// 简单的 hex 编码辅助函数（避免依赖）
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02X}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_conversion() {
        let v = DbValue::integer(42);
        assert_eq!(v.as_integer(), Some(42));
        assert_eq!(v.as_real(), Some(42.0));
        assert_eq!(v.as_boolean(), None);
    }

    #[test]
    fn test_text_conversion() {
        let v = DbValue::text("hello");
        assert_eq!(v.as_text(), Some("hello"));
        assert_eq!(v.as_integer(), None);
    }

    #[test]
    fn test_boolean_conversion() {
        let v = DbValue::boolean(true);
        assert_eq!(v.as_boolean(), Some(true));
    }

    #[test]
    fn test_null() {
        let v = DbValue::null();
        assert!(v.is_null());
        assert_eq!(v.as_integer(), None);
    }
}
