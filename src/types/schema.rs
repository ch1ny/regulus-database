use super::value::DbValue;

/// 数据类型定义
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Null,
    Integer,
    Real,
    Text { max_length: Option<usize> },
    Blob { max_length: Option<usize> },
    Boolean,
    Date,
    Datetime,
}

impl DataType {
    pub fn null() -> Self {
        DataType::Null
    }

    pub fn integer() -> Self {
        DataType::Integer
    }

    pub fn real() -> Self {
        DataType::Real
    }

    pub fn text() -> Self {
        DataType::Text { max_length: None }
    }

    pub fn text_with_max(max_length: usize) -> Self {
        DataType::Text { max_length: Some(max_length) }
    }

    pub fn blob() -> Self {
        DataType::Blob { max_length: None }
    }

    pub fn blob_with_max(max_length: usize) -> Self {
        DataType::Blob { max_length: Some(max_length) }
    }

    pub fn boolean() -> Self {
        DataType::Boolean
    }

    pub fn date() -> Self {
        DataType::Date
    }

    pub fn datetime() -> Self {
        DataType::Datetime
    }

    /// 验证值是否与该类型兼容
    pub fn validate(&self, value: &DbValue) -> bool {
        match (self, value) {
            (DataType::Null, DbValue::Null) => true,
            (DataType::Integer, DbValue::Integer(_)) => true,
            (DataType::Real, DbValue::Real(_)) => true,
            (DataType::Real, DbValue::Integer(_)) => true, // 整数可以隐式转换为实数
            (DataType::Text { max_length }, DbValue::Text(s)) => {
                max_length.map_or(true, |max| s.len() <= max)
            }
            (DataType::Blob { max_length }, DbValue::Blob(b)) => {
                max_length.map_or(true, |max| b.len() <= max)
            }
            (DataType::Boolean, DbValue::Boolean(_)) => true,
            (DataType::Date, DbValue::Date(_)) => true,
            (DataType::Datetime, DbValue::Datetime(_)) => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text { max_length } => match max_length {
                Some(max) => write!(f, "TEXT({})", max),
                None => write!(f, "TEXT"),
            },
            DataType::Blob { max_length } => match max_length {
                Some(max) => write!(f, "BLOB({})", max),
                None => write!(f, "BLOB"),
            },
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Date => write!(f, "DATE"),
            DataType::Datetime => write!(f, "DATETIME"),
        }
    }
}

/// 列定义
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_value: Option<DbValue>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Column {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            default_value: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn default(mut self, value: DbValue) -> Self {
        self.default_value = Some(value);
        self
    }
}

/// 表结构定义
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        TableSchema {
            name: name.into(),
            columns,
        }
    }

    /// 根据列名获取列索引
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// 根据列名获取列定义
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// 获取主键列
    pub fn primary_key(&self) -> Option<&Column> {
        self.columns.iter().find(|c| c.primary_key)
    }

    /// 验证行值是否与 schema 匹配
    pub fn validate(&self, values: &[(String, DbValue)]) -> Result<(), SchemaError> {
        for (name, value) in values {
            let column = self.column(name).ok_or_else(|| SchemaError::UnknownColumn {
                table: self.name.clone(),
                column: name.clone(),
            })?;

            if !column.data_type.validate(value) {
                return Err(SchemaError::TypeMismatch {
                    table: self.name.clone(),
                    column: name.clone(),
                    expected: column.data_type.clone(),
                    actual: value.type_name().to_string(),
                });
            }

            if !column.nullable && value.is_null() {
                return Err(SchemaError::NotNullViolation {
                    table: self.name.clone(),
                    column: name.clone(),
                });
            }
        }

        Ok(())
    }
}

/// Schema 错误类型
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Unknown column '{column}' in table '{table}'")]
    UnknownColumn { table: String, column: String },

    #[error("Type mismatch for column '{column}' in table '{table}': expected {expected}, got {actual}")]
    TypeMismatch {
        table: String,
        column: String,
        expected: DataType,
        actual: String,
    },

    #[error("NOT NULL constraint failed for column '{column}' in table '{table}'")]
    NotNullViolation { table: String, column: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_builder() {
        let col = Column::new("id", DataType::integer())
            .not_null()
            .primary_key();

        assert_eq!(col.name, "id");
        assert!(!col.nullable);
        assert!(col.primary_key);
    }

    #[test]
    fn test_text_with_max_length() {
        let dt = DataType::text_with_max(50);
        assert!(dt.validate(&DbValue::text("hello")));
        assert!(!dt.validate(&DbValue::text("a".repeat(51))));
    }

    #[test]
    fn test_schema_validation() {
        let schema = TableSchema::new(
            "users",
            vec![
                Column::new("id", DataType::integer()).primary_key(),
                Column::new("name", DataType::text()).not_null(),
                Column::new("age", DataType::integer()),
            ],
        );

        let valid_values = vec![
            ("id".to_string(), DbValue::integer(1)),
            ("name".to_string(), DbValue::text("Alice")),
            ("age".to_string(), DbValue::integer(25)),
        ];

        assert!(schema.validate(&valid_values).is_ok());
    }
}
