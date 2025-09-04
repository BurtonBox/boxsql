//! Basic data types and values for query processing.
//!
//! This module defines the type system used throughout the query processor,
//! including SQL data types, values, and schema definitions.

use serde::{Deserialize, Serialize};
use std::fmt;

/// SQL data types supported by the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// 32-bit signed integer
    Integer,
    /// Variable-length string
    Varchar(usize),
    /// Boolean value
    Boolean,
}

/// Runtime values that can be stored and processed.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// 32-bit signed integer
    Integer(i32),
    /// String value
    Varchar(String),
    /// Boolean value
    Boolean(bool),
    /// NULL value
    Null,
}

impl Value {
    /// Returns the data type of this value.
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Varchar(s) => DataType::Varchar(s.len()),
            Value::Boolean(_) => DataType::Boolean,
            Value::Null => DataType::Varchar(0), // NULL can be any type
        }
    }

    /// Serializes this value to bytes for storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Integer(i) => i.to_le_bytes().to_vec(),
            Value::Varchar(s) => {
                let mut bytes = Vec::new();
                let len = s.len() as u32;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
                bytes
            }
            Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
            Value::Null => vec![],
        }
    }

    /// Deserializes a value from bytes.
    pub fn from_bytes(bytes: &[u8], data_type: &DataType) -> anyhow::Result<Self> {
        match data_type {
            DataType::Integer => {
                if bytes.len() != 4 {
                    anyhow::bail!("Invalid integer length: {}", bytes.len());
                }
                let i = i32::from_le_bytes(bytes.try_into()?);
                Ok(Value::Integer(i))
            }
            DataType::Varchar(_) => {
                if bytes.len() < 4 {
                    anyhow::bail!("Invalid varchar length: {}", bytes.len());
                }
                let len = u32::from_le_bytes(bytes[0..4].try_into()?) as usize;
                if bytes.len() != len + 4 {
                    anyhow::bail!("Varchar length mismatch: {} vs {}", bytes.len(), len + 4);
                }
                let s = String::from_utf8(bytes[4..].to_vec())?;
                Ok(Value::Varchar(s))
            }
            DataType::Boolean => {
                if bytes.len() != 1 {
                    anyhow::bail!("Invalid boolean length: {}", bytes.len());
                }
                Ok(Value::Boolean(bytes[0] != 0))
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Varchar(s) => write!(f, "'{}'", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// Column definition in a table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: DataType,
    /// Whether the column allows NULL values
    pub nullable: bool,
}

/// Table schema definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    /// Columns in the table
    pub columns: Vec<Column>,
}

impl Schema {
    /// Creates a new schema with the given columns.
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Finds a column by name.
    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Returns the index of a column by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

/// A row of values matching a schema.
pub type Row = Vec<Value>;
