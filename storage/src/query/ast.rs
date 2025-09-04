//! Abstract Syntax Tree (AST) definitions for SQL statements.
//!
//! This module defines the data structures representing parsed SQL statements.
//! The AST provides a structured representation that can be analyzed and
//! converted into execution plans.

use crate::query::types::Value;

/// Root SQL statement enum.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// SELECT statement
    Select(SelectStatement),
}

/// SELECT statement AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// List of expressions to select
    pub select_list: Vec<SelectItem>,
    /// Table to select from (optional for simple expressions)
    pub from: Option<String>,
    /// WHERE clause condition (optional)
    pub where_clause: Option<Expression>,
    /// LIMIT clause (optional)
    pub limit: Option<u32>,
}

/// Item in the SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// Wildcard (*) - select all columns
    Wildcard,
    /// Specific expression with optional alias
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

/// SQL expression node.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Column reference (e.g., "name", "users.id")
    Column { name: String },
    /// Literal value (e.g., 42, 'hello', true)
    Literal { value: Value },
    /// Binary operation (e.g., a + b, x = y)
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
}

/// Binary operators supported in expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    /// Equality (=)
    Eq,
    /// Inequality (!=, <>)
    Ne,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    Le,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Ge,
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Logical AND
    And,
    /// Logical OR
    Or,
}

impl SelectStatement {
    /// Creates a simple SELECT * FROM table statement.
    pub fn select_all_from(table: &str) -> Self {
        Self {
            select_list: vec![SelectItem::Wildcard],
            from: Some(table.to_string()),
            where_clause: None,
            limit: None,
        }
    }

    /// Creates a simple SELECT expression (no FROM clause).
    pub fn select_expression(expr: Expression) -> Self {
        Self {
            select_list: vec![SelectItem::Expression { expr, alias: None }],
            from: None,
            where_clause: None,
            limit: None,
        }
    }
}

impl Expression {
    /// Creates a column reference expression.
    pub fn column(name: &str) -> Self {
        Self::Column {
            name: name.to_string(),
        }
    }

    /// Creates a literal integer expression.
    pub fn integer(value: i32) -> Self {
        Self::Literal {
            value: Value::Integer(value),
        }
    }

    /// Creates a literal string expression.
    pub fn string(value: &str) -> Self {
        Self::Literal {
            value: Value::Varchar(value.to_string()),
        }
    }

    /// Creates a literal boolean expression.
    pub fn boolean(value: bool) -> Self {
        Self::Literal {
            value: Value::Boolean(value),
        }
    }

    /// Creates an equality expression.
    pub fn eq(left: Expression, right: Expression) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        }
    }

    /// Creates an addition expression.
    pub fn add(left: Expression, right: Expression) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Add,
            right: Box::new(right),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_all_from() {
        let stmt = SelectStatement::select_all_from("users");

        assert_eq!(stmt.select_list, vec![SelectItem::Wildcard]);
        assert_eq!(stmt.from, Some("users".to_string()));
        assert!(stmt.where_clause.is_none());
        assert!(stmt.limit.is_none());
    }

    #[test]
    fn test_expression_builders() {
        let col_expr = Expression::column("name");
        let int_expr = Expression::integer(42);
        let str_expr = Expression::string("hello");
        let bool_expr = Expression::boolean(true);

        assert_eq!(
            col_expr,
            Expression::Column {
                name: "name".to_string()
            }
        );
        assert_eq!(
            int_expr,
            Expression::Literal {
                value: Value::Integer(42)
            }
        );
        assert_eq!(
            str_expr,
            Expression::Literal {
                value: Value::Varchar("hello".to_string())
            }
        );
        assert_eq!(
            bool_expr,
            Expression::Literal {
                value: Value::Boolean(true)
            }
        );
    }

    #[test]
    fn test_binary_operations() {
        let left = Expression::column("id");
        let right = Expression::integer(100);
        let eq_expr = Expression::eq(left.clone(), right.clone());

        if let Expression::BinaryOp {
            left: l,
            op,
            right: r,
        } = eq_expr
        {
            assert_eq!(*l, left);
            assert_eq!(op, BinaryOperator::Eq);
            assert_eq!(*r, right);
        } else {
            panic!("Expected BinaryOp");
        }
    }
}
