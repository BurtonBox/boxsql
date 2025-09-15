use crate::query::types::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub select_list: Vec<SelectItem>,
    pub from: Option<String>,
    pub where_clause: Option<Expression>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Column {
        name: String,
    },
    Literal {
        value: Value,
    },
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
}

impl SelectStatement {
    pub fn select_all_from(table: &str) -> Self {
        Self {
            select_list: vec![SelectItem::Wildcard],
            from: Some(table.to_string()),
            where_clause: None,
            limit: None,
        }
    }

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
    pub fn column(name: &str) -> Self {
        Self::Column {
            name: name.to_string(),
        }
    }

    pub fn integer(value: i32) -> Self {
        Self::Literal {
            value: Value::Integer(value),
        }
    }

    pub fn string(value: &str) -> Self {
        Self::Literal {
            value: Value::Varchar(value.to_string()),
        }
    }

    pub fn boolean(value: bool) -> Self {
        Self::Literal {
            value: Value::Boolean(value),
        }
    }

    pub fn eq(left: Expression, right: Expression) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        }
    }

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
