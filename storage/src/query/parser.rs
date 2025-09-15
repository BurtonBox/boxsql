use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::separated_list1,
    sequence::{delimited, preceded, terminated, tuple},
};

use crate::query::ast::{BinaryOperator, Expression, SelectItem, SelectStatement, Statement};
use crate::query::types::Value;

pub fn parse_sql(input: &str) -> anyhow::Result<Statement> {
    let (_remaining, stmt) = statement(input).map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
    Ok(stmt)
}

fn statement(input: &str) -> IResult<&str, Statement> {
    preceded(multispace0, alt((select_statement,)))(input)
}

fn select_statement(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("select")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, select_list) = select_list(input)?;
    let (input, from) = opt(from_clause)(input)?;
    let (input, where_clause) = opt(where_clause)(input)?;
    let (input, limit) = opt(limit_clause)(input)?;
    let (input, _) = multispace0(input)?;

    Ok((
        input,
        Statement::Select(SelectStatement {
            select_list,
            from,
            where_clause,
            limit,
        }),
    ))
}

fn select_list(input: &str) -> IResult<&str, Vec<SelectItem>> {
    separated_list1(delimited(multispace0, char(','), multispace0), select_item)(input)
}

fn select_item(input: &str) -> IResult<&str, SelectItem> {
    alt((
        map(char('*'), |_| SelectItem::Wildcard),
        map(expression, |expr| SelectItem::Expression {
            expr,
            alias: None,
        }),
    ))(input)
}

fn from_clause(input: &str) -> IResult<&str, String> {
    let (input, _) = preceded(multispace1, tag_no_case("from"))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = identifier(input)?;
    Ok((input, table))
}

fn where_clause(input: &str) -> IResult<&str, Expression> {
    let (input, _) = preceded(multispace1, tag_no_case("where"))(input)?;
    let (input, _) = multispace1(input)?;
    expression(input)
}

fn limit_clause(input: &str) -> IResult<&str, u32> {
    let (input, _) = preceded(multispace1, tag_no_case("limit"))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, num) = digit1(input)?;
    let limit = num.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((input, limit))
}

fn expression(input: &str) -> IResult<&str, Expression> {
    or_expression(input)
}

fn or_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = and_expression(input)?;
    let (input, rights) = nom::multi::many0(tuple((
        preceded(multispace0, tag_no_case("or")),
        preceded(multispace0, and_expression),
    )))(input)?;

    Ok((
        input,
        rights
            .into_iter()
            .fold(left, |acc, (_, right)| Expression::BinaryOp {
                left: Box::new(acc),
                op: BinaryOperator::Or,
                right: Box::new(right),
            }),
    ))
}

fn and_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = equality_expression(input)?;
    let (input, rights) = nom::multi::many0(tuple((
        preceded(multispace0, tag_no_case("and")),
        preceded(multispace0, equality_expression),
    )))(input)?;

    Ok((
        input,
        rights
            .into_iter()
            .fold(left, |acc, (_, right)| Expression::BinaryOp {
                left: Box::new(acc),
                op: BinaryOperator::And,
                right: Box::new(right),
            }),
    ))
}

fn equality_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = additive_expression(input)?;
    let (input, op_right) = opt(tuple((
        preceded(
            multispace0,
            alt((
                map(tag(">="), |_| BinaryOperator::Ge),
                map(tag("<="), |_| BinaryOperator::Le),
                map(tag("<>"), |_| BinaryOperator::Ne),
                map(tag("!="), |_| BinaryOperator::Ne),
                map(tag("="), |_| BinaryOperator::Eq),
                map(tag("<"), |_| BinaryOperator::Lt),
                map(tag(">"), |_| BinaryOperator::Gt),
            )),
        ),
        preceded(multispace0, additive_expression),
    )))(input)?;

    match op_right {
        Some((op, right)) => Ok((
            input,
            Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        )),
        None => Ok((input, left)),
    }
}

fn additive_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = multiplicative_expression(input)?;
    let (input, rights) = nom::multi::many0(tuple((
        preceded(
            multispace0,
            alt((
                map(char('+'), |_| BinaryOperator::Add),
                map(char('-'), |_| BinaryOperator::Sub),
            )),
        ),
        preceded(multispace0, multiplicative_expression),
    )))(input)?;

    Ok((
        input,
        rights
            .into_iter()
            .fold(left, |acc, (op, right)| Expression::BinaryOp {
                left: Box::new(acc),
                op,
                right: Box::new(right),
            }),
    ))
}

fn multiplicative_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = primary_expression(input)?;
    let (input, rights) = nom::multi::many0(tuple((
        preceded(
            multispace0,
            alt((
                map(char('*'), |_| BinaryOperator::Mul),
                map(char('/'), |_| BinaryOperator::Div),
            )),
        ),
        preceded(multispace0, primary_expression),
    )))(input)?;

    Ok((
        input,
        rights
            .into_iter()
            .fold(left, |acc, (op, right)| Expression::BinaryOp {
                left: Box::new(acc),
                op,
                right: Box::new(right),
            }),
    ))
}

fn primary_expression(input: &str) -> IResult<&str, Expression> {
    alt((
        literal_expression,
        column_expression,
        delimited(
            char('('),
            preceded(multispace0, terminated(expression, multispace0)),
            char(')'),
        ),
    ))(input)
}

fn literal_expression(input: &str) -> IResult<&str, Expression> {
    alt((integer_literal, string_literal, boolean_literal))(input)
}

fn integer_literal(input: &str) -> IResult<&str, Expression> {
    let (input, sign) = opt(char('-'))(input)?;
    let (input, digits) = digit1(input)?;
    let num_str = if sign.is_some() {
        format!("-{}", digits)
    } else {
        digits.to_string()
    };
    let value = num_str.parse::<i32>().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((
        input,
        Expression::Literal {
            value: Value::Integer(value),
        },
    ))
}

fn string_literal(input: &str) -> IResult<&str, Expression> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_while1(|c| c != '\'')(input)?;
    let (input, _) = char('\'')(input)?;
    Ok((
        input,
        Expression::Literal {
            value: Value::Varchar(content.to_string()),
        },
    ))
}

fn boolean_literal(input: &str) -> IResult<&str, Expression> {
    alt((
        map(tag_no_case("true"), |_| Expression::Literal {
            value: Value::Boolean(true),
        }),
        map(tag_no_case("false"), |_| Expression::Literal {
            value: Value::Boolean(false),
        }),
    ))(input)
}

fn column_expression(input: &str) -> IResult<&str, Expression> {
    let (input, name) = identifier(input)?;
    Ok((input, Expression::Column { name }))
}

fn identifier(input: &str) -> IResult<&str, String> {
    let (input, name) = recognize(tuple((
        alt((nom::character::complete::alpha1, tag("_"))),
        nom::bytes::complete::take_while(|c: char| c.is_alphanumeric() || c == '_'),
    )))(input)?;
    Ok((input, name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::SelectItem;

    #[test]
    fn test_select_star() {
        let sql = "SELECT * FROM users";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        assert_eq!(select.select_list, vec![SelectItem::Wildcard]);
        assert_eq!(select.from, Some("users".to_string()));
        assert!(select.where_clause.is_none());
        assert!(select.limit.is_none());
    }

    #[test]
    fn test_select_columns() {
        let sql = "SELECT id, name FROM users";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        assert_eq!(select.select_list.len(), 2);
        assert_eq!(select.from, Some("users".to_string()));
    }

    #[test]
    fn test_select_with_where() {
        let sql = "SELECT * FROM users WHERE id = 42";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        assert!(select.where_clause.is_some());
    }

    #[test]
    fn test_select_with_limit() {
        let sql = "SELECT * FROM users LIMIT 10";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        assert_eq!(select.limit, Some(10));
    }

    #[test]
    fn test_expression_parsing() {
        let sql = "SELECT 42 + 3 * 5";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        assert_eq!(select.select_list.len(), 1);
        assert!(select.from.is_none());
    }

    #[test]
    fn test_string_literal() {
        let sql = "SELECT 'hello world'";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        if let SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let Expression::Literal { value } = expr {
                assert_eq!(*value, Value::Varchar("hello world".to_string()));
            } else {
                panic!("Expected literal expression");
            }
        } else {
            panic!("Expected expression item");
        }
    }

    #[test]
    fn test_boolean_literals() {
        let sql = "SELECT true, false";
        let stmt = parse_sql(sql).unwrap();

        let Statement::Select(select) = stmt;
        assert_eq!(select.select_list.len(), 2);
    }
}
