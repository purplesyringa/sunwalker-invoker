use crate::term::{CallTerm, Term};
use anyhow::{bail, Context, Result};
use std::iter::Peekable;
use std::str::Chars;

fn parse_literal(it: &mut Peekable<Chars>) -> Result<String> {
    let mut literal = String::new();
    loop {
        match it.peek() {
            None | Some(' ' | ')') => break,
            Some(_) => literal.push(it.next().unwrap()),
        }
    }
    if literal.is_empty() {
        bail!("Unexpected EOF");
    }
    Ok(literal)
}

fn parse_expr(it: &mut Peekable<Chars>) -> Result<Term> {
    skip_whitespace(it);
    match it.peek() {
        None => bail!("Unexpected EOF, expected expression"),
        Some('(') => {
            it.next();
            let name = parse_literal(it)?;
            skip_whitespace(it);
            let mut params: Vec<Term> = Vec::new();
            while it.peek() != Some(&')') {
                params.push(parse_expr(it)?);
                skip_whitespace(it);
            }
            it.next();
            Ok(Term::Call(CallTerm { name, params }))
        }
        Some('"') => {
            it.next();
            let mut value = String::new();
            loop {
                let c = it.next();
                match c {
                    None => bail!("Unexpected EOF inside string literal"),
                    Some('"') => break,
                    Some('\\') => {
                        let c = it.next();
                        match c {
                            None => {
                                bail!("Unexpected EOF inside string literal after escape character")
                            }
                            Some('\\') => value.push('\\'),
                            Some('n') => value.push('\n'),
                            Some('r') => value.push('\r'),
                            Some('t') => value.push('\t'),
                            Some(c) => bail!("Unknown escape \\{}", c),
                        };
                    }
                    Some(c) => value.push(c),
                };
            }
            Ok(Term::String(value))
        }
        Some(_) => match parse_literal(it)?.as_ref() {
            "nil" => Ok(Term::Nil),
            literal => {
                let value = literal
                    .parse()
                    .with_context(|| format!("Could not parse literal {literal}"))?;
                Ok(Term::Number(value))
            }
        },
    }
}

fn skip_whitespace(it: &mut Peekable<Chars>) {
    while let Some(' ' | '\t' | '\n' | '\r') = it.peek() {
        it.next();
    }
}

pub fn parse(code: &str) -> Result<Term> {
    let mut it = code.chars().peekable();
    let expr = parse_expr(&mut it)?;
    skip_whitespace(&mut it);
    if let Some(c) = it.next() {
        bail!("Expected EOF, got {}", c);
    }
    Ok(expr)
}
