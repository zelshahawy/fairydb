use sqlparser::parser::Parser;

use sqlparser::ast::TableConstraint;
use sqlparser::ast::{ColumnDef, ColumnOption, Ident, Statement};
use sqlparser::parser::ParserError;

pub struct SQLParser {}

#[allow(clippy::upper_case_acronyms)]
#[allow(dead_code)]
#[derive(Debug)]
pub enum ParserResponse {
    Err,
    SQLError(ParserError),
    SQL(Vec<Statement>),
    SQLConstraintError(String),
}

impl Default for SQLParser {
    fn default() -> Self {
        Self::new()
    }
}

impl SQLParser {
    pub fn new() -> SQLParser {
        SQLParser {}
    }

    /// Validates sql string, first if it is sql itself, then if it has a primary key
    pub fn parse_sql(sql: String) -> ParserResponse {
        // Allows for multiple checks and different errors for each fail
        let request = SQLParser::validate_sql(sql);
        match request {
            ParserResponse::SQLError(_) => request,
            ParserResponse::SQL(ref ast) => {
                if ast.is_empty() {
                    return ParserResponse::SQLConstraintError(String::from("No Statement"));
                } else if ast.len() > 1 {
                    return ParserResponse::SQLConstraintError(String::from(
                        "Multiple Statements not supported",
                    ));
                }

                let statement = ast.first().unwrap();
                if let Statement::CreateTable {
                    columns,
                    constraints,
                    ..
                } = statement
                {
                    match SQLParser::get_pks(columns, constraints) {
                        Ok(_) => request,
                        Err(e) => e,
                    }
                } else {
                    request
                }
            }
            _ => unreachable!(),
        }
    }

    /// Returns Request::SQL if given string is valid sql, else returns Request::SQLError
    fn validate_sql(sql: String) -> ParserResponse {
        let dialect = sqlparser::dialect::GenericDialect {};
        match Parser::parse_sql(&dialect, &sql) {
            Ok(a) => ParserResponse::SQL(a),
            Err(e) => ParserResponse::SQLError(e),
        }
    }

    /// Returns a vector of the Idents of tables that are primary keys if valid
    /// Returns an error (as request) if there is a problem
    ///
    /// Returns an error in the following cases
    /// multiple inline primary key declarations: create table _ (a int primary key, b int primary key)
    /// multiple external primary key declarations: create table _ (a int, b int, primary key(a), primary key(b))
    /// primary keys declared inline and external: create table _ (a int primary key, b int, primary key(a))
    /// no primary key: create table _ (a int, b int)
    pub fn get_pks(
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Vec<Ident>, ParserResponse> {
        let mut res = Vec::new();
        let mut has_inline_pk = false;
        let mut has_external_pk = false;

        // checking for inline primary keys
        for column in columns {
            for column_option in &column.options {
                let option = &column_option.option;
                if let ColumnOption::Unique { is_primary: true } = option {
                    if has_inline_pk {
                        return Err(ParserResponse::SQLConstraintError(String::from(
                            "Table cannot have multiple inline primary keys",
                        )));
                    }
                    res.push(column.name.clone());
                    has_inline_pk = true;
                }
            }
        }

        // checking for external primary keys
        for constraint in constraints {
            if let TableConstraint::Unique {
                is_primary: true,
                columns: pk_columns,
                ..
            } = constraint
            {
                if has_inline_pk {
                    return Err(ParserResponse::SQLConstraintError(String::from(
                        "Table cannot have external and inline primary keys",
                    )));
                }

                if has_external_pk {
                    return Err(ParserResponse::SQLConstraintError(String::from(
                        "Table cannot have multiple external primary keys",
                    )));
                }
                for pk_column in pk_columns {
                    res.push(pk_column.clone());
                }
                has_external_pk = true;
            }
        }

        if res.is_empty() {
            return Err(ParserResponse::SQLConstraintError(String::from(
                "Table has no primary key",
            )));
        }
        Ok(res)
    }
    /*
    fn is_create_table(ast: &Request) -> bool {
        let mut create_table_check = false;
        if let Request::SQL(a) = ast {
            let table = a.get(0).unwrap();
            if let Statement::CreateTable { .. } = table {
                create_table_check = true;
            }
        }
        create_table_check
    }

    /// Returns true if given Request::SQL has a primary key (pk), else returns false
    fn validate_pk(ast: &Request) -> bool {
        let mut pk_check = false;
        if let Request::SQL(a) = ast {
            if a.len() != 1 {
                panic!("Multiple statements not supported");
            }
            let table = a.get(0).unwrap();
            if let Statement::CreateTable { constraints, .. } = table {
                let constraint_count = constraints.len();
                let mut n = 0;
                while n < constraint_count {
                    if let Some(TableConstraint::Unique {
                        is_primary: true,
                        name: _,
                        columns: _,
                    }) = constraints.get(n)
                    {
                        pk_check = true;
                        break;
                    }
                    n += 1;
                }
            } else {
                //All SQL statements that are not createTable is true
                return true;
            }
        }
        pk_check
    }

    pub fn is_pk(column: &ColumnDef) -> bool {
        let mut pk_check = false;
        let option_count = column.options.len();
        let mut n = 0;
        while n < option_count {
            let current_option = column.options.get(n);
            if let ColumnOption::Unique { is_primary: true } = current_option.unwrap().option {
                pk_check = true;
                break;
            }
            n += 1;
        }

        pk_check
    }
    */
}

#[cfg(test)]
mod libtests {
    use super::*;
    /*
    #[test]
    fn test_validate_non_createtable_sql() {
        let sql1 = String::from("SELECT * FROM Test");
        assert!(!SQLParser::is_create_table(&SQLParser::validate_sql(sql1)));
    }

    #[test]
    fn test_pk_constraint() {
        // Fail case
        let _sql1 = String::from(
            "CREATE TABLE Test (
            id int NOT NULL,
            age int NOT NULL,
            gpa int unique
        );",
        );
        // Pass case
        let sql2 = String::from(
            "CREATE TABLE Test (
            id int NOT NULL,
            age int NOT NULL,
            gpa int unique,
            PRIMARY KEY (ID)
        );",
        );
        // Pass case
        let _sql3 = String::from(
            "CREATE TABLE Test (
            id int NOT NULL,
            age int NOT NULL,
            gpa int unique,
            CONSTRAINT pk_id PRIMARY KEY (id)
        );",
        );
        // Temporary fail case
        let _sql4 = String::from(
            "CREATE TABLE Test (
            id int NOT NULL PRIMARY KEY,
            age int NOT NULL,
            gpa int unique
        );",
        );
        let request = SQLParser::validate_sql(sql2);
        let pk_check = SQLParser::validate_pk(&request);
        // println!("{:?}", pk_check);
        assert!(pk_check);
    }
    */

    #[test]
    fn test_get_pks() {
        // fail cases

        let sql1 = String::from("create table test (a int primary key, b int primary key)");
        if let ParserResponse::SQL(ast) = SQLParser::parse_sql(sql1) {
            if let Statement::CreateTable {
                columns,
                constraints,
                ..
            } = ast.first().unwrap()
            {
                assert!(SQLParser::get_pks(columns, constraints).is_err());
            }
        }

        let sql2 = String::from("create table test (a int primary key, b int, primary key(a)");
        if let ParserResponse::SQL(ast) = SQLParser::parse_sql(sql2) {
            if let Statement::CreateTable {
                columns,
                constraints,
                ..
            } = ast.first().unwrap()
            {
                assert!(SQLParser::get_pks(columns, constraints).is_err());
            }
        }

        let sql3 = String::from("create table test (a int, b int, primary key(a), primary key(b)");
        if let ParserResponse::SQL(ast) = SQLParser::parse_sql(sql3) {
            if let Statement::CreateTable {
                columns,
                constraints,
                ..
            } = ast.first().unwrap()
            {
                assert!(SQLParser::get_pks(columns, constraints).is_err());
            }
        }

        let sql4 = String::from("create table test (a int, b int)");
        if let ParserResponse::SQL(ast) = SQLParser::parse_sql(sql4) {
            if let Statement::CreateTable {
                columns,
                constraints,
                ..
            } = ast.first().unwrap()
            {
                assert!(SQLParser::get_pks(columns, constraints).is_err());
            }
        }

        // success cases
        let sql5 = String::from("create table test (a int, b int primary key)");
        if let ParserResponse::SQL(ast) = SQLParser::parse_sql(sql5) {
            if let Statement::CreateTable {
                columns,
                constraints,
                ..
            } = ast.first().unwrap()
            {
                assert_eq!(
                    SQLParser::get_pks(columns, constraints)
                        .unwrap()
                        .iter()
                        .map(|x| x.value.clone())
                        .collect::<Vec<String>>(),
                    vec!["b"]
                );
            }
        }

        let sql6 = String::from("create table test(a int, b int, primary key (a, b)");
        if let ParserResponse::SQL(ast) = SQLParser::parse_sql(sql6) {
            if let Statement::CreateTable {
                columns,
                constraints,
                ..
            } = ast.first().unwrap()
            {
                assert_eq!(
                    SQLParser::get_pks(columns, constraints)
                        .unwrap()
                        .iter()
                        .map(|x| x.value.clone())
                        .collect::<Vec<String>>(),
                    vec!["a", "b"]
                );
            }
        }

        let _sql_tpch = String::from(
            "CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
            PS_SUPPKEY     INTEGER NOT NULL,
            PS_AVAILQTY    INTEGER NOT NULL,
            PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
            PS_COMMENT     VARCHAR(199) NOT NULL,
            PRIMARY KEY(PS_PARTKEY, PS_SUPPKEY) )",
        );
    }
}
