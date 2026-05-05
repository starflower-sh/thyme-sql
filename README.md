[![Crates.io](https://img.shields.io/crates/v/thyme-sql.svg)](https://crates.io/crates/thyme-sql)

## Thyme-sql
Query runner and performance benchmark tool for Postgres.

### Installation
Thyme is available to install through cargo using:
`cargo install thyme-sql`

or you can pull the repo and use cargo to build a binary using:
`cargo build --release`

Thyme is in early development and not available on any other package managers as a result.

### Usage
Make a `THYME_DATABASE_URL` environment variable available pointing at your database, or use the `-u` argument.
Run `thyme` in a directory with sql files in it or `thyme -d '{target_dir}'` to run, sort, and print a formatted table of the performance of all the queries in that directory.

### The plans
- A way to specify expected times in the sql files using comments
- A way to define a reference file that will return the performance delta and alert if results have changed, for the purpose of refactoring queries.
- A way to output as CSV and Excel docs
- A way to repeat queries for averages and load testing
- A way to run the query over multiple connections for load testing
