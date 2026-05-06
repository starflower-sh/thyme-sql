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
