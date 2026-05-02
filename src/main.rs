use clap::{ArgAction, Parser};
use std::{
    cmp::Reverse,
    env,
    path::{Path, PathBuf},
};

use comfy_table::{Cell, Table};
use dotenv::dotenv;
use serde_json::Value;
use sqlformat::{Dialect, FormatOptions, QueryParams, format};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{fs, time::Instant};

pub const RUN_FLAG: &str = "thyme-run";
pub const SKIP_FLAG: &str = "thyme-skip";
pub const NAME_PREFIX: &str = "thyme-name=";
pub const ARGS_PREFIX: &str = "thyme-args=";
pub const EXPECT_PREFIX: &str = "thyme-expect=";

fn get_env_var_or_exit(name: &str) -> String {
    dotenv().ok();

    match std::env::var(name) {
        Ok(val) => val,
        Err(_) => {
            println!("Required variable not set in environment: {name}");
            std::process::exit(1);
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'u', long)]
    database_url: Option<String>,

    #[arg(short = 'd', long, conflicts_with = "file")]
    dir: Option<PathBuf>,

    #[arg(short = 'f', long, conflicts_with = "dir")]
    file: Option<PathBuf>,

    #[arg(long)]
    thyme_file: Option<PathBuf>,

    #[arg(long, action = ArgAction::Set, default_value_t = true)]
    require_run_flag: bool,
}

#[tokio::main]
async fn main() {
    let arg = Args::parse();

    let database_url = arg
        .database_url
        .unwrap_or_else(|| get_env_var_or_exit("THYME_DATABASE_URL"));

    let thyme_config = match &arg.thyme_file {
        Some(path) => load_thyme_config(path).await,
        None => Value::Null,
    };

    let pg_pool = match PgPoolOptions::new()
        .max_connections(100)
        .connect(&database_url)
        .await
    {
        Ok(pool) => {
            println!("Successfully connected to the database.");
            pool
        }
        Err(err) => {
            println!("An error occurred connecting to the database: {err}");
            std::process::exit(1);
        }
    };

    println!("Running queries...");

    let mut res_vec = if let Some(file) = arg.file {
        if !file.is_file() {
            println!("Provided file path is not a file: {}", file.display());
            std::process::exit(1);
        }

        run_file(&pg_pool, &file, arg.require_run_flag, &thyme_config).await
    } else {
        let dir = arg.dir.unwrap_or_else(|| env::current_dir().unwrap());

        if !dir.is_dir() {
            println!(
                "Provided directory path is not a directory: {}",
                dir.display()
            );
            std::process::exit(1);
        }

        traverse_dirs(pg_pool, &dir, arg.require_run_flag, &thyme_config).await
    };

    if res_vec.is_empty() {
        println!("No queries found.");
        return;
    }

    res_vec.sort_by_key(|i| Reverse(i.1));

    let mut table = Table::new();
    table.set_header(vec!["Query", "Duration (sec)", "Duration (ms)", "Expected"]);

    for el in res_vec {
        let expected_cell = match el.2 {
            Some(true) => Cell::new("✅ pass").fg(comfy_table::Color::Green),
            Some(false) => Cell::new("❌ fail").fg(comfy_table::Color::Red),
            None => Cell::new("n/a"),
        };

        table.add_row(vec![
            Cell::new(el.0).fg(comfy_table::Color::Blue),
            Cell::new((el.1 as f64) / 1000.0).fg(comfy_table::Color::Green),
            Cell::new(el.1).fg(comfy_table::Color::Green),
            expected_cell,
        ]);
    }

    println!("{table}");
}

async fn load_thyme_config(path: &Path) -> Value {
    if !path.is_file() {
        println!("Provided thyme file path is not a file: {}", path.display());
        std::process::exit(1);
    }

    match fs::read_to_string(path).await {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(json) => json,
            Err(err) => {
                println!("Failed to parse thyme file: {err}");
                std::process::exit(1);
            }
        },
        Err(err) => {
            println!("Failed to read thyme file: {err}");
            std::process::exit(1);
        }
    }
}

fn extract_quoted_value(query: &str, prefix: &str) -> Option<String> {
    let start = query.find(prefix)? + prefix.len();
    let rest = query[start..].trim_start();

    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;

    Some(rest[..end].to_string())
}

fn json_value_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Array(_) | Value::Object(_) => {
            format!("'{}'", value.to_string().replace('\'', "''"))
        }
    }
}

fn query_params_from_value(value: &Value, label: &str) -> Result<QueryParams, String> {
    match value {
        Value::Object(map) => Ok(QueryParams::Named(
            map.iter()
                .map(|(key, value)| (key.clone(), json_value_to_sql_literal(value)))
                .collect(),
        )),
        Value::Array(values) => Ok(QueryParams::Indexed(
            values.iter().map(json_value_to_sql_literal).collect(),
        )),
        _ => Err(format!(
            "{label} must be an object for named args or an array for positional args"
        )),
    }
}

fn query_params_from_config(query: &str, config: &Value) -> Result<QueryParams, String> {
    let Some(key) = extract_quoted_value(query, ARGS_PREFIX) else {
        return Ok(QueryParams::None);
    };

    let Some(value) = config.get("args").and_then(|args| args.get(&key)) else {
        return Err(format!("No value found for args.{key}"));
    };

    query_params_from_value(value, &format!("args.{key}"))
}

fn format_query_with_args(query: &str, config: &Value) -> Result<String, String> {
    let params = query_params_from_config(query, config)?;

    let options = FormatOptions {
        dialect: Dialect::PostgreSql,
        ..FormatOptions::default()
    };

    Ok(format(query, &params, &options))
}

fn expected_rows_from_config(query: &str, config: &Value) -> Result<Option<Value>, String> {
    let Some(key) = extract_quoted_value(query, EXPECT_PREFIX) else {
        return Ok(None);
    };

    let Some(value) = config.get("expect").and_then(|expect| expect.get(&key)) else {
        return Err(format!("No value found for expect.{key}"));
    };

    match value {
        Value::Object(_) => Ok(Some(Value::Array(vec![value.clone()]))),
        Value::Array(values) => {
            for value in values {
                if !value.is_object() {
                    return Err(format!(
                        "expect.{key} must be an object or an array of objects"
                    ));
                }
            }

            Ok(Some(value.clone()))
        }
        _ => Err(format!(
            "expect.{key} must be an object or an array of objects"
        )),
    }
}

fn wrap_query_as_json(query: &str) -> String {
    format!(
        "select coalesce(jsonb_agg(to_jsonb(thyme_result)), '[]'::jsonb)::text from ({query}) as thyme_result"
    )
}

async fn query_output_as_json(pg_pool: &PgPool, query: &str) -> Result<Value, String> {
    let output = sqlx::query_scalar::<_, String>(&wrap_query_as_json(query))
        .fetch_one(pg_pool)
        .await
        .map_err(|err| err.to_string())?;

    serde_json::from_str::<Value>(&output).map_err(|err| err.to_string())
}

async fn traverse_dirs(
    pg_pool: PgPool,
    dir: &Path,
    require_run_flag: bool,
    thyme_config: &Value,
) -> Vec<(String, u128, Option<bool>)> {
    let mut stack = vec![dir.to_path_buf()];
    let mut res_vec: Vec<(String, u128, Option<bool>)> = vec![];

    while let Some(current_dir) = stack.pop() {
        let mut entries = fs::read_dir(&current_dir).await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();
            let file_type = entry.file_type().await.unwrap();

            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                let mut file_results =
                    run_file(&pg_pool, &path, require_run_flag, thyme_config).await;
                res_vec.append(&mut file_results);
            }
        }
    }

    res_vec
}

async fn run_file(
    pg_pool: &PgPool,
    path: &Path,
    require_run_flag: bool,
    thyme_config: &Value,
) -> Vec<(String, u128, Option<bool>)> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();

    if !filename.ends_with(".sql") {
        return vec![];
    }

    let queries: String = fs::read_to_string(path).await.unwrap();
    let queries: Vec<&str> = queries.split(';').collect();
    let mut res_vec = vec![];

    for (idx, query) in queries.iter().enumerate() {
        if query.trim().is_empty()
            || query.contains(SKIP_FLAG)
            || (!query.contains(RUN_FLAG) && require_run_flag)
        {
            continue;
        }

        let actual_query = match format_query_with_args(query, thyme_config) {
            Ok(query) => query,
            Err(err) => {
                println!("Skipping {} ({}): {err}", path.display(), idx + 1);
                continue;
            }
        };

        let expected_rows = match expected_rows_from_config(query, thyme_config) {
            Ok(value) => value,
            Err(err) => {
                println!("Skipping {} ({}): {err}", path.display(), idx + 1);
                continue;
            }
        };

        let query_name = extract_quoted_value(query, NAME_PREFIX)
            .unwrap_or_else(|| format!("{} ({})", path.display(), idx + 1));

        res_vec
            .push(execute_queries_in_file(pg_pool, query_name, &actual_query, expected_rows).await);
    }

    res_vec
}

async fn execute_queries_in_file(
    pg_pool: &PgPool,
    file_name: String,
    actual_query: &str,
    expected_rows: Option<Value>,
) -> (String, u128, Option<bool>) {
    let query_start_time = Instant::now();

    let result = match expected_rows {
        Some(expected_rows) => match query_output_as_json(pg_pool, actual_query).await {
            Ok(actual_rows) => {
                if actual_rows == expected_rows {
                    Ok(Some(true))
                } else {
                    println!("Expectation failed for {file_name}");
                    println!("Actual: {actual_rows}");
                    println!("Expected: {expected_rows}");
                    Ok(Some(false))
                }
            }
            Err(err) => Err(err),
        },
        None => sqlx::query(actual_query)
            .fetch_all(pg_pool)
            .await
            .map(|_| None)
            .map_err(|err| err.to_string()),
    };

    match result {
        Ok(expectation_result) => {
            let elapsed_time = query_start_time.elapsed();
            let query_execution_time_ms = elapsed_time.as_millis();
            (file_name, query_execution_time_ms, expectation_result)
        }
        Err(err) => {
            println!("Query failed for {file_name}: {err}");
            (file_name, 0, Some(false))
        }
    }
}
