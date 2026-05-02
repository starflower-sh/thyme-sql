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
pub const KEY_PREFIX: &str = "thyme-key=";

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
    thyme_args_file: Option<PathBuf>,

    #[arg(long, action = ArgAction::Set, default_value_t = true)]
    require_run_flag: bool,
}

#[tokio::main]
async fn main() {
    let arg = Args::parse();

    let database_url = arg
        .database_url
        .unwrap_or_else(|| get_env_var_or_exit("THYME_DATABASE_URL"));

    let thyme_args = match &arg.thyme_args_file {
        Some(path) => load_thyme_args(path).await,
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

        run_file(&pg_pool, &file, arg.require_run_flag, &thyme_args).await
    } else {
        let dir = arg.dir.unwrap_or_else(|| env::current_dir().unwrap());

        if !dir.is_dir() {
            println!(
                "Provided directory path is not a directory: {}",
                dir.display()
            );
            std::process::exit(1);
        }

        traverse_dirs(pg_pool, &dir, arg.require_run_flag, &thyme_args).await
    };

    if res_vec.is_empty() {
        println!("No queries found.");
        return;
    }

    res_vec.sort_by_key(|i| Reverse(i.1));

    let mut table = Table::new();
    table.set_header(vec!["Query", "Duration (sec)", "Duration (ms)"]);

    for el in res_vec {
        table.add_row(vec![
            Cell::new(el.0).fg(comfy_table::Color::Blue),
            Cell::new((el.1 as f64) / 1000.0).fg(comfy_table::Color::Green),
            Cell::new(el.1).fg(comfy_table::Color::Green),
        ]);
    }

    println!("{table}");
}

async fn load_thyme_args(path: &Path) -> Value {
    if !path.is_file() {
        println!(
            "Provided thyme args file path is not a file: {}",
            path.display()
        );
        std::process::exit(1);
    }

    match fs::read_to_string(path).await {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(json) => json.get("args").cloned().unwrap_or(Value::Null),
            Err(err) => {
                println!("Failed to parse thyme args file: {err}");
                std::process::exit(1);
            }
        },
        Err(err) => {
            println!("Failed to read thyme args file: {err}");
            std::process::exit(1);
        }
    }
}

fn extract_thyme_key(query: &str) -> Option<String> {
    let start = query.find(KEY_PREFIX)? + KEY_PREFIX.len();
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

fn query_params_from_thyme_key(query: &str, thyme_args: &Value) -> Result<QueryParams, String> {
    let Some(key) = extract_thyme_key(query) else {
        return Ok(QueryParams::None);
    };

    let Some(value) = thyme_args.get(&key) else {
        return Err(format!("No value found for args.{key}"));
    };

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
            "args.{key} must be an object for named args or an array for positional args"
        )),
    }
}

fn format_query_with_thyme_args(query: &str, thyme_args: &Value) -> Result<String, String> {
    let params = query_params_from_thyme_key(query, thyme_args)?;

    let options = FormatOptions {
        dialect: Dialect::PostgreSql,
        ..FormatOptions::default()
    };

    Ok(format(query, &params, &options))
}

async fn traverse_dirs(
    pg_pool: PgPool,
    dir: &Path,
    require_run_flag: bool,
    thyme_args: &Value,
) -> Vec<(String, u128)> {
    let mut stack = vec![dir.to_path_buf()];
    let mut res_vec: Vec<(String, u128)> = vec![];

    while let Some(current_dir) = stack.pop() {
        let mut entries = fs::read_dir(&current_dir).await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();
            let file_type = entry.file_type().await.unwrap();

            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                let mut file_results =
                    run_file(&pg_pool, &path, require_run_flag, thyme_args).await;
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
    thyme_args: &Value,
) -> Vec<(String, u128)> {
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

        let formatted_query = match format_query_with_thyme_args(query, thyme_args) {
            Ok(query) => query,
            Err(err) => {
                println!("Skipping {} ({}): {err}", path.display(), idx + 1);
                continue;
            }
        };
        println!("{formatted_query}");

        let query_name = format!("{} ({})", path.display(), idx + 1);
        res_vec.push(execute_queries_in_file(pg_pool, query_name, &formatted_query).await);
    }

    res_vec
}

async fn execute_queries_in_file(
    pg_pool: &PgPool,
    file_name: String,
    file_content: &str,
) -> (String, u128) {
    let query_start_time = Instant::now();

    match sqlx::query(file_content).fetch_all(pg_pool).await {
        Ok(_) => {
            let elapsed_time = query_start_time.elapsed();
            let query_execution_time_ms = elapsed_time.as_millis();
            (file_name, query_execution_time_ms)
        }
        Err(err) => {
            println!("Query failed for {file_name}: {err}");
            (file_name, 0)
        }
    }
}
