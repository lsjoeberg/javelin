use std::path::Path;

use clap::Parser;

#[derive(Parser, Debug)]
pub struct Config {
    /// List of dataset root paths to serve
    #[arg(long, short, num_args = 0.., required = true)]
    pub datasets: Vec<String>,
    /// List of table names to use for each dataset
    #[arg(long, short, num_args = 0..)]
    pub table_names: Option<Vec<String>>,
    /// Server address
    #[arg(long, short = 's', default_value = "0.0.0.0")]
    pub host: String,
    /// Server port
    #[arg(long, short = 'p', default_value_t = 50051)]
    pub port: u16,
}

fn table_name_from_path(path: &str) -> Option<String> {
    let p = Path::new(path);
    match p.file_stem() {
        Some(s) => match s.to_ascii_lowercase().into_string() {
            Ok(s) => Some(s),
            _ => None,
        },
        None => None,
    }
}

pub fn parse_cfg() -> Config {
    let mut cfg = Config::parse();

    // Dataset paths and table names are pairwise.
    match &cfg.table_names {
        Some(names) => {
            if names.len() != cfg.datasets.len() {
                eprintln!("must provide exactly one table name for each dataset");
                std::process::exit(1);
            }
        }
        None => {
            let mut names: Vec<String> = vec![];
            for ds in &cfg.datasets {
                match table_name_from_path(ds) {
                    Some(t) => names.push(t.to_string()),
                    None => {
                        eprintln!("failed to parse table name from path {}", ds);
                        std::process::exit(1);
                    }
                }
            }
            cfg.table_names = Some(names);
        }
    }
    cfg
}
