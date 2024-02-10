use clap::Parser;

#[derive(Parser, Debug)]
pub struct Config {
    /// List of table sources to load.
    ///
    /// Example: table1=./data/weather table2=/mnt/taxi
    #[arg(long, short, num_args = 1.., value_parser = parse_table_spec)]
    pub tables: Vec<TableSource>,
    /// Server address
    #[arg(long, short = 's', default_value = "0.0.0.0")]
    pub host: String,
    /// Server port
    #[arg(long, short = 'p', default_value_t = 50051)]
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct TableSource {
    pub name: String,
    pub path: String,
}

fn parse_table_spec(s: &str) -> Result<TableSource, clap::Error> {
    let Some(pos) = s.find('=') else {
        return Err(clap::Error::new(clap::error::ErrorKind::ValueValidation));
    };

    let table = TableSource {
        name: s[..pos].to_string(),
        path: s[pos + 1..].to_string(),
    };

    Ok(table)
}
