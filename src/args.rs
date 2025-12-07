use std::fs::OpenOptions;
use std::path::PathBuf;

//use clap::builder::styling;
use anyhow::anyhow;
use clap::Parser;
use clap::builder::styling;
use parse_size::Config;
use simplelog::*;

const DEFAULT_BLOCK_SIZE: usize = 32768;

/// Hide a file into a PNG one.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, color = clap::ColorChoice::Always)]
pub struct Args {
    /// device tio image
    #[arg(short, long, required = true, value_name = "DEVICE")]
    pub dev: PathBuf,

    /// output file
    #[arg(short, long, required = true, value_name = "OUTPUT")]
    pub output: PathBuf,

    /// block size
    #[arg(long, value_name = "BLOCK_SIZE")]
    pub bs: Option<String>,

    /// number of thread to use
    #[arg(long, short)]
    pub threads: Option<usize>,

    /// stops after reading N blocks
    #[arg(long, short, value_name = "NB_BLOCKS")]
    pub nblocks: Option<u64>,

    /// log file
    #[arg(long)]
    pub log: Option<PathBuf>,

    // /// Postgresql database URL. if not specified, takes the value from the IAA_DB enviroment variable
    // #[arg(long)]
    // pub db: String,

    // /// if set, delete all rows from the table before inserting
    // #[arg(long)]
    // pub overwrite: bool,

    // /// if set, calculate BLAKE3 hashes
    // #[arg(long)]
    // pub blake3: bool,
    /// if set, output is similar to dd
    #[arg(long)]
    pub dd: bool,

    // /// if set, calculate Shannon entropy
    // #[arg(long)]
    // pub entropy: bool,
    /// Verbose mode (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

impl Args {
    pub fn block_size(&self) -> usize {
        let cfg = Config::new().with_binary();

        // convert any human units
        if let Some(bs) = &self.bs {
            cfg.parse_size(bs).unwrap_or(DEFAULT_BLOCK_SIZE as u64) as usize
        } else {
            DEFAULT_BLOCK_SIZE
        }
    }
}

pub fn get_args() -> anyhow::Result<Args> {
    let mut args = Args::parse();

    // by default, use number of cores for threads
    if args.threads.is_none() {
        args.threads = Some(num_cpus::get());
    }

    // extract loglevel from verbose flag
    let level = match args.verbose {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    // manage log file
    if let Some(path) = &args.log {
        init_write_logger(&path, level)?;
    } else {
        init_term_logger(level)?;
    }

    Ok(args)
}

// colors when displaying
const STYLES: styling::Styles = styling::Styles::styled()
    .header(styling::AnsiColor::Green.on_default().bold())
    .usage(styling::AnsiColor::Green.on_default().bold())
    .literal(styling::AnsiColor::Blue.on_default().bold())
    .placeholder(styling::AnsiColor::Cyan.on_default());

// Initialize write logger: either create it or use it
fn init_write_logger(logfile: &PathBuf, level: log::LevelFilter) -> anyhow::Result<()> {
    if level == log::LevelFilter::Off {
        return Ok(());
    }

    // initialize logger
    let writable = OpenOptions::new().create(true).append(true).open(logfile)?;

    WriteLogger::init(
        level,
        simplelog::ConfigBuilder::new()
            .set_time_format_rfc3339()
            // .set_time_format_custom(format_description!(
            //     "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
            .build(),
        writable,
    )?;

    Ok(())
}

// Initialize terminal logger
fn init_term_logger(level: log::LevelFilter) -> anyhow::Result<()> {
    if level == log::LevelFilter::Off {
        return Ok(());
    }
    TermLogger::init(
        level,
        simplelog::Config::default(),
        TerminalMode::Stderr,
        ColorChoice::Auto,
    )?;

    Ok(())
}
