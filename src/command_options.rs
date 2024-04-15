use structopt::StructOpt;

#[derive(StructOpt)]
pub enum CommandOptions {
    #[structopt(name = "start", about = "Start the server")]
    Start(StartOptions),
}

#[derive(StructOpt)]
pub struct StartOptions {
    #[structopt(short, long)]
    pub config_file: String,
}
