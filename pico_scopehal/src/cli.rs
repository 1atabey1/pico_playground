use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about = "PicoScope HAL bridge for ngscopeclient")]
pub(crate) struct Cli {
    #[arg(long, default_value_t = 5025, help = "SCPI control port")]
    pub(crate) scpi_port: u16,
    #[arg(long, default_value_t = 5026, help = "Waveform streaming port")]
    pub(crate) waveform_port: u16,
    #[arg(long, help = "Exact serial to open (optional)")]
    pub(crate) serial: Option<String>,
}
