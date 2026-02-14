use std::{net::TcpListener, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::Parser;
use pico_streaming::ToStreamDevice;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::{
    cli::Cli,
    device::open_pico_device,
    server::run_server,
    state::{AppState, DeviceMetadata},
};

pub fn run() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    info!(
        "starting pico scopehal bridge on ports {} / {}",
        cli.scpi_port, cli.waveform_port
    );

    let device = open_pico_device(cli.serial.as_deref())?;
    let analog_channels = device.get_channels();
    if analog_channels.is_empty() {
        bail!("device reports zero analog channels");
    }

    info!(
        "connected to {} serial {} (USB {})",
        device.variant, device.serial, device.usb_version
    );

    let metadata = DeviceMetadata {
        serial: device.serial.clone(),
        variant: device.variant.clone(),
        usb_version: device.usb_version.clone(),
    };
    let max_adc = device.max_adc_value;
    let streaming = device.into_streaming_device();
    let state = Arc::new(AppState::new(streaming, analog_channels, max_adc, metadata));

    state.sync_all_channels();

    let scpi_listener = TcpListener::bind(("0.0.0.0", cli.scpi_port))
        .with_context(|| format!("failed to bind SCPI port {}", cli.scpi_port))?;
    let waveform_listener = TcpListener::bind(("0.0.0.0", cli.waveform_port))
        .with_context(|| format!("failed to bind waveform port {}", cli.waveform_port))?;

    run_server(state, scpi_listener, waveform_listener)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
