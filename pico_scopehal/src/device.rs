use anyhow::{Context, Result, anyhow, bail};
use pico_device::PicoDevice;
use pico_sdk::enumeration::{DeviceEnumerator, EnumeratedDevice, EnumerationError};
use tracing::warn;

pub(crate) fn open_pico_device(serial: Option<&str>) -> Result<PicoDevice> {
    let enumerator = DeviceEnumerator::new();
    let (devices, errors) =
        enumerator
            .enumerate()
            .into_iter()
            .fold((Vec::new(), Vec::new()), |mut acc, result| {
                match result {
                    Ok(device) => acc.0.push(device),
                    Err(error) => acc.1.push(error),
                }
                acc
            });

    for error in &errors {
        warn!("enumeration error: {}", error);
    }

    if devices.is_empty() {
        if errors.is_empty() {
            bail!("no PicoScope devices detected over USB");
        }
        bail!(
            "no PicoScope devices detected. {}",
            format_enumeration_errors(&errors)
        );
    }

    let descriptions: Vec<String> = devices
        .iter()
        .map(|dev| format!("{} ({})", dev.serial, dev.variant))
        .collect();

    let selected = select_device(serial, &devices, &descriptions)?;

    let selected_serial = selected.serial.clone();
    selected
        .open()
        .with_context(|| format!("failed to open PicoScope {}", selected_serial))
}

fn format_enumeration_errors(errors: &[EnumerationError]) -> String {
    errors
        .iter()
        .map(|err| err.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

fn select_device(
    serial: Option<&str>,
    devices: &[EnumeratedDevice],
    descriptions: &[String],
) -> Result<EnumeratedDevice> {
    match serial {
        Some(requested_serial) => devices
            .iter()
            .find(|dev| dev.serial.eq_ignore_ascii_case(requested_serial))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "no PicoScope with serial {} found. Detected devices: {}",
                    requested_serial,
                    descriptions.join(", ")
                )
            }),
        None => devices
            .iter()
            .find(|dev| dev.variant.to_ascii_uppercase().contains("2000"))
            .or_else(|| devices.first())
            .cloned()
            .ok_or_else(|| anyhow!("enumeration yielded no PicoScope devices")),
    }
}
