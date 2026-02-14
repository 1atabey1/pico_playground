# pico-scopehal

Rust reimplementation of the ngscopeclient PicoScope hardware abstraction layer. This binary wraps the official [`pico-sdk`](https://docs.rs/pico-sdk/) crates to expose a SCPI control plane plus a waveform streaming socket that mirrors `scopehal-pico-bridge` behavior for PS2000A-class instruments.

## Features
- Enumerates connected PicoScope devices via `pico-sdk` and automatically loads the correct driver.
- Optional `--serial` filtering if multiple scopes are attached.
- SCPI control server (default port `5025`) compatible with ngscopeclient HAL expectations.
- Waveform streaming server (default port `5026`) built on `pico-streaming` for gapless acquisitions.
- Basic trigger, rate, depth, and per-channel configuration commands already wired up.

## Building
```bash
cargo build --release
```
The project targets Rust 1.78+ (Edition 2024). Installing the Pico Technology drivers for your platform is still required; `pico-sdk` dynamically loads the vendor libraries at runtime.

## Running
```bash
cargo run -- --scpi-port 5025 --waveform-port 5026 [--serial PSxxxxxx]
```
The binary will:
1. Enumerate all available scopes.
2. Pick the requested serial (or the first PS2000-series unit if no serial is provided).
3. Start listening for SCPI connections and waveform subscribers.

Point ngscopeclient (or another HAL consumer) to the SCPI port and connect the waveform socket to begin streaming data.

## Troubleshooting
- **No device found**: ensure the Pico drivers are installed and the USB kernel drivers are loaded (on Linux, `libudev-dev` is required). The application logs enumeration errors as they occur.
- **Permission issues**: on Linux you may need udev rules; on Windows run from an elevated shell the first time to allow driver installation.
- **Multiple clients**: only one SCPI client and one waveform subscriber are supported at a time; disconnect existing sessions before reconnecting.
