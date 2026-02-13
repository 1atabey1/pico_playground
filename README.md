# PicoScope 2000 Series Data Streamer

A Rust tool for streaming data from a PicoScope 2000 series oscilloscope into a CSV file.

## Features

- Stream data from PicoScope 2000 series devices
- Configurable streaming duration
- Adjustable sample rate
- Support for Channel A and Channel B
- Configurable voltage ranges per channel
- Real-time CSV output with timestamps

## Prerequisites

- PicoScope 2000 series device
- PicoScope drivers installed on your system
  - Windows: Download from [Pico Technology website](https://www.picotech.com/downloads)
  - Linux: Install libudev-dev and drivers

## Building

```bash
cargo build --release
```

## Usage

### Basic Usage

Stream for 10 seconds at 1000 Hz from Channel A:

```bash
cargo run --release
```

### Custom Configuration

```bash
cargo run --release -- \
    --output my_data.csv \
    --duration 30 \
    --sample-rate 10000 \
    --channel-a \
    --channel-b \
    --range-a 5.0 \
    --range-b 2.0
```

### Command Line Options

- `-o, --output <FILE>` - Output CSV file path (default: `picoscope_data.csv`)
- `-d, --duration <SECONDS>` - Duration to stream in seconds (default: `10`)
- `-s, --sample-rate <HZ>` - Sample rate in Hz (default: `1000`)
- `--channel-a` - Enable Channel A (default: `true`)
- `--channel-b` - Enable Channel B (default: `false`)
- `--range-a <VOLTS>` - Voltage range for Channel A (default: `2.0`)
- `--range-b <VOLTS>` - Voltage range for Channel B (default: `2.0`)

### Supported Voltage Ranges

The tool automatically selects the appropriate hardware range based on your specified voltage:
- 0.02V (20mV)
- 0.05V (50mV)
- 0.1V (100mV)
- 0.2V (200mV)
- 0.5V (500mV)
- 1.0V
- 2.0V
- 5.0V
- 10.0V
- 20.0V

### Examples

**Stream from both channels for 60 seconds:**
```bash
cargo run --release -- -d 60 --channel-a --channel-b
```

**High-speed acquisition (100kHz) from Channel A:**
```bash
cargo run --release -- -s 100000 -d 5 --range-a 1.0
```

**Low-voltage measurements on Channel B:**
```bash
cargo run --release -- --channel-a=false --channel-b --range-b 0.1
```

## Output Format

The CSV file contains:
- **Time (s)** - Elapsed time since start in seconds
- **Sample #** - Sequential sample number
- **Channel A (V)** - Voltage reading from Channel A (if enabled)
- **Channel B (V)** - Voltage reading from Channel B (if enabled)

Example:
```csv
Time (s),Sample #,Channel A (V),Channel B (V)
0.000000,0,0.123456,0.234567
0.001000,1,0.125432,0.236789
0.002000,2,0.127654,0.238901
...
```

## Troubleshooting

**No device found:**
- Ensure your PicoScope is connected via USB
- Verify drivers are installed correctly
- On Linux, check USB permissions

**Compilation errors:**
- On Linux: Install `libudev-dev` package
- Ensure you have the latest Rust toolchain

**Performance issues:**
- Reduce sample rate for longer acquisitions
- Use release build (`--release`) for better performance
- Consider writing to SSD for high-speed streaming

## License

This project uses the pico-sdk crate, which provides unofficial Rust bindings for Pico Technology oscilloscope drivers.
