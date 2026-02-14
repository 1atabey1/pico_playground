use anyhow::{Context, Result};
use clap::Parser;
use csv::Writer;
use pico_sdk::prelude::*;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Stream data from a PicoScope 2000 series oscilloscope to a CSV file
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Output CSV file path
    #[arg(short, long, default_value = "picoscope_data.csv")]
    output: String,

    /// Duration to stream in seconds
    #[arg(short, long, default_value_t = 10)]
    duration: u64,

    /// Sample rate in Hz
    #[arg(short, long, default_value_t = 1000)]
    sample_rate: u32,

    /// Enable Channel A
    #[arg(long, default_value_t = true)]
    channel_a: bool,

    /// Enable Channel B
    #[arg(long, default_value_t = false)]
    channel_b: bool,

    /// Voltage range for Channel A (in volts)
    #[arg(long, default_value_t = 5.0)]
    range_a: f64,

    /// Voltage range for Channel B (in volts)
    #[arg(long, default_value_t = 5.0)]
    range_b: f64,
}

/// Handler that writes streaming data to CSV
struct CsvDataHandler {
    writer: Arc<Mutex<Writer<File>>>,
    start_time: Instant,
    sample_count: Arc<Mutex<usize>>,
}

impl CsvDataHandler {
    fn new(output_path: &str) -> Result<Self> {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;
        let writer = Writer::from_writer(file);
        
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            start_time: Instant::now(),
            sample_count: Arc::new(Mutex::new(0)),
        })
    }
}

impl NewDataHandler for CsvDataHandler {
    fn handle_event(&self, event: &StreamingEvent) {
        let mut writer = self.writer.lock().unwrap();
        let mut count = self.sample_count.lock().unwrap();
        
        // Process each sample in the event
        for i in 0..event.length {
            let mut record = vec![
                format!("{:.6}", self.start_time.elapsed().as_secs_f64()),
                format!("{}", *count),
            ];
            
            // Add data for each channel
            for (_channel, data_block) in &event.channels {
                let scaled_value = data_block.scale_sample(i);
                record.push(format!("{:.6}", scaled_value));
            }
            
            if let Err(e) = writer.write_record(&record) {
                eprintln!("Error writing CSV record: {}", e);
            }
            
            *count += 1;
        }
        
        if let Err(e) = writer.flush() {
            eprintln!("Error flushing CSV: {}", e);
        }
    }
}

fn voltage_to_range(voltage: f64) -> PicoRange {
    match voltage {
        v if v <= 0.02 => PicoRange::X1_PROBE_20MV,
        v if v <= 0.05 => PicoRange::X1_PROBE_50MV,
        v if v <= 0.1 => PicoRange::X1_PROBE_100MV,
        v if v <= 0.2 => PicoRange::X1_PROBE_200MV,
        v if v <= 0.5 => PicoRange::X1_PROBE_500MV,
        v if v <= 1.0 => PicoRange::X1_PROBE_1V,
        v if v <= 2.0 => PicoRange::X1_PROBE_2V,
        v if v <= 5.0 => PicoRange::X1_PROBE_5V,
        v if v <= 10.0 => PicoRange::X1_PROBE_10V,
        _ => PicoRange::X1_PROBE_20V,
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("PicoScope 2000 Series Data Streamer");
    println!("====================================");
    println!("Output file: {}", args.output);
    println!("Duration: {} seconds", args.duration);
    println!("Sample rate: {} Hz", args.sample_rate);
    println!();

    // Enumerate devices
    println!("Enumerating devices...");
    let enumerator = DeviceEnumerator::new();
    let results = enumerator.enumerate();
    
    // Check for missing drivers
    let missing_drivers = results.missing_drivers();
    if !missing_drivers.is_empty() {
        println!("\n⚠ WARNING: Missing drivers detected!");
        println!("The following drivers are not available:");
        for driver in &missing_drivers {
            println!("  - {:?}", driver);
        }
        println!("\nYou can download missing drivers from:");
        println!("  https://www.picotech.com/downloads");
        println!("\nSome devices may not be detected without the required drivers.\n");
    }
    
    // Find the first available device
    let enum_device = results
        .into_iter()
        .flatten()
        .next()
        .context("No PicoScope device found")?;

    println!("Found device: {} (Serial: {})", 
             enum_device.variant, 
             &enum_device.serial);

    // Open the device
    println!("Opening device...");
    let device = enum_device.open()
        .context("Failed to open device")?;

    // Convert to streaming device
    let stream_device = device.into_streaming_device();

    // Configure channels
    let mut header = vec!["Time (s)".to_string(), "Sample #".to_string()];
    
    if args.channel_a {
        let range = voltage_to_range(args.range_a);
        println!("Enabling Channel A with range: {:?}", range);
        stream_device.enable_channel(PicoChannel::A, range, PicoCoupling::DC);
        header.push("Channel A (V)".to_string());
    }
    
    if args.channel_b {
        let range = voltage_to_range(args.range_b);
        println!("Enabling Channel B with range: {:?}", range);
        stream_device.enable_channel(PicoChannel::B, range, PicoCoupling::DC);
        header.push("Channel B (V)".to_string());
    }

    if !args.channel_a && !args.channel_b {
        anyhow::bail!("At least one channel must be enabled");
    }

    // Create CSV handler
    let handler = Arc::new(CsvDataHandler::new(&args.output)?);
    
    // Write CSV header
    {
        let mut writer = handler.writer.lock().unwrap();
        writer.write_record(&header)
            .context("Failed to write CSV header")?;
        writer.flush()
            .context("Failed to flush CSV header")?;
    }

    // Subscribe to streaming events
    stream_device.new_data.subscribe(handler.clone());

    // Start streaming
    println!("Starting streaming at {} Hz...", args.sample_rate);
    stream_device.start(args.sample_rate)
        .context("Failed to start streaming")?;

    // Stream for the specified duration
    let duration = Duration::from_secs(args.duration);
    let start = Instant::now();
    
    println!("Streaming... (Press Ctrl+C to stop early)");
    while start.elapsed() < duration {
        thread::sleep(Duration::from_millis(100));
        
        // Print progress every second
        if start.elapsed().as_secs() as u64 % 1 == 0 {
            let sample_count = *handler.sample_count.lock().unwrap();
            print!("\rElapsed: {:.1}s | Samples: {}    ", 
                   start.elapsed().as_secs_f64(), 
                   sample_count);
        }
    }
    
    println!("\n\nStopping streaming...");
    stream_device.stop();

    let final_count = *handler.sample_count.lock().unwrap();
    println!("Streaming complete!");
    println!("Total samples captured: {}", final_count);
    println!("Data saved to: {}", args.output);

    Ok(())
}
