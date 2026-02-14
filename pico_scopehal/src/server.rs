/**
*   SCPI commands supported:

       *IDN?
           Returns a standard SCPI instrument identification string

       CHANS?
           Returns the number of channels on the instrument.

       SEQNUM?
           Returns the most recently sent sequence number

       [1|2]D:PRESENT?
           Returns 1 = MSO pod present, 0 = MSO pod not present

       [chan]:BWLIM [freq]
           Sets the channel's bandwith limiter to freq in MHz, 0 for full bandwidth.

       [chan]:BWLIM?
           Returns the channel's bandwith limiter frequency in MHz, 0 for full bandwidth.

       [chan]:COUP [DC1M|AC1M|DC50]
           Sets channel coupling

       [chan]:HYS [mV]
           Sets MSO channel hysteresis to mV millivolts

       [chan]:OFF
           Turns the channel off

       [chan]:OFFS [num]
           Sets channel offset to num volts

       [chan]:ON
           Turns the channel on

       [chan]:RANGE [num]
           Sets channel full-scale range to num volts

       [chan]:THRESH [mV]
           Sets MSO channel threshold to mV millivolts

       BITS [num]
           Sets ADC bit depth

       DEPTH [num]
           Sets memory depth

       DEPTHS?
           Returns the set of available memory depths

       EXIT
           Terminates the connection

       FORCE
           Forces a single acquisition

       RATE [num]
           Sets sample rate

       RATES?
           Returns a comma separated list of sampling rates (in femtoseconds)

       SINGLE
           Arms the trigger in one-shot mode

       START
           Arms the trigger

       STOP
           Disarms the trigger

       TRIG:DELAY [delay]
           Sets trigger delay (in fs)

       TRIG:EDGE:DIR [direction]
           Sets trigger direction. Legal values are RISING, FALLING, or ANY.

       TRIG:LEV [level]
           Selects trigger level (in volts)

       TRIG:SOU [chan]
           Selects the channel as the trigger source

       TODO: SetDigitalPortInteractionCallback to determine when pods are connected/removed

       AWG:DUTY [duty cycle]
           Sets duty cycle of function generator output

       AWG:FREQ [freq]
           Sets function generator frequency, in Hz

       AWG:OFF [offset]
           Sets offset of the function generator output

       AWG:RANGE [range]
           Sets p-p voltage of the function generator output

       AWG:SHAPE [waveform type]
           Sets waveform type

       AWG:START
           Starts the function generator

       AWG:STOP
           Stops the function generator
*/
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use pico_streaming::{NewDataHandler, StreamingEvent};
use tracing::{debug, info, warn};

use crate::scpi::{parse_command, process_command, CommandResult};
use crate::state::{AppState, FS_PER_SECOND};

const MAX_WAVEFORMS_IN_FLIGHT: u32 = 5;

pub(crate) fn run_server(
    state: Arc<AppState>,
    scpi_listener: TcpListener,
    waveform_listener: TcpListener,
) -> Result<()> {
    loop {
        let (stream, addr) = scpi_listener.accept()?;
        info!("SCPI client connected from {}", addr);

        let waveform_listener = waveform_listener.try_clone()?;
        let (stop_tx, stop_rx) = bounded::<()>(1);
        let wf_state = state.clone();
        let handle = thread::Builder::new()
            .name("waveform-session".to_string())
            .spawn(move || {
                if let Err(err) = serve_waveform_session(wf_state, waveform_listener, stop_rx) {
                    warn!("waveform session ended: {}", err);
                }
            })?;

        if let Err(err) = handle_scpi_session(state.clone(), stream, stop_tx.clone()) {
            warn!("SCPI session error: {}", err);
        }

        let _ = stop_tx.send(());
        let _ = handle.join();
        state.stop_streaming();
        info!("SCPI client disconnected");
    }
}

fn handle_scpi_session(state: Arc<AppState>, stream: TcpStream, stop_tx: Sender<()>) -> Result<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = stream;

    while let Some(line) = read_scpi_line(&mut reader)? {
        // debug!("SCPI <= {}", line);
        if let Some(command) = parse_command(&line) {
            match process_command(state.as_ref(), &command) {
                Ok(CommandResult::Reply(response)) => {
                    // println!(
                    //     "Received SCPI cmd: {} and replied: {}",
                    //     command.command, response
                    // );
                    writer.write_all(response.as_bytes())?;
                    writer.write_all(b"\n")?;
                    writer.flush()?;
                }
                Ok(CommandResult::NoReply) => {
                    debug!("SCPI <= {} (no response)", line);
                }
                Ok(CommandResult::Exit) => break,
                Err(err) => {
                    let msg = format!("ERR,{}\n", err);
                    println!(
                        "Received SCPI cmd: {} and replied: {}",
                        command.command, msg
                    );
                    writer.write_all(msg.as_bytes())?;
                    writer.flush()?;
                }
            }
        }
    }
    let _ = stop_tx.send(());
    Ok(())
}

fn read_scpi_line(reader: &mut BufReader<TcpStream>) -> io::Result<Option<String>> {
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        return Ok(Some(trimmed.to_string()));
    }
}


struct StreamForwarder {
    tx: Sender<StreamingEvent>,
}

impl NewDataHandler for StreamForwarder {
    fn handle_event(&self, event: &StreamingEvent) {
        let _ = self.tx.send(event.clone());
    }
}

fn serve_waveform_session(
    state: Arc<AppState>,
    listener: TcpListener,
    stop_rx: Receiver<()>,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                info!("waveform client connected from {}", addr);
                let result = stream_waveforms(state.clone(), stream, stop_rx.clone());
                info!("waveform client disconnected");
                return result;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                match stop_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(_) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(RecvTimeoutError::Timeout) => continue,
                }
            }
            Err(err) => return Err(err.into()),
        }
    }
}

fn stream_waveforms(
    state: Arc<AppState>,
    mut stream: TcpStream,
    stop_rx: Receiver<()>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(1)))?;

    let (tx, rx) = bounded::<StreamingEvent>(8);
    let handler = Arc::new(StreamForwarder { tx });
    state.device().new_data.subscribe(handler.clone());

    let mut sequence = 0u32;
    let mut last_ack = 0u32;

    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }

        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => {
                sequence = sequence.wrapping_add(1);
                send_waveform(&state, &mut stream, &event, sequence)?;
                state.update_sequence(sequence);
                debug!("Checking for ACKs after sending seq={}, last_ack={}", sequence, last_ack);
                check_for_acks(&mut stream, &mut last_ack)?;
                debug!("After check: last_ack={}, outstanding={}", last_ack, sequence.wrapping_sub(last_ack));
                while sequence.wrapping_sub(last_ack) >= MAX_WAVEFORMS_IN_FLIGHT {
                    debug!("Waiting for ACKs: seq={}, last_ack={}, outstanding={}", 
                           sequence, last_ack, sequence.wrapping_sub(last_ack));
                    check_for_acks(&mut stream, &mut last_ack)?;
                    thread::sleep(Duration::from_millis(1));
                }
                state.handle_waveform_complete();
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    Ok(())
}

fn send_waveform(
    state: &AppState,
    stream: &mut TcpStream,
    event: &StreamingEvent,
    sequence: u32,
) -> Result<()> {
    debug!("send_waveform: state.analog_channels = {:?}", state.analog_channels);
    let channels: Vec<_> = state
        .analog_channels
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, channel)| event.channels.get(&channel).map(|block| (index, channel, block)))
        .collect();

    if channels.is_empty() {
        debug!("No channels with data in event, skipping waveform");
        return Ok(());
    }
    
    debug!("Sending waveform seq={} with {} channels", sequence, channels.len());

    let fs_per_sample = if event.samples_per_second > 0 {
        (FS_PER_SECOND / event.samples_per_second as i64).max(1)
    } else {
        state.sample_interval_fs()
    };

    let header = WaveformHeader {
        sequence,
        num_channels: channels.len() as u16,
        fs_per_sample,
    };
    write_waveform_header(stream, &header)?;
    stream.flush()?;
    debug!("Sent waveform header: seq={}, num_channels={}, fs_per_sample={}", 
           sequence, header.num_channels, header.fs_per_sample);

    for (index, channel, block) in channels {
        let state_snapshot = state.channel_state(index);
        let channel_id = channel as u64;
        debug!("Processing channel: index={}, channel={:?}, channel_as_u64={}", index, channel, channel_id);
        let chan_header = ChannelHeader {
            index: channel_id,
            num_samples: block.samples.len() as u64,
            scale: (block.multiplier as f32).max(f32::MIN_POSITIVE),
            offset: state_snapshot.offset,
            trig_phase: 0.0,
        };
        debug!("  Channel {} ({:?}): {} samples, scale={}, offset={}, channel_id_u64={}", 
               chan_header.index, channel, chan_header.num_samples, chan_header.scale, chan_header.offset, channel_id);
        write_channel_header(stream, &chan_header)?;
        debug!("  Sent channel header for channel {}", channel_id);
        for sample in &block.samples {
            stream.write_i16::<LittleEndian>(*sample)?;
        }
        debug!("  Sent {} samples for channel {}", block.samples.len(), channel_id);
        stream.flush()?;
        debug!("  Flushed channel {} data", channel_id);
    }

    debug!("Completed sending waveform seq={}", sequence);
    Ok(())
}

struct WaveformHeader {
    sequence: u32,
    num_channels: u16,
    fs_per_sample: i64,
}

fn write_waveform_header(stream: &mut TcpStream, header: &WaveformHeader) -> io::Result<()> {
    stream.write_u32::<LittleEndian>(header.sequence)?;
    stream.write_u16::<LittleEndian>(header.num_channels)?;
    stream.write_i64::<LittleEndian>(header.fs_per_sample)?;
    Ok(())
}

struct ChannelHeader {
    index: u64,
    num_samples: u64,
    scale: f32,
    offset: f32,
    trig_phase: f32,
}

fn write_channel_header(stream: &mut TcpStream, header: &ChannelHeader) -> io::Result<()> {
    stream.write_u64::<LittleEndian>(header.index)?;
    stream.write_u64::<LittleEndian>(header.num_samples)?;
    stream.write_f32::<LittleEndian>(header.scale)?;
    stream.write_f32::<LittleEndian>(header.offset)?;
    stream.write_f32::<LittleEndian>(header.trig_phase)?;
    Ok(())
}

fn check_for_acks(stream: &mut TcpStream, last_ack: &mut u32) -> io::Result<()> {
    loop {
        let mut buf = [0u8; 4];
        match stream.read_exact(&mut buf) {
            Ok(()) => {
                *last_ack = u32::from_le_bytes(buf);
                debug!("received ACK {}", *last_ack);
            }
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            // Windows-specific: error 10035 is WSAEWOULDBLOCK
            #[cfg(windows)]
            Err(ref err) if err.raw_os_error() == Some(10035) => {
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "waveform client closed",
                ));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}
