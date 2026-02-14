use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::{Result, anyhow, bail};
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use pico_common::PicoCoupling;
use pico_streaming::{NewDataHandler, StreamingEvent};
use tracing::{debug, info, warn};

use crate::state::{AppState, FS_PER_SECOND, TriggerEdge};

const MAX_WAVEFORMS_IN_FLIGHT: u32 = 5;
const AWG_WAVEFORMS: &[&str] = &[
    "SINE",
    "SQUARE",
    "TRIANGLE",
    "RAMP_UP",
    "RAMP_DOWN",
    "DC",
    "WHITENOISE",
    "PRBS",
    "ARBITRARY",
];

enum CommandResult {
    NoReply,
    Reply(String),
    Exit,
}

impl From<Option<String>> for CommandResult {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(v) => CommandResult::Reply(v),
            None => CommandResult::NoReply,
        }
    }
}

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
        debug!("SCPI <= {}", line);
        if let Some(command) = parse_command(&line) {
            match process_command(&state, &command) {
                Ok(CommandResult::Reply(response)) => {
                    println!("Received SCPI cmd: {} and replied: {}", command.command, response);
                    writer.write_all(response.as_bytes())?;
                    writer.write_all(b"\n")?;
                    writer.flush()?;
                }
                Ok(CommandResult::NoReply) => {println!("Received SCPI cmd: {} and did not reply :(", command.command);}
                Ok(CommandResult::Exit) => break,
                Err(err) => {
                    let msg = format!("ERR,{}\n", err);
                    println!("Received SCPI cmd: {} and replied: {}", command.command, msg);
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

struct ParsedCommand<'a> {
    subject: Option<&'a str>,
    command: String,
    is_query: bool,
    args: Vec<&'a str>,
}

fn parse_command(line: &str) -> Option<ParsedCommand<'_>> {
    let mut parts = line.split_whitespace();
    let head = parts.next()?;
    let args: Vec<&str> = parts.collect();

    let (raw_subject, raw_command) = if let Some(pos) = head.find(':') {
        (&head[..pos], &head[pos + 1..])
    } else {
        ("", head)
    };

    let (command, is_query) = if let Some(cmd) = raw_command.strip_suffix('?') {
        (cmd.to_ascii_uppercase(), true)
    } else {
        (raw_command.to_ascii_uppercase(), false)
    };

    let subject = if raw_subject.is_empty() {
        None
    } else {
        Some(raw_subject)
    };

    Some(ParsedCommand {
        subject,
        command,
        is_query,
        args,
    })
}

fn process_command(state: &Arc<AppState>, command: &ParsedCommand<'_>) -> Result<CommandResult> {
    if let Some(subject) = command.subject {
        if subject.eq_ignore_ascii_case("AWG") {
            return handle_awg_command(state, command).map(Into::into);
        }
        if let Some(key) = parse_digital_subject(subject) {
            return handle_digital_command(state, &key, command).map(Into::into);
        }
        return handle_channel_command(state, subject, command).map(Into::into);
    }

    match command.command.as_str() {
        "*IDN" if command.is_query => Ok(Some(state.idn()).into()),
        "CHANS" if command.is_query => Ok(Some(state.channel_count().to_string()).into()),
        "SEQNUM" if command.is_query => Ok(Some(state.last_sequence().to_string()).into()),
        "RATES" if command.is_query => {
            let values = state
                .allowed_sample_rates()
                .iter()
                .map(|rate| {
                    if *rate == 0 {
                        0
                    } else {
                        FS_PER_SECOND / *rate as i64
                    }
                })
                .map(|interval| interval.to_string())
                .collect::<Vec<_>>()
                .join(",");
            Ok(Some(values).into())
        }
        "RATE" if command.is_query => Ok(Some(state.sample_rate().to_string()).into()),
        "RATE" => {
            let rate = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("RATE requires an argument"))?
                .parse()?;
            state.set_sample_rate(rate)?;
            Ok(CommandResult::NoReply)
        }
        "DEPTHS" if command.is_query => {
            let values = state
                .allowed_sample_depths()
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(",");
            Ok(Some(values).into())
        }
        "DEPTH" if command.is_query => Ok(Some(state.sample_depth().to_string()).into()),
        "DEPTH" => {
            let depth = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("DEPTH requires an argument"))?
                .parse()?;
            state.set_sample_depth(depth)?;
            Ok(CommandResult::NoReply)
        }
        "BITS" if command.is_query => Ok(Some(state.adc_bits().to_string()).into()),
        "BITS" => {
            let bits: u32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("BITS requires a value"))?
                .parse()?;
            state.set_adc_bits(bits)?;
            Ok(CommandResult::NoReply)
        }
        "START" => {
            state.start_streaming(false)?;
            Ok(CommandResult::NoReply)
        }
        "STOP" => {
            state.stop_streaming();
            Ok(CommandResult::NoReply)
        }
        "SINGLE" => {
            state.start_streaming(true)?;
            Ok(CommandResult::NoReply)
        }
        "FORCE" => {
            state.force_trigger();
            Ok(CommandResult::NoReply)
        }
        "ARMED" if command.is_query => {
            Ok(Some(if state.is_armed() { "1" } else { "0" }.into()).into())
        }
        cmd if cmd.starts_with("TRIG") => handle_trigger_command(state, command).map(Into::into),
        "EXIT" => Ok(CommandResult::Exit),
        _ => bail!("unsupported command {}", command.command),
    }
}

fn handle_trigger_command(
    state: &Arc<AppState>,
    command: &ParsedCommand<'_>,
) -> Result<Option<String>> {
    let segments: Vec<&str> = command.command.split(':').collect();
    match segments.as_slice() {
        ["TRIG", "SOU"] => {
            if command.is_query {
                let idx = state.trigger_source();
                return Ok(Some(format!("C{}", idx + 1)));
            }
            let token = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("TRIG:SOU requires a channel"))?;
            let idx = parse_channel_reference(token, state.channel_count())?;
            state.set_trigger_source(idx);
            Ok(None)
        }
        ["TRIG", "LEV"] => {
            if command.is_query {
                return Ok(Some(format!("{:.6}", state.trigger_level())));
            }
            let level: f64 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("TRIG:LEV requires a value"))?
                .parse()?;
            state.set_trigger_level(level);
            Ok(None)
        }
        ["TRIG", "DELAY"] => {
            if command.is_query {
                return Ok(Some(state.trigger_delay().to_string()));
            }
            let delay: i64 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("TRIG:DELAY requires a value"))?
                .parse()?;
            state.set_trigger_delay(delay);
            Ok(None)
        }
        ["TRIG", "EDGE", "DIR"] => {
            if command.is_query {
                return Ok(Some(state.trigger_edge().as_str().into()));
            }
            let dir = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("TRIG:EDGE:DIR requires a value"))?
                .to_ascii_uppercase();
            let edge = match dir.as_str() {
                "RISING" => TriggerEdge::Rising,
                "FALLING" => TriggerEdge::Falling,
                "EITHER" | "ANY" => TriggerEdge::Either,
                _ => bail!("invalid trigger direction {dir}"),
            };
            state.set_trigger_edge(edge);
            Ok(None)
        }
        _ => bail!("unsupported trigger command {}", command.command),
    }
}

fn handle_channel_command(
    state: &Arc<AppState>,
    subject: &str,
    command: &ParsedCommand<'_>,
) -> Result<Option<String>> {
    let index = parse_channel_reference(subject, state.channel_count())?;
    match command.command.as_str() {
        "ON" => {
            state.set_channel_enabled(index, true);
            Ok(None)
        }
        "OFF" => {
            state.set_channel_enabled(index, false);
            Ok(None)
        }
        "STATE" if command.is_query => {
            let enabled = state.channel_state(index).enabled;
            Ok(Some(if enabled { "1" } else { "0" }.into()))
        }
        "COUP" => {
            if command.is_query {
                let snapshot = state.channel_state(index);
                return Ok(Some(snapshot.coupling.to_string()));
            }
            let mode = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("channel coupling requires an argument"))?
                .to_ascii_uppercase();
            let coupling = match mode.as_str() {
                "DC" | "DC1M" => PicoCoupling::DC,
                "AC" | "AC1M" => PicoCoupling::AC,
                "DC50" => PicoCoupling::DC,
                _ => bail!("unsupported coupling {mode}"),
            };
            state.set_channel_coupling(index, coupling);
            Ok(None)
        }
        "RANGE" => {
            if command.is_query {
                let range = state.channel_state(index).range.get_max_scaled_value();
                return Ok(Some(range.to_string()));
            }
            let volts: f64 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("channel range requires a value"))?
                .parse()?;
            state.set_channel_range(index, volts);
            Ok(None)
        }
        "OFFS" => {
            if command.is_query {
                let offset = state.channel_state(index).offset;
                return Ok(Some(format!("{offset:.6}")));
            }
            let offset: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("channel offset requires a value"))?
                .parse()?;
            state.set_channel_offset(index, offset);
            Ok(None)
        }
        "BWLIM" => {
            if command.is_query {
                let limit = state.channel_bandwidth_limit(index).unwrap_or(0);
                return Ok(Some(limit.to_string()));
            }
            let freq: u32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("channel bandwidth limit requires a value"))?
                .parse()?;
            if freq == 0 {
                state.set_channel_bandwidth_limit(index, None);
            } else {
                state.set_channel_bandwidth_limit(index, Some(freq));
            }
            Ok(None)
        }
        _ => bail!(
            "unsupported channel command {} for {}",
            command.command,
            subject
        ),
    }
}

fn handle_awg_command(
    state: &Arc<AppState>,
    command: &ParsedCommand<'_>,
) -> Result<Option<String>> {
    match command.command.as_str() {
        "START" => {
            if command.is_query {
                let enabled = state.awg_state().enabled;
                return Ok(Some(if enabled { "1" } else { "0" }.into()));
            }
            state.set_awg_enabled(true);
            Ok(None)
        }
        "STOP" => {
            if command.is_query {
                let enabled = state.awg_state().enabled;
                return Ok(Some(if enabled { "1" } else { "0" }.into()));
            }
            state.set_awg_enabled(false);
            Ok(None)
        }
        "STATE" if command.is_query => {
            let enabled = state.awg_state().enabled;
            Ok(Some(if enabled { "1" } else { "0" }.into()))
        }
        "FREQ" => {
            if command.is_query {
                let freq = state.awg_state().frequency_hz;
                return Ok(Some(format!("{freq:.6}")));
            }
            let freq: f64 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("AWG:FREQ requires a value"))?
                .parse()?;
            state.set_awg_frequency(freq);
            Ok(None)
        }
        "DUTY" => {
            if command.is_query {
                let duty = state.awg_state().duty_cycle;
                return Ok(Some(format!("{duty:.3}")));
            }
            let duty: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("AWG:DUTY requires a value"))?
                .parse()?;
            state.set_awg_duty(duty);
            Ok(None)
        }
        "RANGE" => {
            if command.is_query {
                let range = state.awg_state().range_vpp;
                return Ok(Some(format!("{range:.3}")));
            }
            let range: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("AWG:RANGE requires a value"))?
                .parse()?;
            state.set_awg_range(range);
            Ok(None)
        }
        "OFF" => {
            if command.is_query {
                let offset = state.awg_state().offset_v;
                return Ok(Some(format!("{offset:.6}")));
            }
            let offset: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("AWG:OFF requires a value"))?
                .parse()?;
            state.set_awg_offset(offset);
            Ok(None)
        }
        "SHAPE" => {
            if command.is_query {
                let shape = state.awg_state().shape;
                return Ok(Some(shape));
            }
            let waveform = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("AWG:SHAPE requires a value"))?
                .to_ascii_uppercase();
            if !AWG_WAVEFORMS.contains(&waveform.as_str()) {
                bail!("unsupported AWG waveform {waveform}");
            }
            state.set_awg_shape(&waveform);
            Ok(None)
        }
        _ => bail!("unsupported AWG command {}", command.command),
    }
}

fn handle_digital_command(
    state: &Arc<AppState>,
    identifier: &str,
    command: &ParsedCommand<'_>,
) -> Result<Option<String>> {
    match command.command.as_str() {
        "PRESENT" => {
            if command.is_query {
                return Ok(Some("0".into()));
            }
            bail!("PRESENT can only be queried");
        }
        "HYS" => {
            if command.is_query {
                let hyst = state.digital_hysteresis(identifier);
                return Ok(Some(format!("{hyst:.3}")));
            }
            let hysteresis: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("HYS requires a value"))?
                .parse()?;
            state.set_digital_hysteresis(identifier, hysteresis);
            Ok(None)
        }
        "THRESH" => {
            if command.is_query {
                let thresh = state.digital_threshold(identifier);
                return Ok(Some(format!("{thresh:.3}")));
            }
            let threshold: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("THRESH requires a value"))?
                .parse()?;
            state.set_digital_threshold(identifier, threshold);
            Ok(None)
        }
        _ => bail!("unsupported digital command {}", command.command),
    }
}

fn parse_channel_reference(input: &str, limit: usize) -> Result<usize> {
    let token = input.trim().to_ascii_uppercase();
    if let Some(rest) = token.strip_prefix("CHAN") {
        let idx: usize = rest.parse()?;
        return channel_index_from_number(idx, limit);
    }
    if let Some(rest) = token.strip_prefix("CH") {
        let idx: usize = rest.parse()?;
        return channel_index_from_number(idx, limit);
    }
    if let Some(rest) = token.strip_prefix('C') {
        let idx: usize = rest.parse()?;
        return channel_index_from_number(idx, limit);
    }
    if token.len() == 1 {
        let ch = token.chars().next().unwrap();
        if ch >= 'A' && ch <= 'Z' {
            let idx = (ch as u8 - b'A') as usize;
            if idx < limit {
                return Ok(idx);
            }
        }
    }
    if let Ok(idx) = token.parse::<usize>() {
        return channel_index_from_number(idx, limit);
    }
    bail!("unable to parse channel reference '{input}'")
}

fn parse_digital_subject(subject: &str) -> Option<String> {
    let token = subject.trim();
    if token.is_empty() {
        return None;
    }
    let upper = token.to_ascii_uppercase();
    if upper
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
        && upper.contains('D')
    {
        Some(upper)
    } else {
        None
    }
}

fn channel_index_from_number(index: usize, limit: usize) -> Result<usize> {
    if index == 0 || index > limit {
        bail!("channel index {} out of range", index);
    }
    Ok(index - 1)
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
                check_for_acks(&mut stream, &mut last_ack)?;
                while sequence.wrapping_sub(last_ack) >= MAX_WAVEFORMS_IN_FLIGHT {
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
    let channels: Vec<_> = state
        .analog_channels
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, channel)| event.channels.get(&channel).map(|block| (index, block)))
        .collect();

    if channels.is_empty() {
        return Ok(());
    }

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

    for (index, block) in channels {
        let state_snapshot = state.channel_state(index);
        let chan_header = ChannelHeader {
            index: index as u64,
            num_samples: block.samples.len() as u64,
            scale: (block.multiplier as f32).max(f32::MIN_POSITIVE),
            offset: state_snapshot.offset,
            trig_phase: 0.0,
        };
        write_channel_header(stream, &chan_header)?;
        for sample in &block.samples {
            stream.write_i16::<LittleEndian>(*sample)?;
        }
    }

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
