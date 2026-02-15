use anyhow::{Result, anyhow, bail};
use pico_common::PicoCoupling;

use crate::state::{AppState, AwgState, ChannelState, FS_PER_SECOND, TriggerEdge};

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

#[derive(Debug, PartialEq, Eq)]
pub enum CommandResult {
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

pub trait ScpiState: Send + Sync {
    fn idn(&self) -> String;
    fn channel_count(&self) -> usize;
    fn last_sequence(&self) -> u32;
    fn allowed_sample_rates(&self) -> &[u32];
    fn sample_rate(&self) -> u32;
    fn set_sample_rate(&self, rate: u32) -> Result<()>;
    fn allowed_sample_depths(&self) -> &[usize];
    fn sample_depth(&self) -> usize;
    fn set_sample_depth(&self, depth: usize) -> Result<()>;
    fn adc_bits(&self) -> u32;
    fn set_adc_bits(&self, bits: u32) -> Result<()>;
    fn start_streaming(&self, one_shot: bool) -> Result<u32>;
    fn stop_streaming(&self);
    fn force_trigger(&self);
    fn is_armed(&self) -> bool;
    fn trigger_source(&self) -> usize;
    fn set_trigger_source(&self, index: usize);
    fn trigger_level(&self) -> f64;
    fn set_trigger_level(&self, level: f64);
    fn trigger_delay(&self) -> i64;
    fn set_trigger_delay(&self, delay: i64);
    fn trigger_edge(&self) -> TriggerEdge;
    fn set_trigger_edge(&self, edge: TriggerEdge);
    fn channel_state(&self, index: usize) -> ChannelState;
    fn set_channel_enabled(&self, index: usize, enabled: bool);
    fn set_channel_coupling(&self, index: usize, coupling: PicoCoupling);
    fn set_channel_range(&self, index: usize, volts: f64);
    fn set_channel_offset(&self, index: usize, offset: f32);
    fn channel_bandwidth_limit(&self, index: usize) -> Option<u32>;
    fn set_channel_bandwidth_limit(&self, index: usize, mhz: Option<u32>);
    fn awg_state(&self) -> AwgState;
    fn set_awg_enabled(&self, enabled: bool);
    fn set_awg_frequency(&self, hz: f64);
    fn set_awg_duty(&self, duty: f32);
    fn set_awg_range(&self, range_vpp: f32);
    fn set_awg_offset(&self, offset_v: f32);
    fn set_awg_shape(&self, shape: &str);
    fn digital_hysteresis(&self, identifier: &str) -> f32;
    fn set_digital_hysteresis(&self, identifier: &str, hysteresis: f32);
    fn digital_threshold(&self, identifier: &str) -> f32;
    fn set_digital_threshold(&self, identifier: &str, threshold: f32);
}

#[derive(Clone)]
pub struct ParsedCommand<'a> {
    pub subject: Option<&'a str>,
    pub command: String,
    pub is_query: bool,
    pub args: Vec<&'a str>,
}

impl<'a> ParsedCommand<'a> {
    pub fn prefixed(&self, prefix: &str) -> ParsedCommand<'a> {
        let mut command = prefix.to_ascii_uppercase();
        if !self.command.is_empty() {
            command.push(':');
            command.push_str(&self.command);
        }
        ParsedCommand {
            subject: None,
            command,
            is_query: self.is_query,
            args: self.args.clone(),
        }
    }
}

pub fn parse_command(line: &str) -> Option<ParsedCommand<'_>> {
    let mut parts = line.split_whitespace();
    let head = parts.next()?;
    let args: Vec<&str> = parts.collect();

    let head = head.trim_start_matches(':');
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

pub fn process_command(state: &impl ScpiState, command: &ParsedCommand<'_>) -> Result<CommandResult> {
    if let Some(subject) = command.subject {
        if looks_like_channel_subject(subject) {
            return handle_channel_command(state, subject, command).map(Into::into);
        }
        if subject.eq_ignore_ascii_case("AWG") {
            return handle_awg_command(state, command).map(Into::into);
        }
        if let Some(key) = parse_digital_subject(subject) {
            return handle_digital_command(state, &key, command).map(Into::into);
        }
        if subject.eq_ignore_ascii_case("TRIG") {
            let combined = command.prefixed("TRIG");
            return handle_trigger_command(state, &combined).map(Into::into);
        }
        let combined = command.prefixed(subject);
        return process_command(state, &combined);
    }

    if let Some(prefixed) = trigger_alias(command) {
        return handle_trigger_command(state, &prefixed).map(Into::into);
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
        "BITS" => {
            if command.is_query {
                return Ok(Some(state.adc_bits().to_string()).into());
            }
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
        "ARMED" if command.is_query => Ok(Some(if state.is_armed() { "1" } else { "0" }.into()).into()),
        cmd if cmd.starts_with("TRIG") => handle_trigger_command(state, command).map(Into::into),
        "EXIT" => Ok(CommandResult::Exit),
        _ => bail!("unsupported command {}", command.command),
    }
}

fn trigger_alias<'a>(command: &ParsedCommand<'a>) -> Option<ParsedCommand<'a>> {
    match command.command.as_str() {
        "DELAY" | "LEV" | "SOU" | "EDGE:DIR" => Some(command.prefixed("TRIG")),
        _ => None,
    }
}

fn handle_trigger_command(
    state: &impl ScpiState,
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
    state: &impl ScpiState,
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
    state: &impl ScpiState,
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
        "OFFS" => {
            if command.is_query {
                let offset = state.awg_state().offset_v;
                return Ok(Some(format!("{offset:.6}")));
            }
            let offset: f32 = command
                .args
                .get(0)
                .ok_or_else(|| anyhow!("AWG:OFFS requires a value"))?
                .parse()?;
            state.set_awg_offset(offset);
            Ok(None)
        }
        "SHAPE" => {
            if command.is_query {
                let shape = state.awg_state().shape.clone();
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
    state: &impl ScpiState,
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
    if token.len() >= 2 {
        let mut chars = token.chars();
        if chars
            .next()
            .map(|c| c.is_ascii_alphabetic())
            .unwrap_or(false)
            && chars.all(|c| c.is_ascii_digit())
        {
            if let Ok(idx) = token[1..].parse::<usize>() {
                return channel_index_from_number(idx, limit);
            }
        }
    }
    if let Ok(idx) = token.parse::<usize>() {
        return channel_index_from_number(idx, limit);
    }
    bail!("unable to parse channel reference '{input}'")
}

fn looks_like_channel_subject(subject: &str) -> bool {
    let token = subject.trim();
    if token.is_empty() {
        return false;
    }
    let upper = token.to_ascii_uppercase();
    if upper.starts_with("CHAN") || upper.starts_with("CH") {
        return true;
    }
    if upper.len() == 1 {
        let c = upper.as_bytes()[0];
        return (b'A'..=b'Z').contains(&c);
    }
    let mut chars = upper.chars();
    if chars
        .next()
        .map(|c| c.is_ascii_alphabetic())
        .unwrap_or(false)
        && chars.clone().all(|c| c.is_ascii_digit())
    {
        return true;
    }
    upper.chars().all(|c| c.is_ascii_digit())
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

impl ScpiState for AppState {
    fn idn(&self) -> String {
        AppState::idn(self)
    }

    fn channel_count(&self) -> usize {
        AppState::channel_count(self)
    }

    fn last_sequence(&self) -> u32 {
        AppState::last_sequence(self)
    }

    fn allowed_sample_rates(&self) -> &[u32] {
        AppState::allowed_sample_rates(self)
    }

    fn sample_rate(&self) -> u32 {
        AppState::sample_rate(self)
    }

    fn set_sample_rate(&self, rate: u32) -> Result<()> {
        AppState::set_sample_rate(self, rate)
    }

    fn allowed_sample_depths(&self) -> &[usize] {
        AppState::allowed_sample_depths(self)
    }

    fn sample_depth(&self) -> usize {
        AppState::sample_depth(self)
    }

    fn set_sample_depth(&self, depth: usize) -> Result<()> {
        AppState::set_sample_depth(self, depth)
    }

    fn adc_bits(&self) -> u32 {
        AppState::adc_bits(self)
    }

    fn set_adc_bits(&self, bits: u32) -> Result<()> {
        AppState::set_adc_bits(self, bits)
    }

    fn start_streaming(&self, one_shot: bool) -> Result<u32> {
        AppState::start_streaming(self, one_shot)
    }

    fn stop_streaming(&self) {
        AppState::stop_streaming(self)
    }

    fn force_trigger(&self) {
        AppState::force_trigger(self)
    }

    fn is_armed(&self) -> bool {
        AppState::is_armed(self)
    }

    fn trigger_source(&self) -> usize {
        AppState::trigger_source(self)
    }

    fn set_trigger_source(&self, index: usize) {
        AppState::set_trigger_source(self, index);
    }

    fn trigger_level(&self) -> f64 {
        AppState::trigger_level(self)
    }

    fn set_trigger_level(&self, level: f64) {
        AppState::set_trigger_level(self, level);
    }

    fn trigger_delay(&self) -> i64 {
        AppState::trigger_delay(self)
    }

    fn set_trigger_delay(&self, delay: i64) {
        AppState::set_trigger_delay(self, delay);
    }

    fn trigger_edge(&self) -> TriggerEdge {
        AppState::trigger_edge(self)
    }

    fn set_trigger_edge(&self, edge: TriggerEdge) {
        AppState::set_trigger_edge(self, edge);
    }

    fn channel_state(&self, index: usize) -> ChannelState {
        AppState::channel_state(self, index)
    }

    fn set_channel_enabled(&self, index: usize, enabled: bool) {
        AppState::set_channel_enabled(self, index, enabled);
    }

    fn set_channel_coupling(&self, index: usize, coupling: PicoCoupling) {
        AppState::set_channel_coupling(self, index, coupling);
    }

    fn set_channel_range(&self, index: usize, volts: f64) {
        AppState::set_channel_range(self, index, volts);
    }

    fn set_channel_offset(&self, index: usize, offset: f32) {
        AppState::set_channel_offset(self, index, offset);
    }

    fn channel_bandwidth_limit(&self, index: usize) -> Option<u32> {
        AppState::channel_bandwidth_limit(self, index)
    }

    fn set_channel_bandwidth_limit(&self, index: usize, mhz: Option<u32>) {
        AppState::set_channel_bandwidth_limit(self, index, mhz);
    }

    fn awg_state(&self) -> AwgState {
        AppState::awg_state(self)
    }

    fn set_awg_enabled(&self, enabled: bool) {
        AppState::set_awg_enabled(self, enabled);
    }

    fn set_awg_frequency(&self, hz: f64) {
        AppState::set_awg_frequency(self, hz);
    }

    fn set_awg_duty(&self, duty: f32) {
        AppState::set_awg_duty(self, duty);
    }

    fn set_awg_range(&self, range_vpp: f32) {
        AppState::set_awg_range(self, range_vpp);
    }

    fn set_awg_offset(&self, offset_v: f32) {
        AppState::set_awg_offset(self, offset_v);
    }

    fn set_awg_shape(&self, shape: &str) {
        AppState::set_awg_shape(self, shape);
    }

    fn digital_hysteresis(&self, identifier: &str) -> f32 {
        AppState::digital_hysteresis(self, identifier)
    }

    fn set_digital_hysteresis(&self, identifier: &str, hysteresis: f32) {
        AppState::set_digital_hysteresis(self, identifier, hysteresis);
    }

    fn digital_threshold(&self, identifier: &str) -> f32 {
        AppState::digital_threshold(self, identifier)
    }

    fn set_digital_threshold(&self, identifier: &str, threshold: f32) {
        AppState::set_digital_threshold(self, identifier, threshold);
    }
}
