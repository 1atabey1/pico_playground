use parking_lot::Mutex;
use pico_common::{PicoCoupling, PicoRange};
use pico_scopehal::{
    parse_command,
    process_command,
    AwgState,
    ChannelState,
    CommandResult,
    DigitalChannelState,
    FS_PER_SECOND,
    ScpiState,
    TriggerEdge,
};
use std::collections::HashMap;

const TEST_RANGES: &[PicoRange] = &[
    PicoRange::X1_PROBE_20MV,
    PicoRange::X1_PROBE_50MV,
    PicoRange::X1_PROBE_100MV,
    PicoRange::X1_PROBE_200MV,
    PicoRange::X1_PROBE_500MV,
    PicoRange::X1_PROBE_1V,
    PicoRange::X1_PROBE_2V,
    PicoRange::X1_PROBE_5V,
    PicoRange::X1_PROBE_10V,
    PicoRange::X1_PROBE_20V,
];

struct TestState {
    idn: String,
    channels: usize,
    allowed_rates: Vec<u32>,
    allowed_depths: Vec<usize>,
    analog: Mutex<Vec<ChannelState>>,
    awg: Mutex<AwgState>,
    digital: Mutex<HashMap<String, DigitalChannelState>>,
    sample_rate: Mutex<u32>,
    sample_depth: Mutex<usize>,
    adc_bits: Mutex<u32>,
    trigger_source: Mutex<usize>,
    trigger_level: Mutex<f64>,
    trigger_delay: Mutex<i64>,
    trigger_edge: Mutex<TriggerEdge>,
    armed: Mutex<bool>,
    last_sequence: u32,
}

impl TestState {
    fn new() -> Self {
        let channels = 2;
        Self {
            idn: "pico_scopehal,MODEL123,SN123,0.1.0".into(),
            channels,
            allowed_rates: vec![1_000_000, 2_000_000],
            allowed_depths: vec![1_000, 2_000],
            analog: Mutex::new(vec![ChannelState::default(); channels]),
            awg: Mutex::new(AwgState::default()),
            digital: Mutex::new(HashMap::new()),
            sample_rate: Mutex::new(1_000_000),
            sample_depth: Mutex::new(1_000),
            adc_bits: Mutex::new(8),
            trigger_source: Mutex::new(0),
            trigger_level: Mutex::new(0.0),
            trigger_delay: Mutex::new(0),
            trigger_edge: Mutex::new(TriggerEdge::Rising),
            armed: Mutex::new(false),
            last_sequence: 7,
        }
    }

    fn rates_response(&self) -> String {
        self.allowed_rates
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
            .join(",")
    }

    fn depths_response(&self) -> String {
        self.allowed_depths
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn normalize_key(key: &str) -> String {
        key.trim().to_ascii_uppercase()
    }

    fn with_channel<F: FnOnce(&mut ChannelState)>(&self, index: usize, f: F) {
        if let Some(state) = self.analog.lock().get_mut(index) {
            f(state);
        }
    }

    fn select_range(volts: f64) -> PicoRange {
        let desired = volts.abs();
        for range in TEST_RANGES {
            if range.get_max_scaled_value() >= desired {
                return *range;
            }
        }
        PicoRange::X1_PROBE_20V
    }
}

impl ScpiState for TestState {
    fn idn(&self) -> String {
        self.idn.clone()
    }

    fn channel_count(&self) -> usize {
        self.channels
    }

    fn last_sequence(&self) -> u32 {
        self.last_sequence
    }

    fn allowed_sample_rates(&self) -> &[u32] {
        &self.allowed_rates
    }

    fn sample_rate(&self) -> u32 {
        *self.sample_rate.lock()
    }

    fn set_sample_rate(&self, rate: u32) -> anyhow::Result<()> {
        if !self.allowed_rates.contains(&rate) {
            anyhow::bail!("unsupported sample rate {rate}");
        }
        *self.sample_rate.lock() = rate;
        Ok(())
    }

    fn allowed_sample_depths(&self) -> &[usize] {
        &self.allowed_depths
    }

    fn sample_depth(&self) -> usize {
        *self.sample_depth.lock()
    }

    fn set_sample_depth(&self, depth: usize) -> anyhow::Result<()> {
        if !self.allowed_depths.contains(&depth) {
            anyhow::bail!("unsupported sample depth {depth}");
        }
        *self.sample_depth.lock() = depth;
        Ok(())
    }

    fn adc_bits(&self) -> u32 {
        *self.adc_bits.lock()
    }

    fn set_adc_bits(&self, bits: u32) -> anyhow::Result<()> {
        if !(8..=16).contains(&bits) {
            anyhow::bail!("unsupported ADC resolution {bits}");
        }
        *self.adc_bits.lock() = bits;
        Ok(())
    }

    fn start_streaming(&self, _one_shot: bool) -> anyhow::Result<u32> {
        *self.armed.lock() = true;
        Ok(*self.sample_rate.lock())
    }

    fn stop_streaming(&self) {
        *self.armed.lock() = false;
    }

    fn force_trigger(&self) {}

    fn is_armed(&self) -> bool {
        *self.armed.lock()
    }

    fn trigger_source(&self) -> usize {
        *self.trigger_source.lock()
    }

    fn set_trigger_source(&self, index: usize) {
        *self.trigger_source.lock() = index;
    }

    fn trigger_level(&self) -> f64 {
        *self.trigger_level.lock()
    }

    fn set_trigger_level(&self, level: f64) {
        *self.trigger_level.lock() = level;
    }

    fn trigger_delay(&self) -> i64 {
        *self.trigger_delay.lock()
    }

    fn set_trigger_delay(&self, delay: i64) {
        *self.trigger_delay.lock() = delay;
    }

    fn trigger_edge(&self) -> TriggerEdge {
        *self.trigger_edge.lock()
    }

    fn set_trigger_edge(&self, edge: TriggerEdge) {
        *self.trigger_edge.lock() = edge;
    }

    fn channel_state(&self, index: usize) -> ChannelState {
        self.analog.lock()[index].clone()
    }

    fn set_channel_enabled(&self, index: usize, enabled: bool) {
        self.with_channel(index, |state| state.enabled = enabled);
    }

    fn set_channel_coupling(&self, index: usize, coupling: PicoCoupling) {
        self.with_channel(index, |state| state.coupling = coupling);
    }

    fn set_channel_range(&self, index: usize, volts: f64) {
        let range = Self::select_range(volts);
        self.with_channel(index, |state| state.range = range);
    }

    fn set_channel_offset(&self, index: usize, offset: f32) {
        self.with_channel(index, |state| state.offset = offset);
    }

    fn channel_bandwidth_limit(&self, index: usize) -> Option<u32> {
        self.analog.lock()[index].bandwidth_limit_mhz
    }

    fn set_channel_bandwidth_limit(&self, index: usize, mhz: Option<u32>) {
        self.with_channel(index, |state| state.bandwidth_limit_mhz = mhz);
    }

    fn awg_state(&self) -> AwgState {
        self.awg.lock().clone()
    }

    fn set_awg_enabled(&self, enabled: bool) {
        self.awg.lock().enabled = enabled;
    }

    fn set_awg_frequency(&self, hz: f64) {
        self.awg.lock().frequency_hz = hz;
    }

    fn set_awg_duty(&self, duty: f32) {
        self.awg.lock().duty_cycle = duty;
    }

    fn set_awg_range(&self, range_vpp: f32) {
        self.awg.lock().range_vpp = range_vpp;
    }

    fn set_awg_offset(&self, offset_v: f32) {
        self.awg.lock().offset_v = offset_v;
    }

    fn set_awg_shape(&self, shape: &str) {
        self.awg.lock().shape = shape.to_ascii_uppercase();
    }

    fn digital_hysteresis(&self, identifier: &str) -> f32 {
        let key = Self::normalize_key(identifier);
        self.digital
            .lock()
            .get(&key)
            .map(|state| state.hysteresis_mv)
            .unwrap_or(0.0)
    }

    fn set_digital_hysteresis(&self, identifier: &str, hysteresis: f32) {
        let key = Self::normalize_key(identifier);
        let mut guard = self.digital.lock();
        let entry = guard.entry(key).or_default();
        entry.hysteresis_mv = hysteresis;
    }

    fn digital_threshold(&self, identifier: &str) -> f32 {
        let key = Self::normalize_key(identifier);
        self.digital
            .lock()
            .get(&key)
            .map(|state| state.threshold_mv)
            .unwrap_or(0.0)
    }

    fn set_digital_threshold(&self, identifier: &str, threshold: f32) {
        let key = Self::normalize_key(identifier);
        let mut guard = self.digital.lock();
        let entry = guard.entry(key).or_default();
        entry.threshold_mv = threshold;
    }
}

enum Expectation {
    Reply(String),
    NoReply,
    Exit,
}

impl Expectation {
    fn reply<S: Into<String>>(value: S) -> Self {
        Expectation::Reply(value.into())
    }
}

#[test]
fn scpi_command_set_matches_client_contract() {
    let state = TestState::new();
    let idn = state.idn.clone();
    let channels = state.channels.to_string();
    let seq = state.last_sequence.to_string();
    let rates = state.rates_response();
    let depths = state.depths_response();

    let scenarios = vec![
        ("*IDN?", Expectation::reply(idn)),
        ("CHANS?", Expectation::reply(channels)),
        ("SEQNUM?", Expectation::reply(seq)),
        ("RATES?", Expectation::reply(rates)),
        ("RATE 2000000", Expectation::NoReply),
        ("RATE?", Expectation::reply("2000000")),
        ("DEPTHS?", Expectation::reply(depths)),
        ("DEPTH 2000", Expectation::NoReply),
        ("DEPTH?", Expectation::reply("2000")),
        ("BITS?", Expectation::reply("8")),
        ("BITS 12", Expectation::NoReply),
        ("BITS?", Expectation::reply("12")),
        ("C1:ON", Expectation::NoReply),
        ("C1:STATE?", Expectation::reply("1")),
        ("C1:COUP AC1M", Expectation::NoReply),
        ("C1:COUP?", Expectation::reply("AC")),
        ("C1:RANGE 5", Expectation::NoReply),
        ("C1:RANGE?", Expectation::reply("5")),
        ("C1:OFFS 0.25", Expectation::NoReply),
        ("C1:OFFS?", Expectation::reply("0.250000")),
        ("C1:BWLIM 20", Expectation::NoReply),
        ("C1:BWLIM?", Expectation::reply("20")),
        ("1D:PRESENT?", Expectation::reply("0")),
        ("1D0:HYS 0.3", Expectation::NoReply),
        ("1D0:HYS?", Expectation::reply("0.300")),
        ("1D0:THRESH 0.4", Expectation::NoReply),
        ("1D0:THRESH?", Expectation::reply("0.400")),
        ("TRIG:SOU C2", Expectation::NoReply),
        ("TRIG:SOU?", Expectation::reply("C2")),
        ("TRIG:LEV 0.35", Expectation::NoReply),
        ("TRIG:LEV?", Expectation::reply("0.350000")),
        ("TRIG:DELAY 42", Expectation::NoReply),
        ("TRIG:DELAY?", Expectation::reply("42")),
        ("TRIG:EDGE:DIR ANY", Expectation::NoReply),
        ("TRIG:EDGE:DIR?", Expectation::reply("EITHER")),
        ("LEV 0.1", Expectation::NoReply),
        ("TRIG:LEV?", Expectation::reply("0.100000")),
        ("DELAY 84", Expectation::NoReply),
        ("TRIG:DELAY?", Expectation::reply("84")),
        ("EDGE:DIR FALLING", Expectation::NoReply),
        ("TRIG:EDGE:DIR?", Expectation::reply("FALLING")),
        ("AWG:RANGE 2.5", Expectation::NoReply),
        ("AWG:RANGE?", Expectation::reply("2.500")),
        ("AWG:FREQ 1000", Expectation::NoReply),
        ("AWG:FREQ?", Expectation::reply("1000.000000")),
        ("AWG:DUTY 45", Expectation::NoReply),
        ("AWG:DUTY?", Expectation::reply("45.000")),
        ("AWG:OFF 0.1", Expectation::NoReply),
        ("AWG:OFF?", Expectation::reply("0.100000")),
        ("AWG:SHAPE WHITENOISE", Expectation::NoReply),
        ("AWG:SHAPE?", Expectation::reply("WHITENOISE")),
        ("AWG:START", Expectation::NoReply),
        ("AWG:START?", Expectation::reply("1")),
        ("AWG:STOP", Expectation::NoReply),
        ("AWG:STOP?", Expectation::reply("0")),
        ("START", Expectation::NoReply),
        ("ARMED?", Expectation::reply("1")),
        ("STOP", Expectation::NoReply),
        ("ARMED?", Expectation::reply("0")),
        ("SINGLE", Expectation::NoReply),
        ("ARMED?", Expectation::reply("1")),
        ("FORCE", Expectation::NoReply),
        ("STOP", Expectation::NoReply),
        ("ARMED?", Expectation::reply("0")),
        ("EXIT", Expectation::Exit),
    ];

    for (raw, expected) in scenarios {
        let parsed = parse_command(raw).unwrap_or_else(|| panic!("failed to parse {raw}"));
        let result = process_command(&state, &parsed)
            .unwrap_or_else(|err| panic!("processing {raw}: {err}"));
        match expected {
            Expectation::Reply(expected_value) => match result {
                CommandResult::Reply(actual) => {
                    assert_eq!(expected_value, actual, "command {raw}")
                }
                other => panic!(
                    "command {raw} expected reply {expected_value}, got {variant}",
                    variant = match other {
                        CommandResult::NoReply => "NoReply",
                        CommandResult::Reply(_) => "Reply",
                        CommandResult::Exit => "Exit",
                    }
                ),
            },
            Expectation::NoReply => match result {
                CommandResult::NoReply => {}
                other => panic!(
                    "command {raw} expected no reply, got {variant}",
                    variant = match other {
                        CommandResult::NoReply => "NoReply",
                        CommandResult::Reply(_) => "Reply",
                        CommandResult::Exit => "Exit",
                    }
                ),
            },
            Expectation::Exit => assert!(
                matches!(result, CommandResult::Exit),
                "command {raw} expected exit"
            ),
        }
    }
}
