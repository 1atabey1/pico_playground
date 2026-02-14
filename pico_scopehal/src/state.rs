use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use parking_lot::RwLock;
use pico_common::{PicoChannel, PicoCoupling, PicoRange};
use pico_streaming::PicoStreamingDevice;
use tracing::info;

const DEFAULT_SAMPLE_RATES: &[u32] = &[
    1_000_000_000,
    500_000_000,
    250_000_000,
    125_000_000,
    62_500_000,
    31_250_000,
    15_625_000,
    7_812_500,
    3_906_250,
    1_953_125,
    976_562,
    488_281,
    244_140,
    122_070,
    61_035,
    30_517,
    15_258,
    7_629,
    3_814,
    1_907,
];

const DEFAULT_MEMORY_DEPTHS: &[usize] = &[
    1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000,
    5_000_000, 10_000_000,
];

const SUPPORTED_RANGES: &[PicoRange] = &[
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

pub(crate) const FS_PER_SECOND: i64 = 1_000_000_000_000_000;
const SERVER_NAME: &str = "pico-scopehal-rs";

#[derive(Clone)]
pub(crate) struct DeviceMetadata {
    pub(crate) serial: String,
    pub(crate) variant: String,
    #[allow(dead_code)]
    pub(crate) usb_version: String,
}

#[derive(Clone)]
pub(crate) struct ChannelState {
    pub(crate) enabled: bool,
    pub(crate) coupling: PicoCoupling,
    pub(crate) range: PicoRange,
    pub(crate) offset: f32,
    pub(crate) bandwidth_limit_mhz: Option<u32>,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            enabled: false,
            coupling: PicoCoupling::DC,
            range: PicoRange::X1_PROBE_2V,
            offset: 0.0,
            bandwidth_limit_mhz: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AwgState {
    pub(crate) enabled: bool,
    pub(crate) frequency_hz: f64,
    pub(crate) duty_cycle: f32,
    pub(crate) range_vpp: f32,
    pub(crate) offset_v: f32,
    pub(crate) shape: String,
}

impl Default for AwgState {
    fn default() -> Self {
        Self {
            enabled: false,
            frequency_hz: 1_000.0,
            duty_cycle: 50.0,
            range_vpp: 1.0,
            offset_v: 0.0,
            shape: "SINE".into(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct DigitalChannelState {
    pub(crate) threshold_mv: f32,
    pub(crate) hysteresis_mv: f32,
}

#[derive(Clone, Copy)]
pub(crate) enum TriggerEdge {
    Rising,
    Falling,
    Either,
}

impl TriggerEdge {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TriggerEdge::Rising => "RISING",
            TriggerEdge::Falling => "FALLING",
            TriggerEdge::Either => "EITHER",
        }
    }
}

#[derive(Clone)]
pub(crate) struct AcquisitionState {
    sample_rate: u32,
    sample_depth: usize,
    trigger_source: usize,
    trigger_level: f64,
    trigger_delay_fs: i64,
    trigger_edge: TriggerEdge,
    one_shot: bool,
    armed: bool,
    actual_sample_rate: u32,
}

impl Default for AcquisitionState {
    fn default() -> Self {
        Self {
            sample_rate: DEFAULT_SAMPLE_RATES[0],
            sample_depth: DEFAULT_MEMORY_DEPTHS[DEFAULT_MEMORY_DEPTHS.len() / 2],
            trigger_source: 0,
            trigger_level: 0.0,
            trigger_delay_fs: 0,
            trigger_edge: TriggerEdge::Rising,
            one_shot: false,
            armed: false,
            actual_sample_rate: 0,
        }
    }
}

pub(crate) struct AppState {
    device: Arc<PicoStreamingDevice>,
    pub(crate) analog_channels: Vec<PicoChannel>,
    #[allow(dead_code)]
    pub(crate) max_adc: i16,
    pub(crate) metadata: DeviceMetadata,
    channels: RwLock<Vec<ChannelState>>,
    acquisition: RwLock<AcquisitionState>,
    awg: RwLock<AwgState>,
    digital: RwLock<HashMap<String, DigitalChannelState>>,
    adc_bits: AtomicU32,
    last_sequence: AtomicU32,
}

impl AppState {
    pub(crate) fn new(
        device: PicoStreamingDevice,
        analog_channels: Vec<PicoChannel>,
        max_adc: i16,
        metadata: DeviceMetadata,
    ) -> Self {
        let channel_states = analog_channels
            .iter()
            .map(|_| ChannelState::default())
            .collect();
        Self {
            device: Arc::new(device),
            analog_channels,
            max_adc,
            metadata,
            channels: RwLock::new(channel_states),
            acquisition: RwLock::new(AcquisitionState::default()),
            awg: RwLock::new(AwgState::default()),
            digital: RwLock::new(HashMap::new()),
            adc_bits: AtomicU32::new(8),
            last_sequence: AtomicU32::new(0),
        }
    }

    pub(crate) fn device(&self) -> Arc<PicoStreamingDevice> {
        Arc::clone(&self.device)
    }

    pub(crate) fn idn(&self) -> String {
        format!(
            "{},{},{},{}",
            SERVER_NAME,
            self.metadata.variant,
            self.metadata.serial,
            env!("CARGO_PKG_VERSION")
        )
    }

    pub(crate) fn channel_count(&self) -> usize {
        self.analog_channels.len()
    }

    pub(crate) fn sample_interval_fs(&self) -> i64 {
        let acq = self.acquisition.read();
        let rate = if acq.actual_sample_rate > 0 {
            acq.actual_sample_rate
        } else {
            acq.sample_rate
        };
        if rate == 0 {
            return 0;
        }
        (FS_PER_SECOND / rate as i64).max(1)
    }

    pub(crate) fn allowed_sample_rates(&self) -> &'static [u32] {
        DEFAULT_SAMPLE_RATES
    }

    pub(crate) fn allowed_sample_depths(&self) -> &'static [usize] {
        DEFAULT_MEMORY_DEPTHS
    }

    pub(crate) fn sample_rate(&self) -> u32 {
        self.acquisition.read().sample_rate
    }

    pub(crate) fn sample_depth(&self) -> usize {
        self.acquisition.read().sample_depth
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.acquisition.read().armed
    }

    pub(crate) fn set_sample_rate(&self, rate: u32) -> Result<()> {
        if !self.allowed_sample_rates().contains(&rate) {
            bail!("unsupported sample rate {rate}");
        }
        self.acquisition.write().sample_rate = rate;
        Ok(())
    }

    pub(crate) fn set_sample_depth(&self, depth: usize) -> Result<()> {
        if !self.allowed_sample_depths().contains(&depth) {
            bail!("unsupported sample depth {depth}");
        }
        self.acquisition.write().sample_depth = depth;
        Ok(())
    }

    pub(crate) fn start_streaming(&self, one_shot: bool) -> Result<u32> {
        let rate = self.sample_rate();
        let actual = self
            .device
            .start(rate)
            .with_context(|| format!("failed to start streaming at {} Sa/s", rate))?;
        let mut acq = self.acquisition.write();
        acq.one_shot = one_shot;
        acq.armed = true;
        acq.actual_sample_rate = actual;
        info!(requested = rate, actual, one_shot, "streaming started");
        Ok(actual)
    }

    pub(crate) fn stop_streaming(&self) {
        self.device.stop();
        let mut acq = self.acquisition.write();
        acq.armed = false;
        acq.one_shot = false;
        info!("streaming stopped");
    }

    pub(crate) fn handle_waveform_complete(&self) {
        let mut acq = self.acquisition.write();
        if acq.one_shot {
            info!("single-shot capture completed");
            acq.armed = false;
            acq.one_shot = false;
            self.device.stop();
        }
    }

    pub(crate) fn force_trigger(&self) {
        info!("force trigger requested (software noop)");
    }

    pub(crate) fn channel_state(&self, index: usize) -> ChannelState {
        self.channels.read()[index].clone()
    }

    #[allow(dead_code)]
    pub(crate) fn channel_label(&self, index: usize) -> String {
        char::from_u32('A' as u32 + index as u32)
            .map(|c| format!("C{}", (c as u8 - b'A') + 1))
            .unwrap_or_else(|| format!("C{}", index + 1))
    }

    pub(crate) fn sync_channel(&self, index: usize) {
        let state = self.channel_state(index);
        if let Some(channel) = self.analog_channels.get(index).copied() {
            if state.enabled {
                self.device
                    .enable_channel(channel, state.range, state.coupling);
            } else {
                self.device.disable_channel(channel);
            }
        }
    }

    pub(crate) fn sync_all_channels(&self) {
        self.analog_channels
            .iter()
            .enumerate()
            .for_each(|(idx, _)| self.sync_channel(idx));
    }

    pub(crate) fn set_channel_enabled(&self, index: usize, enabled: bool) {
        if let Some(state) = self.channels.write().get_mut(index) {
            state.enabled = enabled;
        }
        self.sync_channel(index);
    }

    pub(crate) fn set_channel_coupling(&self, index: usize, coupling: PicoCoupling) {
        if let Some(state) = self.channels.write().get_mut(index) {
            state.coupling = coupling;
        }
        self.sync_channel(index);
    }

    pub(crate) fn set_channel_range(&self, index: usize, volts: f64) {
        let desired = pick_range(volts);
        if let Some(state) = self.channels.write().get_mut(index) {
            state.range = desired;
        }
        self.sync_channel(index);
    }

    pub(crate) fn set_channel_offset(&self, index: usize, offset: f32) {
        if let Some(state) = self.channels.write().get_mut(index) {
            state.offset = offset;
        }
    }

    pub(crate) fn channel_bandwidth_limit(&self, index: usize) -> Option<u32> {
        self.channels
            .read()
            .get(index)
            .and_then(|c| c.bandwidth_limit_mhz)
    }

    pub(crate) fn set_channel_bandwidth_limit(&self, index: usize, limit_mhz: Option<u32>) {
        if let Some(state) = self.channels.write().get_mut(index) {
            state.bandwidth_limit_mhz = limit_mhz;
        }
    }

    pub(crate) fn set_trigger_source(&self, index: usize) {
        self.acquisition.write().trigger_source = index;
    }

    pub(crate) fn trigger_source(&self) -> usize {
        self.acquisition.read().trigger_source
    }

    pub(crate) fn set_trigger_level(&self, level: f64) {
        self.acquisition.write().trigger_level = level;
    }

    pub(crate) fn trigger_level(&self) -> f64 {
        self.acquisition.read().trigger_level
    }

    pub(crate) fn set_trigger_delay(&self, delay_fs: i64) {
        self.acquisition.write().trigger_delay_fs = delay_fs;
    }

    pub(crate) fn trigger_delay(&self) -> i64 {
        self.acquisition.read().trigger_delay_fs
    }

    pub(crate) fn set_trigger_edge(&self, edge: TriggerEdge) {
        self.acquisition.write().trigger_edge = edge;
    }

    pub(crate) fn trigger_edge(&self) -> TriggerEdge {
        self.acquisition.read().trigger_edge
    }

    pub(crate) fn adc_bits(&self) -> u32 {
        self.adc_bits.load(Ordering::Relaxed)
    }

    pub(crate) fn set_adc_bits(&self, bits: u32) -> Result<()> {
        if !(8..=16).contains(&bits) {
            bail!("unsupported ADC resolution {bits}");
        }
        self.adc_bits.store(bits, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn awg_state(&self) -> AwgState {
        self.awg.read().clone()
    }

    pub(crate) fn set_awg_enabled(&self, enabled: bool) {
        self.awg.write().enabled = enabled;
    }

    pub(crate) fn set_awg_frequency(&self, hz: f64) {
        self.awg.write().frequency_hz = hz.max(0.0);
    }

    pub(crate) fn set_awg_duty(&self, duty: f32) {
        let duty = duty.clamp(0.0, 100.0);
        self.awg.write().duty_cycle = duty;
    }

    pub(crate) fn set_awg_range(&self, range_vpp: f32) {
        self.awg.write().range_vpp = range_vpp.max(0.0);
    }

    pub(crate) fn set_awg_offset(&self, offset_v: f32) {
        self.awg.write().offset_v = offset_v;
    }

    pub(crate) fn set_awg_shape(&self, shape: &str) {
        self.awg.write().shape = shape.to_string();
    }

    pub(crate) fn update_sequence(&self, sequence: u32) {
        self.last_sequence.store(sequence, Ordering::Relaxed);
    }

    pub(crate) fn last_sequence(&self) -> u32 {
        self.last_sequence.load(Ordering::Relaxed)
    }

    pub(crate) fn digital_threshold(&self, key: &str) -> f32 {
        self.digital
            .read()
            .get(&normalize_key(key))
            .map(|d| d.threshold_mv)
            .unwrap_or(0.0)
    }

    pub(crate) fn set_digital_threshold(&self, key: &str, threshold_mv: f32) {
        let mut guard = self.digital.write();
        let entry = guard.entry(normalize_key(key)).or_default();
        entry.threshold_mv = threshold_mv;
    }

    pub(crate) fn digital_hysteresis(&self, key: &str) -> f32 {
        self.digital
            .read()
            .get(&normalize_key(key))
            .map(|d| d.hysteresis_mv)
            .unwrap_or(0.0)
    }

    pub(crate) fn set_digital_hysteresis(&self, key: &str, hysteresis_mv: f32) {
        let mut guard = self.digital.write();
        let entry = guard.entry(normalize_key(key)).or_default();
        entry.hysteresis_mv = hysteresis_mv;
    }
}

fn pick_range(volts: f64) -> PicoRange {
    let desired = volts.abs();
    for range in SUPPORTED_RANGES {
        if range.get_max_scaled_value() >= desired {
            return *range;
        }
    }
    PicoRange::X1_PROBE_20V
}

fn normalize_key(key: &str) -> String {
    key.trim().to_ascii_uppercase()
}
