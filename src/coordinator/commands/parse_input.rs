use crate::prelude::*;

/// Parse and decode an input register value according to Table 7 of the protocol specification
pub fn parse_input_register(reg: u16, value: u16) -> String {
    match reg {
        // System Status (0-39)
        0 => format!("Register {} - Inverter Status: {}", reg, match value {
            0 => "Standby - Waiting",
            1 => "Self-Test",
            2 => "Normal Operation",
            3 => "Alarm",
            4 => "Fault",
            _ => "Unknown Status"
        }),
        1 => format!("Register {} - PV1 Voltage (Vpv1): {:.1} V", reg, (value as f64) / 10.0),
        2 => format!("Register {} - PV2 Voltage (Vpv2): {:.1} V", reg, (value as f64) / 10.0),
        3 => format!("Register {} - PV3 Voltage (Vpv3): {:.1} V", reg, (value as f64) / 10.0),
        4 => format!("Register {} - Battery Voltage (Vbat): {:.1} V", reg, (value as f64) / 10.0),
        5 => format!("Register {} - State of Charge (SOC): {}%", reg, value.min(100)),
        6 => format!("Register {} - Internal Fault: {:#06x} (See Internal DTC Definitions)", reg, value),
        7 => format!("Register {} - PV1 Power (Ppv1): {} W", reg, value),
        8 => format!("Register {} - PV2 Power (Ppv2): {} W", reg, value),
        9 => format!("Register {} - PV3 Power (Ppv3): {} W", reg, value),
        10 => format!("Register {} - Charge Power (Pcharge): {} W (incoming battery power)", reg, value),
        11 => format!("Register {} - Discharge Power (Pdischarge): {} W (outflow battery power)", reg, value),
        12 => format!("Register {} - R-phase Mains Voltage (VacR): {:.1} V", reg, (value as f64) / 10.0),
        13 => format!("Register {} - S-phase Mains Voltage (VacS): {:.1} V", reg, (value as f64) / 10.0),
        14 => format!("Register {} - T-phase Mains Voltage (VacT): {:.1} V", reg, (value as f64) / 10.0),
        15 => format!("Register {} - Mains Frequency (Fac): {:.2} Hz", reg, (value as f64) / 100.0),
        16 => format!("Register {} - Inverter Output Power (Pinv): {} W (Grid port)", reg, value),
        17 => format!("Register {} - AC Charging Rectified Power (Prec): {} W", reg, value),
        18 => format!("Register {} - Inverter Current RMS (IinvRMS): {:.2} A", reg, (value as f64) / 100.0),
        19 => {
            let pf = if value <= 1000 {
                value as f64 / 1000.0
            } else {
                (2000 - value) as f64 / 1000.0
            };
            format!("Register {} - Power Factor (PF): {:.3}", reg, pf)
        },
        20 => format!("Register {} - R-phase Off-grid Output Voltage (VepsR): {:.1} V", reg, (value as f64) / 10.0),
        21 => format!("Register {} - S-phase Off-grid Output Voltage (VepsS): {:.1} V", reg, (value as f64) / 10.0),
        22 => format!("Register {} - T-phase Off-grid Output Voltage (VepsT): {:.1} V", reg, (value as f64) / 10.0),
        23 => format!("Register {} - Off-grid Output Frequency (Feps): {:.2} Hz", reg, (value as f64) / 100.0),
        24 => format!("Register {} - Off-grid Inverter Power (Peps): {} W", reg, value),
        25 => format!("Register {} - Off-grid Apparent Power (Seps): {} VA", reg, value),
        26 => format!("Register {} - Export Power to Grid (Ptogrid): {} W", reg, value),
        27 => format!("Register {} - Import Power from Grid (Ptouser): {} W", reg, value),
        28 => format!("Register {} - PV1 Power Generation Today (Epv1_day): {:.1} kWh", reg, (value as f64) / 10.0),
        29 => format!("Register {} - PV2 Power Generation Today (Epv2_day): {:.1} kWh", reg, (value as f64) / 10.0),
        30 => format!("Register {} - PV3 Power Generation Today (Epv3_day): {:.1} kWh", reg, (value as f64) / 10.0),
        31 => format!("Register {} - Grid-connected Inverter Output Energy Today (Einv_day): {:.1} kWh", reg, (value as f64) / 10.0),
        32 => format!("Register {} - AC Charging Rectified Energy Today (Erec_day): {:.1} kWh", reg, (value as f64) / 10.0),
        33 => format!("Register {} - Charged Energy Today (Echg_day): {:.1} kWh", reg, (value as f64) / 10.0),
        34 => format!("Register {} - Discharged Energy Today (Edischg_day): {:.1} kWh", reg, (value as f64) / 10.0),
        35 => format!("Register {} - Off-grid Output Energy Today (Eeps_day): {:.1} kWh", reg, (value as f64) / 10.0),
        36 => format!("Register {} - Export Energy to Grid Today (Etogrid_day): {:.1} kWh", reg, (value as f64) / 10.0),
        37 => format!("Register {} - Import Energy from Grid Today (Etouser_day): {:.1} kWh", reg, (value as f64) / 10.0),
        38 => format!("Register {} - Bus 1 Voltage (Vbus1): {:.1} V", reg, (value as f64) / 10.0),
        39 => format!("Register {} - Bus 2 Voltage (Vbus2): {:.1} V", reg, (value as f64) / 10.0),

        // Cumulative Energy Statistics (40-59)
        40..=59 => {
            let desc = match reg {
                40 => "PV1 Cumulative Power Generation Low Word (Epv1_all L)",
                41 => "PV1 Cumulative Power Generation High Word (Epv1_all H)",
                42 => "PV2 Cumulative Power Generation Low Word (Epv2_all L)",
                43 => "PV2 Cumulative Power Generation High Word (Epv2_all H)",
                44 => "PV3 Cumulative Power Generation Low Word (Epv3_all L)",
                45 => "PV3 Cumulative Power Generation High Word (Epv3_all H)",
                46 => "Inverter Cumulative Output Energy Low Word (Einv_all L)",
                47 => "Inverter Cumulative Output Energy High Word (Einv_all H)",
                48 => "AC Charging Cumulative Rectified Energy Low Word (Erec_all L)",
                49 => "AC Charging Cumulative Rectified Energy High Word (Erec_all H)",
                50 => "Cumulative Charge Energy Low Word (Echg_all L)",
                51 => "Cumulative Charge Energy High Word (Echg_all H)",
                52 => "Cumulative Discharge Energy Low Word (Edischg_all L)",
                53 => "Cumulative Discharge Energy High Word (Edischg_all H)",
                54 => "Cumulative Off-grid Inverter Power Low Word (Eeps_all L)",
                55 => "Cumulative Off-grid Inverter Power High Word (Eeps_all H)",
                56 => "Cumulative Export Energy to Grid Low Word (Etogrid_all L)",
                57 => "Cumulative Export Energy to Grid High Word (Etogrid_all H)",
                58 => "Cumulative Import Energy from Grid Low Word (Etouser_all L)",
                59 => "Cumulative Import Energy from Grid High Word (Etouser_all H)",
                _ => "Unknown Cumulative Energy Register"
            };
            format!("Register {} - {}: {:.1} kWh", reg, desc, (value as f64) / 10.0)
        },

        // System Status and Temperature (60-67)
        60 => format!("Fault Code Low Word (FaultCode L): {:#06x}", value),
        61 => format!("Fault Code High Word (FaultCode H): {:#06x}", value),
        62 => format!("Warning Code Low Word (WarningCode L): {:#06x}", value),
        63 => format!("Warning Code High Word (WarningCode H): {:#06x}", value),
        64 => format!("Internal Ring Temperature (Tinner): {} °C", value as i16),
        65 => format!("Radiator Temperature 1 (Tradiator1): {} °C", value as i16),
        66 => format!("Radiator Temperature 2 (Tradiator2): {} °C", value as i16),
        67 => format!("Battery Temperature (Tbat): {} °C", value as i16),

        // Runtime and AutoTest Status (68-75)
        68 => format!("Reserved Register {}", reg),
        69 => format!("Runtime Low Word (RunningTime L): {} seconds", value),
        70 => format!("Runtime High Word (RunningTime H): {} seconds", value),
        71 => {
            let auto_test_start = value & 0x0F;
            let auto_test_status = (value >> 4) & 0x0F;
            let auto_test_step = (value >> 8) & 0x0F;
            format!("AutoTest Status:\n  Start: {}\n  Status: {}\n  Step: {}\n  Raw: {:#06x}",
                if auto_test_start == 1 { "Activated" } else { "Not Activated" },
                match auto_test_status {
                    0 => "Waiting",
                    1 => "Testing",
                    2 => "Test Failed",
                    3 => "V Test OK",
                    4 => "F Test OK",
                    5 => "Test Passed",
                    _ => "Unknown"
                },
                match auto_test_step {
                    1 => "V1L Test",
                    2 => "V1H Test",
                    3 => "F1L Test",
                    4 => "F1H Test",
                    5 => "V2L Test",
                    6 => "V2H Test",
                    7 => "F2L Test",
                    8 => "F2H Test",
                    _ => "Unknown"
                },
                value
            )
        },
        72 => {
            let unit = if (71..=72).contains(&value) { "V" } else { "Hz" };
            let val = if unit == "V" {
                (value as f64) / 10.0
            } else {
                (value as f64) / 100.0
            };
            format!("AutoTest Limit (wAutoTestLimit): {:.1} {}", val, unit)
        },
        73 => format!("AutoTest Default Time (uwAutoTestDefaultTime): {} ms", value),
        74 => {
            let unit = if (71..=72).contains(&value) { "V" } else { "Hz" };
            let val = if unit == "V" {
                (value as f64) / 10.0
            } else {
                (value as f64) / 100.0
            };
            format!("AutoTest Trip Value (uwAutoTestTripValue): {:.1} {}", val, unit)
        },
        75 => format!("AutoTest Trip Time (uwAutoTestTripTime): {} ms", value),

        // AC Input and Reserved (76-80)
        77 => format!("Register {} - AC Input Type: {}", reg, if value == 0 { "Grid" } else { "Generator (12K Hybrid)" }),
        76 | 78 | 79 | 80 => format!("Register {} - Reserved Register", reg),

        // BMS Data (81-112)
        81 => format!("Register {} - BMS Max Charging Current (MaxChgCurr): {:.2} A", reg, (value as f64) / 100.0),
        82 => format!("Register {} - BMS Max Discharge Current (MaxDischgCurr): {:.2} A", reg, (value as f64) / 100.0),
        83 => format!("Register {} - BMS Recommended Charging Voltage (ChargeVoltRef): {:.1} V", reg, (value as f64) / 10.0),
        84 => format!("Register {} - BMS Recommended Discharge Cut-off Voltage (DischgCutVolt): {:.1} V", reg, (value as f64) / 10.0),
        85..=94 => format!("Register {} - BMS Status {} (BatStatus{}_BMS): {:#06x}", reg, reg - 85, reg - 85, value),
        95 => format!("Register {} - Inverter Battery Status (BatStatus_INV): {:#06x}", reg, value),
        96 => format!("Register {} - Number of Batteries in Parallel (BatParallelNum): {}", reg, value),
        97 => format!("Register {} - Battery Capacity (BatCapacity): {} Ah", reg, value),
        98 => format!("Register {} - BMS Battery Current (BatCurrent_BMS): {:.2} A", reg, (value as i16 as f64) / 100.0),
        99 => format!("Register {} - BMS Fault Code (FaultCode_BMS): {:#06x}", reg, value),
        100 => format!("Register {} - BMS Warning Code (WarningCode_BMS): {:#06x}", reg, value),
        101 => format!("Register {} - BMS Maximum Cell Voltage (MaxCellVolt_BMS): {:.3} V", reg, (value as f64) / 1000.0),
        102 => format!("Register {} - BMS Minimum Cell Voltage (MinCellVolt_BMS): {:.3} V", reg, (value as f64) / 1000.0),
        103 => format!("Register {} - BMS Maximum Cell Temperature (MaxCellTemp_BMS): {:.1} °C", reg, (value as i16 as f64) / 10.0),
        104 => format!("Register {} - BMS Minimum Cell Temperature (MinCellTemp_BMS): {:.1} °C", reg, (value as i16 as f64) / 10.0),
        105 => format!("Register {} - BMS Firmware Update State (BMSFWUpdateState): {}", reg, match value {
            1 => "Upgrading",
            2 => "Upgrade Successful",
            3 => "Upgrade Failed",
            _ => "Unknown"
        }),
        106 => format!("Register {} - BMS Cycle Count (CycleCnt_BMS): {}", reg, value),
        107 => format!("Register {} - Inverter Battery Voltage Sample (BatVoltSample_INV): {:.1} V", reg, (value as f64) / 10.0),
        108 => format!("Register {} - 12K BT Temperature (T1): {:.1} °C", reg, (value as f64) / 10.0),
        109..=112 => format!("Register {} - Reserved Temperature {} (T{}): {:.1} °C", reg, reg - 108, reg - 107, (value as f64) / 10.0),

        // Parallel System Status (113-119)
        113 => {
            let master_slave = value & 0x03;
            let phase = (value >> 2) & 0x03;
            let parallel_num = (value >> 8) & 0xFF;
            format!("System Configuration:\n  Role: {} (MasterOrSlave)\n  Phase: {} (SingleOrThreePhase)\n  Parallel Units: {} (ParallelNum)\n  Raw: {:#06x}",
                match master_slave {
                    1 => "Master",
                    2 => "Slave",
                    _ => "Unknown"
                },
                match phase {
                    1 => "R",
                    2 => "S",
                    3 => "T",
                    _ => "Unknown"
                },
                parallel_num,
                value
            )
        },
        114..=119 => format!("Reserved Register {}", reg),

        // Generator and EPS Data (120-138)
        120 => format!("Half BUS Voltage (VBusP): {:.1} V", (value as f64) / 10.0),
        121 => format!("Generator Voltage (GenVolt): {:.1} V", (value as f64) / 10.0),
        122 => format!("Generator Frequency (GenFreq): {:.2} Hz", (value as f64) / 100.0),
        123 => format!("Generator Power (GenPower): {} W", value),
        124 => format!("Generator Energy Today (Egen_day): {:.1} kWh", (value as f64) / 10.0),
        125 => format!("Generator Total Energy Low Word (Egen_all L): {:.1} kWh", (value as f64) / 10.0),
        126 => format!("Generator Total Energy High Word (Egen_all H): {:.1} kWh", (value as f64) / 10.0),
        127 => format!("EPS L1N Voltage (EPSVoltL1N): {:.1} V", (value as f64) / 10.0),
        128 => format!("EPS L2N Voltage (EPSVoltL2N): {:.1} V", (value as f64) / 10.0),
        129 => format!("EPS L1N Active Power (Peps_L1N): {} W", value),
        130 => format!("EPS L2N Active Power (Peps_L2N): {} W", value),
        131 => format!("EPS L1N Apparent Power (Seps_L1N): {} VA", value),
        132 => format!("EPS L2N Apparent Power (Seps_L2N): {} VA", value),
        133 => format!("EPS L1N Energy Today (EepsL1N_day): {:.1} kWh", (value as f64) / 10.0),
        134 => format!("EPS L2N Energy Today (EepsL2N_day): {:.1} kWh", (value as f64) / 10.0),
        135 => format!("EPS L1N Total Energy Low Word (EepsL1N_all L): {:.1} kWh", (value as f64) / 10.0),
        136 => format!("EPS L1N Total Energy High Word (EepsL1N_all H): {:.1} kWh", (value as f64) / 10.0),
        137 => format!("EPS L2N Total Energy Low Word (EepsL2N_all L): {:.1} kWh", (value as f64) / 10.0),
        138 => format!("EPS L2N Total Energy High Word (EepsL2N_all H): {:.1} kWh", (value as f64) / 10.0),

        // AFCI Data (139-152)
        139 => format!("Register {} - AFCI Self-Test Status: {}", reg, match value {
            0 => "Not Activated",
            1 => "Activated",
            _ => "Unknown"
        }),
        140..=143 => format!("Register {} - AFCI Current CH{} (AFCI_CurrCH{}): {} mA", reg, reg - 139, reg - 139, value),
        144 => {
            let mut flags = Vec::new();
            for i in 0..4 {
                if value & (1 << i) != 0 {
                    flags.push(format!("Arc Alarm CH{}", i + 1));
                }
                if value & (1 << (i + 4)) != 0 {
                    flags.push(format!("Self-Test Failed CH{}", i + 1));
                }
            }
            format!("Register {} - AFCI Status Flags: {:#06x}\nActive Flags: {}", 
                reg,
                value,
                if flags.is_empty() { "None".to_string() } else { flags.join(", ") }
            )
        },
        145..=148 => format!("Register {} - AFCI Max Arc CH{} (AFCI_MaxArcCH{}): {} mA", reg, reg - 144, reg - 144, value),
        149..=152 => format!("Register {} - Reserved AFCI Register", reg),
        244 => format!("Register {} - 12K_BOOT_LOADER_VERSION: {}", reg, value),
        245 => format!("Register {} - 12K_CHIP_FLASH_SIZE: {}", reg, value),
        252 => format!("Register {} - 12K_BUS_BAR_CURRENT: (): {} Amps", reg, value),
        253 => format!("Register {} - 12K_HOLD_SOC_HYSTERESIS: {}", reg, value),

        // Default case for unknown registers
        _ => format!("Register {} - Unknown input register: {}", reg, value),
    }
} 
