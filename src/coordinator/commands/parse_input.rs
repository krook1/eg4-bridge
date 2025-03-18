use crate::prelude::*;

/// Parse and decode an input register value according to Table 7 of the protocol specification
pub fn parse_input_register(reg: u16, value: u16) -> String {
    match reg {
        // System Status (0-39)
        0 => format!("Inverter Status: {}", match value {
            0 => "Standby - Waiting",
            1 => "Self-Test",
            2 => "Normal Operation",
            3 => "Alarm",
            4 => "Fault",
            _ => "Unknown Status"
        }),
        1 => format!("PV1 Voltage: {:.1} V", (value as f64) / 10.0),
        2 => format!("PV2 Voltage: {:.1} V", (value as f64) / 10.0),
        3 => format!("PV3 Voltage: {:.1} V", (value as f64) / 10.0),
        4 => format!("Battery Voltage: {:.1} V", (value as f64) / 10.0),
        5 => format!("Battery SOH: {}%", value),
        6 => format!("Internal Fault: {:#06x}", value),
        7 => format!("PV1 Power: {} W", value),
        8 => format!("PV2 Power: {} W", value),
        9 => format!("PV3 Power: {} W", value),
        10 => format!("Charge Power: {} W (incoming battery power)", value),
        11 => format!("Discharge Power: {} W (outflow battery power)", value),
        12 => format!("R-phase Mains Voltage: {:.1} V", (value as f64) / 10.0),
        13 => format!("S-phase Mains Voltage: {:.1} V", (value as f64) / 10.0),
        14 => format!("T-phase Mains Voltage: {:.1} V", (value as f64) / 10.0),
        15 => format!("Mains Frequency: {:.2} Hz", (value as f64) / 100.0),
        16 => format!("Inverter Output Power: {} W (Grid port)", value),
        17 => format!("AC Charging Rectified Power: {} W", value),
        18 => format!("Inverter Current RMS: {:.2} A", (value as f64) / 100.0),
        19 => {
            let pf = if value <= 1000 {
                value as f64 / 1000.0
            } else {
                (2000 - value) as f64 / 1000.0
            };
            format!("Power Factor: {:.3}", pf)
        },
        20 => format!("R-phase Off-grid Output Voltage: {:.1} V", (value as f64) / 10.0),
        21 => format!("S-phase Off-grid Output Voltage: {:.1} V", (value as f64) / 10.0),
        22 => format!("T-phase Off-grid Output Voltage: {:.1} V", (value as f64) / 10.0),
        23 => format!("Off-grid Output Frequency: {:.2} Hz", (value as f64) / 100.0),
        24 => format!("Off-grid Inverter Power: {} W", value),
        25 => format!("Off-grid Apparent Power: {} VA", value),
        26 => format!("Export Power to Grid: {} W", value),
        27 => format!("Import Power from Grid: {} W", value),
        28 => format!("PV1 Power Generation Today: {:.1} kWh", (value as f64) / 10.0),
        29 => format!("PV2 Power Generation Today: {:.1} kWh", (value as f64) / 10.0),
        30 => format!("PV3 Power Generation Today: {:.1} kWh", (value as f64) / 10.0),
        31 => format!("Grid-connected Inverter Output Energy Today: {:.1} kWh", (value as f64) / 10.0),
        32 => format!("AC Charging Rectified Energy Today: {:.1} kWh", (value as f64) / 10.0),
        33 => format!("Charged Energy Today: {:.1} kWh", (value as f64) / 10.0),
        34 => format!("Discharged Energy Today: {:.1} kWh", (value as f64) / 10.0),
        35 => format!("Off-grid Output Energy Today: {:.1} kWh", (value as f64) / 10.0),
        36 => format!("Export Energy to Grid Today: {:.1} kWh", (value as f64) / 10.0),
        37 => format!("Import Energy from Grid Today: {:.1} kWh", (value as f64) / 10.0),
        38 => format!("Bus 1 Voltage: {:.1} V", (value as f64) / 10.0),
        39 => format!("Bus 2 Voltage: {:.1} V", (value as f64) / 10.0),

        // Cumulative Energy Statistics (40-59)
        40..=59 => {
            let desc = match reg {
                40 => "PV1 Cumulative Power Generation Low Word",
                41 => "PV1 Cumulative Power Generation High Word",
                42 => "PV2 Cumulative Power Generation Low Word",
                43 => "PV2 Cumulative Power Generation High Word",
                44 => "PV3 Cumulative Power Generation Low Word",
                45 => "PV3 Cumulative Power Generation High Word",
                46 => "Inverter Cumulative Output Energy Low Word",
                47 => "Inverter Cumulative Output Energy High Word",
                48 => "AC Charging Cumulative Rectified Energy Low Word",
                49 => "AC Charging Cumulative Rectified Energy High Word",
                50 => "Cumulative Charge Energy Low Word",
                51 => "Cumulative Charge Energy High Word",
                52 => "Cumulative Discharge Energy Low Word",
                53 => "Cumulative Discharge Energy High Word",
                54 => "Cumulative Off-grid Inverter Power Low Word",
                55 => "Cumulative Off-grid Inverter Power High Word",
                56 => "Cumulative Export Energy to Grid Low Word",
                57 => "Cumulative Export Energy to Grid High Word",
                58 => "Cumulative Import Energy from Grid Low Word",
                59 => "Cumulative Import Energy from Grid High Word",
                _ => "Unknown Cumulative Energy Register"
            };
            format!("{}: {:.1} kWh", desc, (value as f64) / 10.0)
        },

        // System Status and Temperature (60-67)
        60 => format!("Fault Code Low Word: {:#06x}", value),
        61 => format!("Fault Code High Word: {:#06x}", value),
        62 => format!("Warning Code Low Word: {:#06x}", value),
        63 => format!("Warning Code High Word: {:#06x}", value),
        64 => format!("Internal Ring Temperature: {} °C", value),
        65 => format!("Radiator Temperature 1: {} °C", value),
        66 => format!("Radiator Temperature 2: {} °C", value),
        67 => format!("Battery Temperature: {} °C", value),

        // Runtime and AutoTest Status (68-75)
        68..=70 => {
            if reg == 68 {
                format!("Reserved Register {}", reg)
            } else {
                let desc = if reg == 69 { "Runtime Low Word" } else { "Runtime High Word" };
                format!("{}: {} seconds", desc, value)
            }
        },
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
        72 => format!("AutoTest Limit: {}", if (71..=72).contains(&value) {
            format!("{:.1} V", (value as f64) / 10.0)
        } else if (73..=74).contains(&value) {
            format!("{:.2} Hz", (value as f64) / 100.0)
        } else {
            format!("{}", value)
        }),
        73 => format!("AutoTest Default Time: {} ms", value),
        74 => format!("AutoTest Trip Value: {}", if (71..=72).contains(&value) {
            format!("{:.1} V", (value as f64) / 10.0)
        } else if (73..=74).contains(&value) {
            format!("{:.2} Hz", (value as f64) / 100.0)
        } else {
            format!("{}", value)
        }),
        75 => format!("AutoTest Trip Time: {} ms", value),

        // AC Input and Reserved (76-80)
        77 => format!("AC Input Type: {}", if value == 0 { "Grid" } else { "Generator (12K Hybrid)" }),
        76 | 78 | 79 | 80 => format!("Reserved Register {}", reg),

        // BMS Data (81-112)
        81 => format!("BMS Max Charging Current: {:.2} A", (value as f64) / 100.0),
        82 => format!("BMS Max Discharge Current: {:.2} A", (value as f64) / 100.0),
        83 => format!("BMS Recommended Charging Voltage: {:.1} V", (value as f64) / 10.0),
        84 => format!("BMS Recommended Discharge Cut-off Voltage: {:.1} V", (value as f64) / 10.0),
        85..=94 => format!("BMS Status {}: {:#06x}", reg - 85, value),
        95 => format!("Inverter Battery Status: {:#06x}", value),
        96 => format!("Number of Batteries in Parallel: {}", value),
        97 => format!("Battery Capacity: {} Ah", value),
        98 => format!("BMS Battery Current: {:.2} A", (value as i16 as f64) / 100.0),
        99 => format!("BMS Fault Code: {:#06x}", value),
        100 => format!("BMS Warning Code: {:#06x}", value),
        101 => format!("BMS Maximum Cell Voltage: {:.3} V", (value as f64) / 1000.0),
        102 => format!("BMS Minimum Cell Voltage: {:.3} V", (value as f64) / 1000.0),
        103 => format!("BMS Maximum Cell Temperature: {:.1} °C", (value as i16 as f64) / 10.0),
        104 => format!("BMS Minimum Cell Temperature: {:.1} °C", (value as i16 as f64) / 10.0),
        105 => format!("BMS Firmware Update State: {}", match value {
            1 => "Upgrading",
            2 => "Upgrade Successful",
            3 => "Upgrade Failed",
            _ => "Unknown"
        }),
        106 => format!("BMS Cycle Count: {}", value),
        107 => format!("Inverter Battery Voltage Sample: {:.1} V", (value as f64) / 10.0),
        108 => format!("12K BT Temperature: {:.1} °C", (value as f64) / 10.0),
        109..=112 => format!("Reserved Temperature {}: {:.1} °C", reg - 108, (value as f64) / 10.0),

        // Parallel System Status (113-119)
        113 => {
            let master_slave = value & 0x03;
            let phase = (value >> 2) & 0x03;
            let parallel_num = (value >> 8) & 0xFF;
            format!("System Configuration:\n  Role: {}\n  Phase: {}\n  Parallel Units: {}\n  Raw: {:#06x}",
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
        120 => format!("Half BUS Voltage: {:.1} V", (value as f64) / 10.0),
        121 => format!("Generator Voltage: {:.1} V", (value as f64) / 10.0),
        122 => format!("Generator Frequency: {:.2} Hz", (value as f64) / 100.0),
        123 => format!("Generator Power: {} W", value),
        124 => format!("Generator Energy Today: {:.1} kWh", (value as f64) / 10.0),
        125 => format!("Generator Total Energy Low Word: {:.1} kWh", (value as f64) / 10.0),
        126 => format!("Generator Total Energy High Word: {:.1} kWh", (value as f64) / 10.0),
        127 => format!("EPS L1N Voltage: {:.1} V", (value as f64) / 10.0),
        128 => format!("EPS L2N Voltage: {:.1} V", (value as f64) / 10.0),
        129 => format!("EPS L1N Active Power: {} W", value),
        130 => format!("EPS L2N Active Power: {} W", value),
        131 => format!("EPS L1N Apparent Power: {} VA", value),
        132 => format!("EPS L2N Apparent Power: {} VA", value),
        133 => format!("EPS L1N Energy Today: {:.1} kWh", (value as f64) / 10.0),
        134 => format!("EPS L2N Energy Today: {:.1} kWh", (value as f64) / 10.0),
        135 => format!("EPS L1N Total Energy Low Word: {:.1} kWh", (value as f64) / 10.0),
        136 => format!("EPS L1N Total Energy High Word: {:.1} kWh", (value as f64) / 10.0),
        137 => format!("EPS L2N Total Energy Low Word: {:.1} kWh", (value as f64) / 10.0),
        138 => format!("EPS L2N Total Energy High Word: {:.1} kWh", (value as f64) / 10.0),

        // AFCI Data (139-148)
        139 => format!("Reserved Register {}", reg),
        140..=143 => format!("AFCI Current CH{}: {} mA", reg - 139, value),
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
            format!("AFCI Status Flags: {:#06x}\nActive Flags: {}", 
                value,
                if flags.is_empty() { "None".to_string() } else { flags.join(", ") }
            )
        },
        145..=148 => format!("AFCI Arc CH{}: {}", reg - 144, value),

        // Default case for unknown registers
        _ => format!("Unknown input register {}: {}", reg, value),
    }
} 