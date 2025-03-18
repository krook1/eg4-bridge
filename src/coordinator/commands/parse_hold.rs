use crate::prelude::*;

/// Parse and decode a hold register value according to Table 8 of the protocol specification
pub fn parse_hold_register(reg: u16, value: u16) -> String {
    match reg {
        // System Information (0-24)
        0 => {
            let lithium_type = (value >> 12) & 0xF;
            let power_rating = (value >> 8) & 0xF;
            let lead_acid_type = (value >> 4) & 0xF;
            let battery_type = value & 0xF;
            format!("Model Info: {:#06x}\n  Lithium Type: {}\n  Power Rating: {}\n  Lead Acid Type: {}\n  Battery Type: {}", 
                value, lithium_type, power_rating, lead_acid_type, battery_type)
        }
        2..=6 => format!("Serial Number Part {}: {:#06x}", reg - 1, value),
        7 => format!("Firmware Code: {}", value),
        9 => format!("Slave Firmware Code: {}", value),
        10 => format!("Control CPU Firmware Code: {}", value),
        11 => {
            let mut flags = Vec::new();
            if value & (1 << 0) != 0 { flags.push("Grid Connected"); }
            if value & (1 << 1) != 0 { flags.push("Grid Synchronization"); }
            if value & (1 << 2) != 0 { flags.push("Soft Start"); }
            if value & (1 << 3) != 0 { flags.push("PV Input"); }
            if value & (1 << 4) != 0 { flags.push("Battery Charging"); }
            if value & (1 << 5) != 0 { flags.push("Battery Discharging"); }
            if value & (1 << 6) != 0 { flags.push("EPS Mode"); }
            if value & (1 << 7) != 0 { flags.push("Fault Present"); }
            if value & (1 << 8) != 0 { flags.push("Charging from Grid"); }
            if value & (1 << 9) != 0 { flags.push("Charge Priority Active"); }
            if value & (1 << 10) != 0 { flags.push("Forced Discharge Active"); }
            if value & (1 << 11) != 0 { flags.push("AC Charge Active"); }
            if value & (1 << 12) != 0 { flags.push("Fault Lock"); }
            if value & (1 << 13) != 0 { flags.push("Battery Full"); }
            if value & (1 << 14) != 0 { flags.push("Battery Empty"); }
            if value & (1 << 15) != 0 { flags.push("Battery Standby"); }
            format!("Status Flags: {:#018b}\nActive flags: {}", value, flags.join(", "))
        }
        12 => {
            let month = value >> 8;
            let year = value & 0xFF;
            format!("Date: Month={}, Year=20{:02}", month, year)
        }
        13 => {
            let hour = value >> 8;
            let day = value & 0xFF;
            format!("Time: Hour={}, Day={}", hour, day)
        }
        14 => {
            let second = value >> 8;
            let minute = value & 0xFF;
            format!("Time: Second={}, Minute={}", second, minute)
        }
        15 => format!("Communication Address: {}", value),
        16 => format!("Language: {}", value),
        19 => format!("Device Type: {}", value),
        20 => format!("PV Input Mode: {}", value),
        23 => format!("Connect Time: {} ms", value),
        24 => format!("Reconnect Time: {} ms", value),

        // Grid Connection Limits (25-28)
        25 => format!("Grid Connect Low Volt: {:.1} V", (value as f64) / 10.0),
        26 => format!("Grid Connect High Volt: {:.1} V", (value as f64) / 10.0),
        27 => format!("Grid Connect Low Freq: {:.2} Hz", (value as f64) / 100.0),
        28 => format!("Grid Connect High Freq: {:.2} Hz", (value as f64) / 100.0),

        // Grid Protection Settings (29-53)
        29..=53 => {
            let desc = match reg {
                29 => "Grid Volt Limit 1 Low",
                30 => "Grid Volt Limit 1 High",
                31 => "Grid Volt Limit 1 Low Time",
                32 => "Grid Volt Limit 1 High Time",
                33 => "Grid Volt Limit 2 Low",
                34 => "Grid Volt Limit 2 High",
                35 => "Grid Volt Limit 2 Low Time",
                36 => "Grid Volt Limit 2 High Time",
                37 => "Grid Volt Limit 3 Low",
                38 => "Grid Volt Limit 3 High",
                39 => "Grid Volt Limit 3 Low Time",
                40 => "Grid Volt Limit 3 High Time",
                41 => "Grid Volt Moving Avg High",
                42 => "Grid Freq Limit 1 Low",
                43 => "Grid Freq Limit 1 High",
                44 => "Grid Freq Limit 1 Low Time",
                45 => "Grid Freq Limit 1 High Time",
                46 => "Grid Freq Limit 2 Low",
                47 => "Grid Freq Limit 2 High",
                48 => "Grid Freq Limit 2 Low Time",
                49 => "Grid Freq Limit 2 High Time",
                50 => "Grid Freq Limit 3 Low",
                51 => "Grid Freq Limit 3 High",
                52 => "Grid Freq Limit 3 Low Time",
                53 => "Grid Freq Limit 3 High Time",
                _ => "Unknown Grid Protection Setting"
            };
            
            if reg % 2 == 0 && reg <= 41 {
                format!("{}: {:.1} V", desc, (value as f64) / 10.0)
            } else if reg % 2 == 0 && reg > 41 {
                format!("{}: {:.2} Hz", desc, (value as f64) / 100.0)
            } else {
                format!("{}: {} ms", desc, value)
            }
        }

        // Power Quality Control (54-63)
        54 => format!("Max Q Percent for QV: {}%", value),
        55 => format!("V1L: {:.1} V", (value as f64) / 10.0),
        56 => format!("V2L: {:.1} V", (value as f64) / 10.0),
        57 => format!("V1H: {:.1} V", (value as f64) / 10.0),
        58 => format!("V2H: {:.1} V", (value as f64) / 10.0),
        59 => format!("Reactive Power Cmd Type: {}", value),
        60 => format!("Active Power Percent: {}%", value),
        61 => format!("Reactive Power Percent: {}%", value),
        62 => format!("Power Factor Command: {:.3}", (value as f64) / 1000.0),
        63 => format!("Power Soft Start Slope: {}", value),

        // System Control (64-67)
        64 => format!("System Charge Rate: {}%", value),
        65 => format!("System Discharge Rate: {}%", value),
        66 => format!("Grid Charge Power Rate: {}%", value),
        67 => format!("AC Charge SOC Limit: {}%", value),

        // AC Charge Settings (160-161)
        160 => format!("AC Charge Start SOC: {}%", value),
        161 => format!("AC Charge End SOC: {}%", value),

        // Battery Warning Settings (162-169)
        162 => format!("Battery Warning Voltage: {:.1} V", (value as f64) / 10.0),
        163 => format!("Battery Warning Recovery Voltage: {:.1} V", (value as f64) / 10.0),
        164 => format!("Battery Warning SOC: {}%", value),
        165 => format!("Battery Warning Recovery SOC: {}%", value),
        166 => format!("Battery Low to Utility Voltage: {:.1} V", (value as f64) / 10.0),
        167 => format!("Battery Low to Utility SOC: {}%", value),
        168 => format!("AC Charge Battery Current: {:.1} A", (value as f64) / 10.0),
        169 => format!("On Grid EOD Voltage: {:.1} V", (value as f64) / 10.0),

        // AutoTest Parameters (170-175)
        170 => format!("AutoTest Command: {}", value),
        171 => {
            let status = (value >> 0) & 0xF;
            let step = (value >> 4) & 0xF;
            let status_desc = match status {
                0 => "Waiting - Test not started",
                1 => "Testing - Test in progress",
                2 => "Test Failed - Last test failed",
                3 => "Voltage Test OK - Voltage tests passed",
                4 => "Frequency Test OK - Frequency tests passed",
                5 => "Test Passed - All tests completed successfully",
                _ => "Unknown status"
            };
            let step_desc = match step {
                1 => "V1L Test - Testing lower voltage limit 1",
                2 => "V1H Test - Testing upper voltage limit 1",
                3 => "F1L Test - Testing lower frequency limit 1",
                4 => "F1H Test - Testing upper frequency limit 1",
                5 => "V2L Test - Testing lower voltage limit 2",
                6 => "V2H Test - Testing upper voltage limit 2",
                7 => "F2L Test - Testing lower frequency limit 2",
                8 => "F2H Test - Testing upper frequency limit 2",
                _ => "No Test Active"
            };
            format!("AutoTest Status: {:#06x}\nStatus: {} - {}\nStep: {} - {}", 
                value, status, status_desc, step, step_desc)
        }
        172 => {
            let value_f = (value as f64) * if value & 0x8000 != 0 { -0.1 } else { 0.1 };
            format!("AutoTest Limit: {:.1} {}", value_f,
                if (reg >= 171 && reg <= 172) || (reg >= 175 && reg <= 176) { "V" } else { "Hz" })
        }
        173 => format!("AutoTest Default Time: {} ms", value),
        174 => {
            let value_f = (value as f64) * if value & 0x8000 != 0 { -0.1 } else { 0.1 };
            format!("AutoTest Trip Value: {:.1} {}", value_f,
                if (reg >= 171 && reg <= 172) || (reg >= 175 && reg <= 176) { "V" } else { "Hz" })
        }
        175 => format!("AutoTest Trip Time: {} ms", value),

        // Default case for unknown registers
        _ => format!("Unknown hold register {}: {}", reg, value),
    }
} 