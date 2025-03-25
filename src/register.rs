use crate::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Register {
    pub number: u16,
    pub name: String,
    pub description: String,
    #[serde(rename = "datatype")]
    pub data_type: String,
    pub access: String,
    #[serde(default = "default_scaling")]
    pub scaling: f64,
    #[serde(default)]
    pub unit: String,
}

fn default_scaling() -> f64 {
    1.0
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterType {
    pub register_type: String,
    pub register_map: Vec<Register>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterMap {
    pub registers: Vec<RegisterType>,
}

#[derive(Clone)]
pub struct RegisterParser {
    registers: HashMap<u16, Register>,
}

impl RegisterParser {
    pub fn new(register_file: &str) -> Result<Self> {
        let content = std::fs::read_to_string(register_file)
            .map_err(|err| anyhow!("Error reading register file {}: {}", register_file, err))?;
        
        let register_map: RegisterMap = serde_json::from_str(&content)
            .map_err(|err| anyhow!("Error parsing register file: {}", err))?;

        let mut registers = HashMap::new();
        
        for register_type in register_map.registers {
            for register in register_type.register_map {
                registers.insert(register.number, register);
            }
        }

        Ok(Self { registers })
    }

    pub fn get_register(&self, number: u16) -> Option<&Register> {
        self.registers.get(&number)
    }

    pub fn decode_registers(&self, raw_data: &HashMap<String, String>, show_unknown: bool, register_type: &str) -> HashMap<String, f64> {
        let mut decoded = HashMap::new();
        
        for (reg_num_str, hex_value) in raw_data {
            if let Ok(reg_num) = reg_num_str.parse::<u16>() {
                if let Some(register) = self.get_register(reg_num) {
                    let value = register.decode_value(hex_value);
                    decoded.insert(register.name.clone(), value);
                } else if show_unknown {
                    let value = u16::from_str_radix(hex_value, 16)
                        .unwrap_or(0) as f64;
                    decoded.insert(format!("{}_unknown_{}", register_type, reg_num), value);
                }
            }
        }

        decoded
    }
}

impl Register {
    pub fn decode_value(&self, hex_value: &str) -> f64 {
        let value = u16::from_str_radix(hex_value, 16)
            .unwrap_or(0) as f64;
        
        if self.data_type == "float" {
            value * self.scaling
        } else {
            value
        }
    }
} 