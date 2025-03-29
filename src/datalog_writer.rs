use crate::prelude::*;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use log::{info, error};

#[derive(Debug, Clone)]
pub struct DatalogWriter {
    file: Arc<Mutex<std::fs::File>>,
    path: String,
    values_written: Arc<Mutex<u64>>,
}

impl DatalogWriter {
    pub fn new(path: &str) -> Result<Self> {
        info!("Opening datalog file at {}", path);
        
        // Ensure the directory exists
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open file in append mode, create if doesn't exist
        let file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to open datalog file {}: {}", path, e);
                return Err(e.into());
            }
        };

        // Set file permissions to 0644
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)) {
                error!("Failed to set permissions on datalog file {}: {}", path, e);
                return Err(e.into());
            }
        }

        info!("Successfully opened datalog file with permissions 0644");

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            path: path.to_string(),
            values_written: Arc::new(Mutex::new(0)),
        })
    }

    pub fn write_hold_data(&self, serial: Serial, datalog: Serial, data: &[(u16, u16)]) -> Result<()> {
        self.write_data(serial, datalog, "hold", data)
    }

    pub fn write_input_data(&self, serial: Serial, datalog: Serial, data: &[(u16, u16)]) -> Result<()> {
        self.write_data(serial, datalog, "input", data)
    }

    fn write_data(&self, serial: Serial, datalog: Serial, register_type: &str, data: &[(u16, u16)]) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        let mut json_data = serde_json::Map::new();
        json_data.insert("utc_timestamp".to_string(), serde_json::Value::Number(timestamp.into()));
        json_data.insert("serial".to_string(), serde_json::Value::String(serial.to_string()));
        json_data.insert("datalog".to_string(), serde_json::Value::String(datalog.to_string()));
        json_data.insert("register_type".to_string(), serde_json::Value::String(register_type.to_string()));

        // Convert register data to hex strings
        let mut raw_data = serde_json::Map::new();
        for (register, value) in data {
            raw_data.insert(
                register.to_string(),
                serde_json::Value::String(format!("0x{:04X}", value)),
            );
        }
        json_data.insert("raw_data".to_string(), serde_json::Value::Object(raw_data));

        let json_value = serde_json::Value::Object(json_data);
        let json_string = serde_json::to_string(&json_value)?;
        
        let mut file = self.file.lock().map_err(|_| anyhow::anyhow!("Failed to lock datalog file"))?;
        match writeln!(file, "{}", json_string) {
            Ok(_) => {
                if let Err(e) = file.flush() {
                    error!("Failed to flush datalog file {}: {}", self.path, e);
                    return Err(e.into());
                }
                
                // Update and log the number of values written
                let mut values_written = self.values_written.lock().map_err(|_| anyhow::anyhow!("Failed to lock values counter"))?;
                *values_written += data.len() as u64;
                info!("Total values stored in datalog file: {}", *values_written);
                
                Ok(())
            },
            Err(e) => {
                error!("Failed to write to datalog file {}: {}", self.path, e);
                Err(e.into())
            }
        }
    }

    pub async fn stop(&self) {
        // Nothing specific to stop for now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_hold_data() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let writer = DatalogWriter::new(temp_file.path().to_str().unwrap())?;

        let serial = Serial::from_str("0000000001")?;
        let datalog = Serial::from_str("0000000002")?;
        let data = vec![(0, 1234), (1, 5678)];

        writer.write_hold_data(serial, datalog, &data)?;

        // Read the file and verify contents
        let contents = std::fs::read_to_string(temp_file.path())?;
        let json: serde_json::Value = serde_json::from_str(&contents)?;
        
        assert_eq!(json["serial"], "0000000001");
        assert_eq!(json["datalog"], "0000000002");
        assert_eq!(json["register_type"], "hold");
        assert_eq!(json["raw_data"]["0"], "0x04D2");
        assert_eq!(json["raw_data"]["1"], "0x162E");

        Ok(())
    }

    #[test]
    fn test_write_input_data() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let writer = DatalogWriter::new(temp_file.path().to_str().unwrap())?;

        let serial = Serial::from_str("0000000001")?;
        let datalog = Serial::from_str("0000000002")?;
        let data = vec![(0, 1234), (1, 5678)];

        writer.write_input_data(serial, datalog, &data)?;

        // Read the file and verify contents
        let contents = std::fs::read_to_string(temp_file.path())?;
        let json: serde_json::Value = serde_json::from_str(&contents)?;
        
        assert_eq!(json["serial"], "0000000001");
        assert_eq!(json["datalog"], "0000000002");
        assert_eq!(json["register_type"], "input");
        assert_eq!(json["raw_data"]["0"], "0x04D2");
        assert_eq!(json["raw_data"]["1"], "0x162E");

        Ok(())
    }
} 