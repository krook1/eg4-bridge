#!/usr/bin/python3

import json
from datetime import datetime
from typing import Dict, List, Optional, Union
import argparse
import os
import yaml
from dataclasses import dataclass
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

@dataclass
class InfluxConfig:
    url: str
    username: str
    password: str
    database: str

@dataclass
class Config:
    influx: Optional[InfluxConfig]
    datalog_file: str
    register_file: str
    verbose: bool = False
    human_timestamps: bool = False
    show_unknown: bool = False

def write_to_influx(config: InfluxConfig, points: List[Point]) -> bool:
    """Write points to InfluxDB"""
    try:
        client = InfluxDBClient(
            url=config.url,
            token=f"{config.username}:{config.password}",
            org="-"
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        # Write all points
        write_api.write(bucket=config.database, record=points)
        
        # Clean up
        write_api.close()
        client.close()
        return True
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        return False

def load_config(config_file: str) -> Optional[Config]:
    """Load configuration from YAML file"""
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            
            # Load InfluxDB config if enabled
            influx_config = None
            influx_section = config.get('influx', {})
            if influx_section.get('enabled', True):
                influx_config = InfluxConfig(
                    url=influx_section.get('url', 'http://localhost:8086'),
                    username=influx_section.get('username', ''),
                    password=influx_section.get('password', ''),
                    database=influx_section.get('database', 'eg4_data')
                )
            
            # Load file paths
            datalog_file = config.get('datalog_file')
            register_file = config.get('register_file')
            
            if not datalog_file or not register_file:
                print("Error: datalog_file and register_file must be specified in config")
                return None
                
            return Config(
                influx=influx_config,
                datalog_file=datalog_file,
                register_file=register_file,
                verbose=config.get('verbose', False),
                human_timestamps=config.get('human_timestamps', False),
                show_unknown=config.get('show_unknown', False)
            )
    except Exception as e:
        print(f"Warning: Could not load config file: {e}")
        return None

class Register:
    def __init__(self, number: int, name: str, description: str, data_type: str, 
                 access: str, scaling: float = 1.0, unit: str = ""):
        self.number = number
        self.name = name
        self.description = description
        self.data_type = data_type
        self.access = access
        self.scaling = scaling
        self.unit = unit

    def decode_value(self, hex_value: str) -> Union[int, float]:
        """Decode a hex string value based on the register's data type"""
        value = int(hex_value, 16)
        if self.data_type == "float":
            return value * self.scaling
        return value

class RegisterMap:
    def __init__(self, registers: List[Register]):
        self.registers = {reg.number: reg for reg in registers}
        
    def get_register(self, number: int) -> Optional[Register]:
        return self.registers.get(number)
    
    def decode_registers(self, raw_data: Dict[str, str], show_unknown: bool = False, register_type: str = "unknown") -> Dict[str, Union[int, float]]:
        """Decode a dictionary of raw register values"""
        decoded = {}
        for reg_num, hex_value in raw_data.items():
            reg = self.get_register(int(reg_num))
            if reg:
                decoded[reg.name] = reg.decode_value(hex_value)
            elif show_unknown:
                decoded[f"{register_type}_unknown_{reg_num}"] = int(hex_value, 16)
        return decoded

class DatalogEntry:
    def __init__(self, datalog: str, raw_data: Dict[str, str], 
                 register_type: str, serial: str, utc_timestamp: int):
        self.datalog = datalog
        self.raw_data = raw_data
        self.register_type = register_type
        self.serial = serial
        self.timestamp = datetime.fromtimestamp(utc_timestamp)

    @classmethod
    def from_json(cls, json_str: str) -> 'DatalogEntry':
        """Create a DatalogEntry from a JSON string"""
        data = json.loads(json_str)
        return cls(
            datalog=data['datalog'],
            raw_data=data['raw_data'],
            register_type=data['register_type'],
            serial=data['serial'],
            utc_timestamp=data['utc_timestamp']
        )

    def decode_values(self, register_map: RegisterMap, show_unknown: bool = False) -> Dict[str, Union[int, float]]:
        """Decode the raw register values using the provided register map"""
        return register_map.decode_registers(self.raw_data, show_unknown, self.register_type)

def load_datalog_file(filepath: str) -> List[DatalogEntry]:
    """Load and parse a datalog.json file"""
    entries = []
    with open(filepath, 'r') as f:
        for line in f:
            if line.strip():
                entries.append(DatalogEntry.from_json(line))
    return entries

def load_register_map(filepath: str) -> RegisterMap:
    """Load register definitions from a JSON file"""
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    registers = []
    for register_type in data.get('registers', []):
        reg_type = register_type.get('register_type', 'unknown').lower()
        for reg_data in register_type.get('register_map', []):
            try:
                # Get required fields with defaults for missing values
                reg_number = reg_data.get('register_number')
                if reg_number is None:
                    print(f"Warning: Skipping register with missing register_number: {reg_data}")
                    continue
                
                # Convert unit_scale to float if present, otherwise use 1.0
                try:
                    scaling = float(reg_data.get('unit_scale', 1.0))
                except (ValueError, TypeError):
                    scaling = 1.0
                
                # Use type-based shortname if none provided
                shortname = reg_data.get('shortname')
                if not shortname:
                    shortname = f"{reg_type}-{reg_number}"
                
                reg = Register(
                    number=reg_number,
                    name=shortname,
                    description=reg_data.get('description', ''),
                    data_type=reg_data.get('datatype', 'uint16'),  # Default to uint16 if not specified
                    access='read_only' if reg_data.get('read_only') == 'true' else 'read_write',
                    scaling=scaling,
                    unit=reg_data.get('unit', '')
                )
                registers.append(reg)
            except Exception as e:
                print(f"Warning: Error processing register data: {e}\nData: {reg_data}")
                continue
    
    if not registers:
        print("Warning: No valid registers were loaded from the file")
    
    return RegisterMap(registers)

# Example usage:
if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Process EG4 datalog entries')
    parser.add_argument('-f', '--datalog-file',
                      help='Path to the datalog.json file (overrides config)')
    parser.add_argument('-s', '--register-file',
                      help='Path to the eg4_registers.json file (overrides config)')
    parser.add_argument('-v', '--verbose', action='store_true',
                      help='Show units in output (overrides config)')
    parser.add_argument('--human', action='store_true',
                      help='Show human readable timestamps (overrides config)')
    parser.add_argument('-u', '--unknown', action='store_true',
                      help='Show undefined registers in output (overrides config)')
    parser.add_argument('--influx', action='store_true',
                      help='Output in InfluxDB line protocol format')
    parser.add_argument('--config', default='config.yaml',
                      help='Path to configuration file (default: config.yaml)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    if not config:
        print("Error: Could not load configuration. Please check your config.yaml file.")
        exit(1)
    
    # Override config with command line arguments
    if args.datalog_file:
        config.datalog_file = args.datalog_file
    if args.register_file:
        config.register_file = args.register_file
    if args.verbose:
        config.verbose = True
    if args.human:
        config.human_timestamps = True
    if args.unknown:
        config.show_unknown = True
    
    # Verify files exist
    if not os.path.exists(config.datalog_file):
        print(f"Error: Datalog file '{config.datalog_file}' not found")
        exit(1)
    if not os.path.exists(config.register_file):
        print(f"Error: Register file '{config.register_file}' not found")
        exit(1)
    
    # Load register definitions
    register_map = load_register_map(config.register_file)
    
    # Load datalog entries
    entries = load_datalog_file(config.datalog_file)
    
    # Process each entry
    points = []  # Collect points for batch writing
    for entry in entries:
        # Decode register values
        decoded_values = entry.decode_values(register_map, config.show_unknown)
        
        # Print decoded values
        for name, value in decoded_values.items():
            reg = next((r for r in register_map.registers.values() if r.name == name), None)
            if reg:
                unit_str = f" {reg.unit}" if reg.unit and config.verbose else ""
            else:
                unit_str = " (undefined)"
            timestamp = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S') if config.human_timestamps else int(entry.timestamp.timestamp())
            
            if args.influx and config.influx:
                # Create InfluxDB point
                point = Point("eg4_inverter") \
                    .tag("serial", entry.serial) \
                    .tag("datalog", entry.datalog) \
                    .field(name, value) \
                    .time(entry.timestamp)
                points.append(point)
                
                # Also print to stdout if verbose
                if config.verbose:
                    print(f"eg4_inverter,serial={entry.serial},datalog={entry.datalog} {name}={value} {int(entry.timestamp.timestamp() * 1e9)}")
            else:
                print(f"{timestamp} com.eg4electronics.inverter.{entry.serial}.{entry.datalog}.{name}: {value}{unit_str}")
    
    # Write all points to InfluxDB if enabled
    if args.influx and config.influx and points:
        if write_to_influx(config.influx, points):
            print(f"Successfully wrote {len(points)} points to InfluxDB")
        else:
            print("Failed to write points to InfluxDB") 
