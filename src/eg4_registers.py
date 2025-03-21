import json
from datetime import datetime
from typing import Dict, List, Optional, Union
import argparse
import os

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
    parser.add_argument('-f', '--datalog-file', required=True,
                      help='Path to the datalog.json file')
    parser.add_argument('-s', '--register-file', required=True,
                      help='Path to the eg4_registers.json file')
    parser.add_argument('-v', '--verbose', action='store_true',
                      help='Show units in output')
    parser.add_argument('--human', action='store_true',
                      help='Show human readable timestamps')
    parser.add_argument('-u', '--unknown', action='store_true',
                      help='Show undefined registers in output')
    
    args = parser.parse_args()
    
    # Verify files exist
    if not os.path.exists(args.datalog_file):
        print(f"Error: Datalog file '{args.datalog_file}' not found")
        exit(1)
    if not os.path.exists(args.register_file):
        print(f"Error: Register file '{args.register_file}' not found")
        exit(1)
    
    # Load register definitions
    register_map = load_register_map(args.register_file)
    
    # Load datalog entries
    entries = load_datalog_file(args.datalog_file)
    
    # Process each entry
    for entry in entries:
        # Decode register values
        decoded_values = entry.decode_values(register_map, args.unknown)
        
        # Print decoded values
        for name, value in decoded_values.items():
            reg = next((r for r in register_map.registers.values() if r.name == name), None)
            if reg:
                unit_str = f" {reg.unit}" if reg.unit and args.verbose else ""
            else:
                unit_str = " (undefined)"
            timestamp = entry.timestamp.strftime('%Y-%m-%d %H:%M:%S') if args.human else int(entry.timestamp.timestamp())
            print(f"{timestamp} {entry.serial} {entry.datalog} {name}: {value}{unit_str}") 
