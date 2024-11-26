"""
CPSC 5520 Lab4 
Jooa Lee
chord_populate python file 
chord_populate takes a port number of an existing node and the filename of the datafile
"""

import csv
import hashlib
import json
import sys
import time
from chord_node import ChordNode
import os

M = 7
NODES = 2**M

class ChordPopulate:
    def __init__(self, node_port: int, csv_filename: str):
        """
        Initialize with an existing node's port and CSV filename
        """
        self.node_port = node_port
        self.csv_filename = csv_filename
        # Connect to existing node instead of creating new socket
        try:
            self.node = ChordNode(f"localhost:{node_port}")
            print(f"Successfully connected to node at port {node_port}")
        except Exception as e:
            print(f"Failed to connect to node at port {node_port}: {e}")
            raise

    def _generate_key_hash(self, playerid: str, year: str) -> int:
        """Generate key hash from playerid and year"""
        pure_id = playerid.split('/')[0] if '/' in playerid else playerid
        key = f"{playerid}{year}"
        sha1_hash = hashlib.sha1(key.encode()).hexdigest()
        return int(sha1_hash, 16) % NODES

    def _read_csv_data(self) -> list:
        data_items = []
        try:
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1']
            
            for encoding in encodings:
                try:
                    with open(self.csv_filename, mode='r', encoding=encoding, newline='') as csvfile:
                        # Print first few lines of the file for debugging
                        print(f"First few lines of the file (using {encoding}):")
                        first_lines = csvfile.read(500)
                        print(first_lines)
                        csvfile.seek(0)
                        
                        # Try to read the CSV
                        reader = csv.DictReader(csvfile)
                        headers = reader.fieldnames
                        
                        if not headers:
                            print(f"No headers found with {encoding} encoding")
                            continue
                            
                        print(f"Found headers: {headers}")
                        
                        if 'Player Id' not in headers or 'Year' not in headers:
                            print(f"Required columns not found in headers with {encoding} encoding")
                            continue
                            
                        row_count = 0
                        for row in reader:
                            row_count += 1
                            try:
                                if not row['Player Id'] or not row['Year'] or row['Year'] == '--':
                                    print(f"Skipping row {row_count} due to invalid data")
                                    continue

                                key_hash = self._generate_key_hash(row['Player Id'], row['Year'])
                                
                                value = {}
                                for column, val in row.items():
                                    if column != 'Player Id' and column != 'Year':
                                        value[column] = None if val == '--' else val
                                
                                data_items.append((key_hash, value))
                                print(f"Successfully processed row {row_count}: Player={row['Player Id']}, Year={row['Year']}")
                            
                            except Exception as e:
                                print(f"Error processing row {row_count}: {e}")
                                print(f"Row content: {row}")
                                continue
                        
                        if data_items:
                            print(f"Successfully read {len(data_items)} records using {encoding} encoding")
                            return data_items
                            
                except UnicodeDecodeError:
                    print(f"Failed to read with {encoding} encoding, trying next...")
                    continue
                except Exception as e:
                    print(f"Error while trying {encoding} encoding: {e}")
                    continue
                    
            print("Failed to read CSV with any encoding")
            return []
            
        except Exception as e:
            print(f"Error reading CSV file {self.csv_filename}: {e}")
            print(f"Current working directory: {os.getcwd()}")
            return []
        
    def populate_network(self) -> bool:
        """
        Populate the Chord network with data from CSV file.
        Duplicate entries are handled using a last-write wins strategy,
        where the most recent entry overwrites pervious ones

        Returns: True if the network was successfully populates, other wise
        return False
        """
        data_items = self._read_csv_data()
        if not data_items:
            print("No data items found to populate")
            return False

        success = True
        total = len(data_items)

        for i, (key_hash, data) in enumerate(data_items, 1):
            try:
                self.node.put(key_hash, json.dumps(data))
                print(f"Progress: {i}/{total} - Successfully stored data for key {key_hash}")
            except Exception as e:
                print(f"Progress: {i}/{total} - Failed to store data for key {key_hash}: {e}")
                success = False
        return success

def main():
    """main function for populating a chord network with data from csv file"""
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py <node_port> <csv_filename>")
        sys.exit(1)
        
    try:
        node_port = int(sys.argv[1])
        csv_filename = sys.argv[2]
        
        print(f"Initializing population with node port {node_port} and file {csv_filename}")
        populator = ChordPopulate(node_port, csv_filename)
        
        print("\nWaiting for network to stabilize...")
        time.sleep(5)
        
        if populator.populate_network():
            print("\nSuccessfully populated Chord network")
            sys.exit(0)
        else:
            print("\nFailed to populate Chord network")
            sys.exit(1)
    except ValueError:
        print("Error: Port must be an integer")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()