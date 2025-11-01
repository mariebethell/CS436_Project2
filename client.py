
import errno
import socket
import sys
import threading
import time

def handle_request(rr_table, udp_connection, transaction_id, hostname, qtype):
    # Check RR table for record
    if rr_table.get_record(hostname, qtype) == None:
        # If not found, ask the local DNS server, then save the record if valid
        local_dns_address = ("127.0.0.1", 21000)

        # Request record
        query = {
            "transaction_id": transaction_id,
            "flag": "0000",
            "question": {
                "name": hostname,
                "type": qtype
            }
        }

        udp_connection.send_message(serialize(query), local_dns_address)

        record = deserialize(udp_connection.receive_message()[0])

        if record["answer"]["result"] != "Record not found":
            rr_table.add_record(record["answer"]["name"], record["answer"]["type"], record["answer"]["result"], record["answer"]["ttl"], 0)

    # Display RR table
    rr_table.display_table()
    transaction_id += 1 # increment transaction id

    return transaction_id

def main():
    # Create RR table
    rr_table = RRTable()

    # Setup UDP connection
    udp_connection = UDPConnection()

    # Initial transaction id is set to 0
    transaction_id = 0

    try:
        while True:
            input_value = input("Enter the hostname (or type 'quit' to exit) ")
            if input_value.lower() == "quit":
                break

            hostname = input_value
            qtype = None # determine if user enters a type or not

            # Extra credit, lets users decide the query type (e.g. A, AAAA, NS, CNAME)
            # This means input_value will be two values separated by a space
            if " " in input_value:
                hostname = input_value.split()[0]
                qtype = input_value.split()[1]

            qtype = qtype if qtype is not None else "A"
            transaction_id = handle_request(rr_table, udp_connection, transaction_id, hostname, qtype)
        
    except KeyboardInterrupt:
        print("Keyboard interrupt received, exiting...")
    finally:
        # Close UDP socket
        udp_connection.close()


def serialize(message: dict) -> str:
    # converting from DNS format (dict) to str 
    # uses provided DNStypes class
    qname = message["question"]["name"]
    qtype = DNSTypes.get_type_code(message["question"]["type"]) or 0
    aname = message.get("answer", {}).get("name", "") or ""
    atype = DNSTypes.get_type_code(message.get("answer", {}).get("type", "")) or 0
    ttl = message.get("answer", {}).get("ttl", "") or None
    result = message.get("answer", {}).get("result", "") or ""
    return f"{message['transaction_id']},{message['flag']},{qname},{qtype},{aname},{atype},{ttl},{result}"

def deserialize(data: str) -> dict:
    # converting from string back to DNS dict
    fields = data.split(',')
    return {
        "transaction_id": int(fields[0]),
        "flag": fields[1],
        "question": {
            "name": fields[2],
            "type": DNSTypes.get_type_name(int(fields[3])) if fields[3] != "" else ""
        },
        "answer": {
            "name": fields[4],
            "type": DNSTypes.get_type_name(int(fields[5])) if fields[5] not in ("", "None") else "",
            "ttl": int(fields[6]) if fields[6] and fields[6] != "None" else None,
            "result": fields[7] if len(fields) > 7 else ""
        }
    }

class RRTable:
    def __init__(self):
        self.records = []

        self.record_number = 0

        # Start the background thread
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.__decrement_ttl, daemon=True)
        self.thread.start()

    def add_record(self, name, type, result, ttl, static):
        with self.lock:
            self.record_number += 1

            record = {
                "record_number": self.record_number,
                "name": name,
                "type": type,
                "result": result,
                "ttl": ttl if ttl is not None else 60,  
                "static": static
            }

            self.records.append(record)

    def get_record(self, name, type):
        with self.lock:
            for record in self.records:
                if record["name"] == name and record["type"] == type:
                    return record
            return None

    def display_table(self):
        with self.lock:
            # Display the table in the following format (include the column names):
            # record_number,name,type,result,ttl,static

            # column names (from project description)
            print("record_no,name,type,result,ttl,static")

            for record in self.records:
                print(f"{record['record_number']},{record['name']},{record['type']},{record['result']},{record['ttl']},{record['static']}")

            print()
    
    def __decrement_ttl(self):
        while True:
            with self.lock:
                # Decrement ttl
                self.__remove_expired_records()
            time.sleep(1)

    def __remove_expired_records(self):
        # This method is only called within a locked context
        new_records = []

        # Remove expired records
        for record in self.records:
            if record['static'] == 0 and record['ttl'] != None:
                record['ttl'] -= 1
            
            # if record is still valid or set to static
            if record['static'] == 1 or record['ttl'] == None or record['ttl'] > 0:
                new_records.append(record)
        self.records = new_records

        # Update record numbers
        for i, record in enumerate(self.records, start = 1):
            record['record_number'] = i
        self.record_number = len(self.records)

class DNSTypes:
    """
    A class to manage DNS query types and their corresponding codes.

    Examples:
    >>> DNSTypes.get_type_code('A')
    8
    >>> DNSTypes.get_type_name(0b0100)
    'AAAA'
    """

    name_to_code = {
        "A": 0b1000,
        "AAAA": 0b0100,
        "CNAME": 0b0010,
        "NS": 0b0001,
    }

    code_to_name = {code: name for name, code in name_to_code.items()}

    @staticmethod
    def get_type_code(type_name: str):
        """Gets the code for the given DNS query type name, or None"""
        return DNSTypes.name_to_code.get(type_name, None)

    @staticmethod
    def get_type_name(type_code: int):
        """Gets the DNS query type name for the given code, or None"""
        return DNSTypes.code_to_name.get(type_code, None)

class UDPConnection:
    """A class to handle UDP socket communication, capable of acting as both a client and a server."""

    def __init__(self, timeout: int = 1):
        """Initializes the UDPConnection instance with a timeout. Defaults to 1."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.is_bound = False

    def send_message(self, message: str, address: tuple[str, int]):
        """Sends a message to the specified address."""
        self.socket.sendto(message.encode(), address)

    def receive_message(self):
        """
        Receives a message from the socket.

        Returns:
            tuple (data, address): The received message and the address it came from.

        Raises:
            KeyboardInterrupt: If the program is interrupted manually.
        """
        while True:
            try:
                data, address = self.socket.recvfrom(4096)
                return data.decode(), address
            except socket.timeout:
                continue
            except OSError as e:
                if e.errno == errno.ECONNRESET:
                    print("Error: Unable to reach the other socket. It might not be up and running.")
                else:
                    print(f"Socket error: {e}")
                self.close()
                sys.exit(1)
            except KeyboardInterrupt:
                raise

    def bind(self, address: tuple[str, int]):
        """Binds the socket to the given address. This means it will be a server."""
        if self.is_bound:
            print(f"Socket is already bound to address: {self.socket.getsockname()}")
            return
        self.socket.bind(address)
        self.is_bound = True

    def close(self):
        """Closes the UDP socket."""
        self.socket.close()

if __name__ == "__main__":
    main()
