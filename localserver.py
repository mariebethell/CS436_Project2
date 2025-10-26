import errno
import socket
import sys
import threading
import time

# passing rr_table as a parameter (maybe a better way around this?)
def listen(rr_table):
    udp_connection = UDPConnection(timeout=1)

    try:
        udp_connection.bind(('127.0.0.1', 21000))

        while True:
            # Wait for query
            query, client_address = udp_connection.receive_message()
            print(f"Query from {client_address}: {query}")

            # Check RR table for record
            record = rr_table.get_record(query['domain_name'], 'A')

            if record:
                response = serialize(record)
                udp_connection.send_message(response, client_address)
            else:
                # If not found, ask the authoritative DNS server of the requested hostname/domain
                amazone_dns_address = ("127.0.0.1", 22000)
                udp_connection.send_message(query, amazone_dns_address)

            # This means parsing the query to get the domain (e.g. amazone.com from shop.amazone.com)
            # With the domain, you can do a self lookup to get the NS record of the domain (e.g. dns.amazone.com)
            # With the server name, you can do a self lookup to get the IP address (e.g. 127.0.0.1)

            # TODO:
            # Then save the record if valid
            # Else, add "Record not found" in the DNS response

            # The format of the DNS query and response is in the project description

            # Display RR table
            rr_table.display_table()
            pass
    except KeyboardInterrupt:
        print("Keyboard interrupt received, exiting...")
    finally:
        # Close UDP socket
        udp_connection.close()


def main():
    # Add initial records from test cases diagram
    rr_table = RRTable()

    initial_records = [
        ("www.csusm.edu", "A", "144.37.5.45", None, 1),
        ("my.csusm.edu", "A", "144.37.5.150", None, 1),
        ("amazone.com", "NS", "dns.amazone.com", None, 1),
        ("dns.amazone.com", "A", "127.0.0.1", None, 1)
    ]

    for record in initial_records:
        rr_table.add_record(*record)
    # testing display table, uncomment if you want to test as well
    # rr_table.display_table()
    # print(rr_table.get_record("www.csusm.edu", "A"))
    # print(rr_table.get_record("amazone.com", "NS"))
    # print(rr_table.get_record("test.com", "A"))

    local_dns_address = ("127.0.0.1", 21000)
    # Bind address to UDP socket

    listen(rr_table)


def serialize(record):
    # Consider creating a serialize function
    # This can help prepare data to send through the socket
    return f"{record['name']},{record['type']},{record['result']},{record['ttl']},{record['static']}"

def deserialize(data):
    # Consider creating a deserialize function
    # This can help prepare data that is received from the socket
    # separate by commas and return dict
    fields = data.split(',')
    return {
        'name': fields[0],
        'type': fields[1],
        'result': fields[2],
        'ttl': int(fields[3]),
        'static': int(fields[4])
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
                "ttl": ttl,
                "static": static
            }

            self.records.append(record)

    # letting user specify type (extra credit in client.py)  
    def get_record(self, name, type):
        with self.lock:
            for record in self.records:
                if record["name"] == name and record["type"] == type:
                    return record

    def display_table(self):
        with self.lock:
            # Display the table in the following format (include the column names):
            # record_number,name,type,result,ttl,static

            # column names (from project descriptiion)
            print("record_no,name,type,result,ttl,static")

            for record in self.records:
                print(f"{record['record_number']},{record['name']},{record['type']},{record['result']},{record['ttl']},{record['static']}")

    def __decrement_ttl(self):
        while True:
            with self.lock:
                # Decrement ttl
                self.__remove_expired_records()
            time.sleep(1)

    def __remove_expired_records(self):
        # This method is only called within a locked context

        # Remove expired records
        # Update record numbers
        pass


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
