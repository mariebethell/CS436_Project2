
import errno
import socket
import sys
import threading
import time

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

    def get_record(self, name, type):
        with self.lock:
            print("User requests:", name, type)
            for record in self.records:
                if record["name"] == name and record["type"] == type:
                    return record
            return None

    def display_table(self):
        with self.lock:
            # Display the table in the following format (include the column names):
            # record_number,name,type,result,ttl,static

            print("#, Name, Type, Result, TTL, Static")

            for record in self.records:
                print(
                    f"{record['record_number']:<{10}}"
                    f"{record['name']:<{10}}"
                    f"{record['type']:<{10}}"
                    f"{record['result']:<{10}}"
                    f"{str(record['ttl']):^{10}}"
                    f"{str(record['static']):^{10}}"
                )

            # column names
            print("#, Name, Type, Result, TTL, Static")

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

    
# Create RR table
rr_table = RRTable()

def handle_request(hostname, type):
    # Check RR table for record
    if rr_table.get_record(hostname, type) == None:
        print("not found, asking local DNS server")

        # If not found, ask the local DNS server, then save the record if valid
        local_dns_address = ("127.0.0.1", 21000)

        # Setup UDP connection
        udp_connection = UDPConnection()

        # Request record
        dns_query = {
            "transaction_id": 0,
            "flag": "0000",
            "question_name": hostname,
            "question_type": type,
            "answer_name": "",
            "answer_type": "",
            "ttl": "",
            "result": ""
        }

        udp_connection.send_message(serialize(dns_query), local_dns_address)

        record = deserialize(udp_connection.receive_message()[0])

        rr_table.add_record(record["transaction_id"], record["answer_name"], record["answer_type"], record["result"], record["ttl"], 0)

    # Display RR table
    rr_table.display_table()


def main():
    try:
        while True:
            input_value = input("Enter the hostname (or type 'quit' to exit) ")
            if input_value.lower() == "quit":
                break

            hostname = input_value
            type = None # determine if user enters a type or not

            query_code = DNSTypes.get_type_code("A")

            # For extra credit, let users decide the query type (e.g. A, AAAA, NS, CNAME)
            # This means input_value will be two values separated by a space
            if " " in input_value:
                hostname = input_value.split()[0]
                type = input_value.split()[1]

            handle_request(hostname, type)

    except KeyboardInterrupt:
        print("Keyboard interrupt received, exiting...")
    finally:
        # Close UDP socket
        pass


def serialize(dns_message):
    # Consider creating a serialize function
    # This can help prepare data to send through the socket

    # Turn dns message dict into comma separated string to send through socket
    serialized_message = ','.join([
        str(dns_message["transaction_id"]),
        str(dns_message["flag"]),
        str(dns_message["question_name"]),
        str(dns_message["question_type"]),
        str(dns_message["answer_name"]),
        str(dns_message["answer_type"]),
        str(dns_message["ttl"]),
        str(dns_message["result"])
    ])

    return serialized_message


def deserialize(data):
    # Consider creating a deserialize function
    # This can help prepare data that is received from the socket
    
    # Turn string from servers back into dict
    try:
        dns_message_contents = data.split(',')

        dns_message = {}

        dns_message["flag"] = dns_message_contents[0],
        dns_message["question_name"] = dns_message_contents[1],
        dns_message["question_type"] = dns_message_contents[2],
        dns_message["answer_name"] = dns_message_contents[3],
        dns_message["answer_type"] = dns_message_contents[4],
        dns_message["ttl"] = dns_message_contents[5],
        dns_message["result"] = dns_message_contents[6]
    except Exception as e:
        # might change this handling
        print("Corrupted dns_message returned from server")
        exit(1)

    return dns_message

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
