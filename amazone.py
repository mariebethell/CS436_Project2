import errno
import socket
import sys


def listen(rr_table):
    udp_connection = UDPConnection(timeout=1)
    try:
        while True:
            # Wait for query
            query, local_address = udp_connection.receive_message()
            query_data = deserialize(query)
            print(f"Query from {local_address}: {query_data}")
            # Check RR table for record
            name = query["question"]["name"]
            query_type = query["question"]["type"]
            record = rr_table.get_record(name, query_type)
            # If not found, add "Record not found" in the DNS response
            if record:
                response = {
                    "transaction_id": query["transaction_id"],
                    "flag": "0001",
                    "question": query["question"],
                    "answer": {
                        "name": record["name"],
                        "type": record["type"],
                        "ttl": record["ttl"],
                        "result": record["result"]
                    }
                }
            # Else, return record in DNS response
            else:
                response = {
                    "transaction_id": query["transaction_id"],
                    "flag": "0001",
                    "question": query["question"],
                    "answer": {
                        "name": name,
                        "type": query_type,
                        "ttl": 0,
                        "result": "Record not found"
                    }
                }

            # The format of the DNS query and response is in the project description
            udp_connection.send_message(serialize(response), local_address)
            # Display RR table
            rr_table.display_table()
            pass
    except KeyboardInterrupt:
        print("Keyboard interrupt received, exiting...")
    finally:
        # Close UDP socket
        udp_connection.close()
        pass


def main():
    # Add initial records
    # These can be found in the test cases diagram
    rr_table = RRTable()
    rr_table.add_record()
    amazone_dns_address = ("127.0.0.1", 22000)
    # Bind address to UDP socket
    udp_connection = UDPConnection(timeout=1)
    udp_connection.bind(amazone_dns_address)
    listen()


def serialize(message: dict) -> str:
    # converting from DNS format (dict) to str 
    # uses provided DNStypes class
    qname = message["question"]["name"]
    qtype = DNSTypes.get_type_code(message["question"]["type"])
    aname = message["answer"].get("name", "")
    atype = DNSTypes.get_type_code(message["answer"].get("type", ""))
    ttl = message["answer"].get("ttl", "")
    result = message["answer"].get("result", "")
    return f"{message['transaction_id']},{message['flag']},{qname},{qtype},{aname},{atype},{ttl},{result}"


def deserialize(data: str) -> dict:
    # converting from string back to DNS dict
    fields = data.split(',')
    return {
        "transaction_id": int(fields[0]),
        "flag": fields[1],
        "question": {
            "name": fields[2],
            "type": DNSTypes.get_type_name(int(fields[3]))
        },
        "answer": {
            "name": fields[4],
            "type": DNSTypes.get_type_name(int(fields[5])) if fields[5] else "",
            "ttl": int(fields[6]) if fields[6] else None,
            "result": fields[7] if len(fields) > 7 else ""
        }
    }


class RRTable:
    def __init__(self):
        self.records = []
        self.record_number = 0

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
            for record in self.records:
                if record["name"] == name and record["type"] == type:
                    return record

    def display_table(self):
        # Display the table in the following format (include the column names):
        print("record_no,name,type,result,ttl,static")

        for record in self.records:
            print(f"{record['record_number']},{record['name']},{record['type']},{record['result']},{record['ttl']},{record['static']}")
        # record_number,name,type,result,ttl,static
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
