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
        authoritative_address = ('127.0.0.1', 22000)

        while True:
            # Wait for query
            query, client_address = udp_connection.receive_message()
            query_data = deserialize(query)
            print(f"Query from {client_address}: {query_data}")

            # Check RR table for record
            name = query_data["question"]["name"]
            query_type = query_data["question"]["type"]
            record = rr_table.get_record(name, query_type)

            # If not found, ask the authoritative DNS server of the requested hostname/domain
            if not record:
                print(f"Not found locally, querying authoritative server.")
                udp_connection.send_message(serialize(query_data), authoritative_address)
                response_data, _ = udp_connection.receive_message()
                response = deserialize(response_data)

                # Then save the record if valid
                if response["answer"]["result"] != "Record not found":
                    rr_table.add_record(
                        response["answer"]["name"],
                        response["answer"]["type"],
                        response["answer"]["result"],
                        response["answer"]["ttl"],
                        static=0
                    )
            else:
                response = {
                    "transaction_id": query_data["transaction_id"],
                    "flag": "0001",
                    "question": query_data["question"],
                    "answer": {
                        "name": record["name"],
                        "type": record["type"],
                        "ttl": record["ttl"],
                        "result": record["result"]
                    }
                }

            # Else, add "Record not found" in the DNS response
            # this looks redundant but if no local OR authoritative, build a record not found response
            if not record:
                if response["answer"]["result"] == "Record not found":
                    response = {
                        "transaction_id": query_data["transaction_id"],
                        "flag": "0001",
                        "question": query_data["question"],
                        "answer": {
                            "name": name,
                            "type": query_type,
                            "ttl": 0,
                            "result": "Record not found"
                        }
                    }

            # send response
            udp_connection.send_message(serialize(response), client_address)

            # The format of the DNS query and response is in the project description

            # Display RR table
            rr_table.display_table()
    except KeyboardInterrupt:
        print("Keyboard interrupt received, exiting...")
    finally:
        # Close UDP socket
        udp_connection.close()


def main():
    # Add initial records from test cases diagram
    rr_table = RRTable()

    # testing TTL/expiration, uncomment if you want
    # rr_table.add_record("temp.com", "A", "2.2.2.2", 3, 0)     # should expire
    # rr_table.add_record("static.com", "A", "1.1.1.1", 5, 1)   # doesnt expire
    # for i in range(6):
    #     print(f"\n After {i} seconds:")
    #     rr_table.display_table()
    #     time.sleep(1)

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

    # if you want to test: run with listen uncommented in one terminal
    # then open new terminal and comment out listen, uncomment test_udp_send
    # "question" can be changed if you want to test other inputs
    listen(rr_table)
    #test_udp_send()
    

def test_udp_send():
    client = UDPConnection()
    message = {
        "transaction_id": 1,
        "flag": "0000",
        "question": {"name": "dns.amazone.com", "type": "A"},
        "answer": {}
    }
    client.send_message(serialize(message), ("127.0.0.1", 21000))
    response, _ = client.receive_message()
    print("Response from server:", response)

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
