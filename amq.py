import socket
import multiprocessing
import logging
import queue


# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
# Create queues
writer_queue = multiprocessing.Queue()
reader_queue = multiprocessing.Queue()

class activemq:
    def __init__(self, hostname: str, port: int):
        self.amq_hostname = hostname
        self.amq_port = port
        self.amq_con = stomp_connection(self.amq_hostname, self.amq_port)
        self.amq_con.start_comms()
        self.amq_con.send_stomp_message('CONNECT', {'accept-version': '1.0,1.1,1.2', 'host': 'localhost'}, None)
        logging.info(f"STOMP connection created to {self.amq_hostname}:{self.amq_port}")

    def subscribe(self, destination: str):
        self.amq_con.send_stomp_message('SUBSCRIBE', {'id':'0', 'destination':f'/queue/{destination}'}, None)
        logging.info(f'Subscribed to AMQ queue: {destination}')

    def unsub_and_close(self):
        self.amq_con.send_stomp_message('DISCONNECT', {'receipt':'9544'}, None)
        logging.info(f'Unsubscribed from AMQ queues')
        self.amq_con.close()

    @staticmethod
    def writer(sock, writer_queue):
        logging.info('Starting Writer Process')
        try:
            while True:
                try:
                    message = writer_queue.get(timeout=1)  # Use timeout to periodically check for stop signal
                    if message == 'STOP':
                        logging.info('Writer process received stop signal.')
                        break
                    sock.sendall(message.encode('utf-8'))
                    logging.info('Queued message sent successfully.')
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f'Failed to send message: {e}')
        except KeyboardInterrupt:
            logging.info('Writer process interrupted and shutting down.')

    @staticmethod
    def reader(sock, reader_queue):
        logging.info('Starting Reader Process')
        buffer = b""  # Persistent buffer for accumulating data

        try:
            while True:
                try:
                    data = sock.recv(1024)  # Read data in chunks
                    if not data:  # If no data, the socket is closed
                        logging.warning("Socket closed or no data received.")
                        break

                    buffer += data  # Append received data to the buffer
                    logging.debug(f"Buffer after receiving data: {buffer!r}")

                    # Process the buffer for complete messages (split by \x00)
                    while b'\x00' in buffer:
                        message, buffer = buffer.split(b'\x00', 1)  # Split at \x00
                        logging.debug(f"Processed message: {message.decode('utf-8')}")
                        reader_queue.put(message.decode('utf-8'))  # Place the message onto the queue

                except Exception as e:
                    logging.error(f"Error reading from socket: {e}")
        except KeyboardInterrupt:
            logging.info('Reader process interrupted and shutting down.')
    def dump_queue_blocking(self):
        try:
            while True:
                if not reader_queue.empty():
                    message = reader_queue.get(timeout=2)
                    logging.info(f"Received message from queue: {message}")
        except KeyboardInterrupt:
            pass
    
    def get_next_off_queue(self, timeout):
        if not reader_queue.empty():
            message = reader_queue.get(timeout=timeout)
        else: message = ''
        return message
    
    def send_message(self, destination, message):
        self.amq_con.send_stomp_message('SEND', {'destination': destination, 'content-type': 'text/plain'}, message)

class stomp_connection:
    def __init__(self, hostname: str, port: int):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((hostname, port))
            logging.info(f'Connected to socket at {hostname}:{port}')
        except Exception as e:
            logging.error(f"Failed to connect to {hostname}:{port}: {e}")
            raise

    def start_comms(self):
        # Pass the socket and queues directly to external processes
        self.writer_process = multiprocessing.Process(target=activemq.writer, args=(self.sock, writer_queue))
        self.reader_process = multiprocessing.Process(target=activemq.reader, args=(self.sock, reader_queue))
        self.writer_process.start()
        self.reader_process.start()

    def send_stomp_message(self, command: str, headers: dict, body: str):
        """
        Sends a STOMP message with the specified command, headers, and body.
        """
        message = f'{command}\r\n'
        if headers:
            for key, value in headers.items():
                message += f'{key}:{value}\r\n'
        message += '\r\n'
        if body:
            message += f'{body}\r\n'
        message += '\x00'
        logging.info(f'Queueing STOMP message: {message}')
        writer_queue.put(message)

    def close(self):
        writer_queue.put('STOP')
        self.writer_process.join()
        self.reader_process.terminate()
        self.reader_process.join()
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        logging.info('Socket Closed')