import socket
import multiprocessing
import logging
import queue

class activemq:
    def __init__(self, hostname: str, port: int):
        self.amq_con = stomp_connection(hostname, port)
        self.amq_con.start_comms()
        self.amq_con.send_stomp_message(
            'CONNECT', {'accept-version': '1.0,1.1,1.2', 'host': 'localhost'}, None
        )
        logging.info(f"STOMP connection created to {hostname}:{port}")

    def subscribe(self, destination: str):
        self.amq_con.send_stomp_message(
            'SUBSCRIBE', {'id': '0', 'destination': f'/queue/{destination}'}, None
        )
        logging.info(f'Subscribed to AMQ queue: {destination}')

    def unsub_and_close(self):
        self.amq_con.send_stomp_message('DISCONNECT', {'receipt': '9544'}, None)
        logging.info('Unsubscribed from AMQ queues')
        self.amq_con.close()

    def get_next_message(self, timeout):
        return self.amq_con.get_next_message(timeout)
    
    def send_message(self, destination, message):
        self.amq_con.send_stomp_message(
            'SEND', {'destination': destination, 'content-type': 'text/plain'}, message
        )

class stomp_connection:
    def __init__(self, hostname: str, port: int):
        self.hostname = hostname
        self.port = port
        self.writer_queue = multiprocessing.Queue()
        self.reader_queue = multiprocessing.Queue()
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((hostname, port))
            logging.info(f'Connected to socket at {hostname}:{port}')
        except Exception as e:
            logging.error(f"Failed to connect to {hostname}:{port}: {e}")
            raise

    def start_comms(self):
        # Start writer and reader processes
        self.writer_process = multiprocessing.Process(
            target=stomp_connection.writer_process_method,
            args=(self.sock, self.writer_queue)
        )
        self.reader_process = multiprocessing.Process(
            target=stomp_connection.reader_process_method,
            args=(self.sock, self.reader_queue)
        )
        self.writer_process.start()
        self.reader_process.start()

    @staticmethod
    def writer_process_method(sock, writer_queue):
        logging.info('Starting Writer Process')
        try:
            while True:
                try:
                    message = writer_queue.get(timeout=1)
                    if message == 'STOP':
                        logging.info('Writer process received stop signal.')
                        break
                    sock.sendall(message.encode('utf-8'))
                    logging.info('Queued message sent successfully.')
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f'Failed to send message: {e}')
                    break  # Exit on failure
        except KeyboardInterrupt:
            logging.info('Writer process interrupted and shutting down.')

    @staticmethod
    def reader_process_method(sock, reader_queue):
        logging.info('Starting Reader Process')
        buffer = b""
        try:
            while True:
                try:
                    data = sock.recv(1024)
                    if not data:
                        logging.warning("Socket closed or no data received.")
                        break
                    buffer += data
                    logging.debug(f"Buffer after receiving data: {buffer!r}")
                    while b'\x00' in buffer:
                        message, buffer = buffer.split(b'\x00', 1)
                        logging.debug(f"Processed message: {message.decode('utf-8')}")
                        reader_queue.put(message.decode('utf-8'))
                except Exception as e:
                    logging.error(f"Error reading from socket: {e}")
                    break
        except KeyboardInterrupt:
            logging.info('Reader process interrupted and shutting down.')

    def send_stomp_message(self, command: str, headers: dict, body: str):
        message = f'{command}\r\n'
        if headers:
            for key, value in headers.items():
                message += f'{key}:{value}\r\n'
        message += '\r\n'
        if body:
            message += f'{body}\r\n'
        message += '\x00'
        logging.info(f'Queueing STOMP message: {message}')
        self.writer_queue.put(message)

    def get_next_message(self, timeout):
        try:
            message = self.reader_queue.get(timeout=timeout)
            return message
        except queue.Empty:
            return ''

    def close(self):
        self.writer_queue.put('STOP')
        self.writer_process.join()
        self.reader_process.terminate()
        self.reader_process.join()
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        logging.info('Socket Closed')

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize ActiveMQ and subscribe
    amq = activemq('localhost', 61613)
    amq.subscribe('main')

    try:
        while True:
            message = amq.get_next_off_queue(timeout=2)
            if message:
                logging.info(f"Received message from queue: {message}")
    except KeyboardInterrupt:
        logging.info("Main process interrupted, shutting down.")

    # Gracefully stop all child processes
    amq.unsub_and_close()
    logging.info("Shutdown complete.")
