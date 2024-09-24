import socket
import threading
import logging
import queue

class activemq:
    def __init__(self, hostname: str, port: int):
        self.amq_con = StompConnection(hostname, port)
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
        try:
            message = self.amq_con.reader_queue.get(timeout=timeout)
            return message
        except queue.Empty:
            return ''
    
    def send_message(self, destination, message):
        self.amq_con.send_stomp_message(
            'SEND', {'destination': destination, 'content-type': 'text/plain'}, message
        )

class StompConnection:
    def __init__(self, hostname: str, port: int):
        self.writer_queue = queue.Queue()
        self.reader_queue = queue.Queue()
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((hostname, port))
            logging.info(f'Connected to socket at {hostname}:{port}')
        except Exception as e:
            logging.error(f"Failed to connect to {hostname}:{port}: {e}")
            raise

    def start_comms(self):
        # Start the writer and reader threads
        self.writer_thread = threading.Thread(target=self.writer)
        self.reader_thread = threading.Thread(target=self.reader)
        self.writer_thread.start()
        self.reader_thread.start()

    def writer(self):
        logging.info('Starting Writer Thread')
        try:
            while True:
                try:
                    message = self.writer_queue.get(timeout=1)
                    if message == 'STOP':
                        logging.info('Writer thread received stop signal.')
                        break
                    self.sock.sendall(message.encode('utf-8'))
                    logging.info('Queued message sent successfully.')
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f'Failed to send message: {e}')
                    break
        except KeyboardInterrupt:
            logging.info('Writer thread interrupted and shutting down.')

    def reader(self):
        logging.info('Starting Reader Thread')
        buffer = b""
        try:
            while True:
                try:
                    data = self.sock.recv(1024)
                    if not data:
                        logging.warning("Socket closed or no data received.")
                        break
                    buffer += data
                    logging.debug(f"Buffer after receiving data: {buffer!r}")
                    while b'\x00' in buffer:
                        message, buffer = buffer.split(b'\x00', 1)
                        logging.debug(f"Processed message: {message.decode('utf-8')}")
                        self.reader_queue.put(message.decode('utf-8'))
                except Exception as e:
                    logging.error(f"Error reading from socket: {e}")
                    break
        except KeyboardInterrupt:
            logging.info('Reader thread interrupted and shutting down.')

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

    def close(self):
        self.writer_queue.put('STOP')
        self.writer_thread.join()
        self.reader_thread.join()
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
            message = amq.get_next_message(timeout=2)
            if message:
                logging.info(f"Received message from queue: {message}")
    except KeyboardInterrupt:
        logging.info("Main thread interrupted, shutting down.")

    # Gracefully stop all threads
    amq.unsub_and_close()
    logging.info("Shutdown complete.")
