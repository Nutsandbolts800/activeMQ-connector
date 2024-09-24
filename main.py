import amq_threading as amq
import logging


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize ActiveMQ and subscribe
    a = amq.activemq('localhost', 61613)
    a.subscribe('main')

    try:
        while True:
            message = a.get_next_message(timeout=2)
            if message:
                logging.info(f"Received message from queue: {message}")
    except KeyboardInterrupt:
        logging.info("Main process interrupted, shutting down.")

    # Gracefully stop all child processes
    a.unsub_and_close()
    logging.info("Shutdown complete.")
