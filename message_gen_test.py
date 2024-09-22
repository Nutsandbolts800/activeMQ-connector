import logging
import time
from amq import activemq

if __name__ == '__main__':
    # Initialize ActiveMQ and subscribe
    a = activemq('localhost', 61613)
    a.subscribe('foo.bar')
    while True:
        try:
            a.send_message('foo.bar', 'ATestMessage')
            time.sleep(2)
        except KeyboardInterrupt:
            break

    # Gracefully stop all child processes
    a.unsub_and_close()
    logging.info("Shutdown complete.")
