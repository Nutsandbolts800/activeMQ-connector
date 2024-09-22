import logging
from amq import activemq

if __name__ == '__main__':
    # Initialize ActiveMQ and subscribe
    a = activemq('localhost', 61613)
    a.subscribe('foo.bar')

    a.dump_queue_blocking()

    # Gracefully stop all child processes
    a.unsub_and_close()
    logging.info("Shutdown complete.")
