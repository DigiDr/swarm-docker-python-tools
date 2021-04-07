import requests
import os
import json
import time
import logging
import threading
import queue

START_NODE = 1
END_NODE = 10
START_NODE_PORT = 1633  # first node API port e.g. 1633
PORT_INTERVAL = 100

DEBUG_ENDPOINT = 'http://localhost'
LOG_FILES_DIR = '../temp_files/'  # used for temp file creation and logging. Use LOG_FILES_DIR = '' for current DIR.

NUM_THREADS = 10  # number of concurrent upload threads.
FILE_SIZE = 1073741824  # 1073741824 bytes = 1G, 5368709120 = 5G
REPEAT = 1  # number of times to repeat job - i.e. number of uploads per NODE.


# For nodes numbered START_NODE to END_NODE, creates NUM_THREADS threads and queues uploads of random files with size FILE_SIZE. Repeats REPEAT times.
# Calculate relevant ports for START_NODE to END_NODE using START_PORT and PORT_INTERVAL.
# Logs uploads to 'upload_log.json' in LOG_FILES_DIR with JSON formatting for use in other functions.


def init_dict():
    #Initiate a log
    if not os.path.exists(LOG_FILES_DIR + 'upload_log.json'):
        logging.info("No log found: creating.")
        init_dict = {}
        for node in range(START_NODE, END_NODE + 1):
            init_dict[str(node)] = {}
        with open(LOG_FILES_DIR + 'upload_log.json', 'w') as file:
            json.dump(init_dict, file)
            file.close()
        logging.info("Wrote initial log file.")
    else:
        logging.info("Log file detected..")


def save_to_dict():
    with open(LOG_FILES_DIR + 'upload_log.json', 'r') as infile:
        ref_tag_dict = json.load(infile)
        infile.close()
    return ref_tag_dict


def node_to_port(node):
    zero_node_port = START_NODE_PORT - PORT_INTERVAL  # node 0 - so - PORT_INTERVAL from node 1 port
    return zero_node_port + (node * PORT_INTERVAL)


def gen_random_file(node, size):
    logging.info("NODE: {} - generating {} MB file".format(node, size / 1048576))
    int_time = int(time.time())
    filename = "node_{}_timestamp_{}".format(str(node), int_time)
    with open(LOG_FILES_DIR + 'node_{}_timestamp_{}'.format(str(node), int_time), 'wb') as f:
        f.write(os.urandom(size))  # 1073741824 = 1G, 5368709120 = 5G
    return filename


def upload(node, filename, size):
    port = node_to_port(node)

    try:
        #POST
        file = {'file': (filename, open(LOG_FILES_DIR + filename, 'rb'), 'Content-Type: application/json')}
        logging.info("NODE: {} - uploading file {}".format(node, filename))

        r = requests.post(DEBUG_ENDPOINT + ":{}/files".format(port), files=file)
        logging.info("NODE: {} - uploaded file: {} with reference: {} and tag: {}".format(node, filename, r.json()['reference'],
                                                                                 r.headers['Swarm-Tag']))
        # create dict object
        file_ref_tag = {filename: {'size': size, 'ref': r.json()['reference'], 'tag': r.headers['Swarm-Tag']}}

        global lock
        lock.acquire()

        with open(LOG_FILES_DIR + 'upload_log.json', 'r') as file:
            ref_tag_dict = json.load(file)
            file.close()

        with open(LOG_FILES_DIR + 'upload_log.json', 'w') as file:
            ref_tag_dict[str(node)].update(file_ref_tag)
            json.dump(ref_tag_dict, file)
            file.close()

        lock.release()
        return file_ref_tag

    except Exception as e:
        logging.info("NODE: {} - FAIL - upload failed on node: {}".format(node, e))


def gen_upload_create_queue():
    q = queue.Queue(maxsize=0)
    return q


def gen_upload_start_threads(q, num_threads, size):
    logging_format = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=logging_format, level=logging.INFO)  # .DEBUG etc

    init_dict()

    for i in range(REPEAT):
        for node in range(START_NODE, END_NODE + 1):
            q.put(node)

    for t in range(num_threads):
        logging.info("THREAD: {} - starting ".format(t))
        worker = threading.Thread(target=gen_upload_threaded, args=(q, t, size))
        worker.setDaemon(True)
        worker.start()
    return q


def gen_upload_threaded(q, t, size):
    while not q.empty():
        node = q.get()
        try:
            logging.info("THREAD {} - requested job on NODE {}".format(t, node))
            filename = gen_random_file(node, size)
            file_ref_tag = upload(node, filename, size)
            logging.info("NODE: {}: - {}".format(node, file_ref_tag))
            os.remove(LOG_FILES_DIR + filename)

        except Exception as e:
            logging.info("NODE: {} - FAIL - other failure on node: {}".format(node, e))

        q.task_done()


def gen_upload_all_nodes(size):
    init_dict()

    for node in range(START_NODE, END_NODE + 1):
        try:
            filename = gen_random_file(node, size)
            file_ref_tag = upload(node, filename, size)
            print(file_ref_tag)
            os.remove(LOG_FILES_DIR + filename)

        except Exception as e:
            logging.info("NODE: {} - FAIL - other failure on node: {}".format(node, e))


def check_status_all_tags():
    print("Checking tag upload status on ALL nodes: ")
    with open(LOG_FILES_DIR + 'upload_log.json', 'r') as file:
        ref_tag_dict = json.load(file)
        file.close()

    for node in range(START_NODE, END_NODE + 1):
        port = node_to_port(node)
        for upload in ref_tag_dict[str(node)].keys():
            tag_uid = ref_tag_dict[str(node)][upload]['tag']
            response = requests.get(DEBUG_ENDPOINT + ":{}/tags/{}".format(port, tag_uid))
            print(response.json())


def pull_file(node, ref):
    #TODO
    pass


if __name__ == "__main__":
    lock = threading.Lock()
    q = gen_upload_create_queue()
    gen_upload_start_threads(q, NUM_THREADS, FILE_SIZE)
    q.join()
    check_status_all_tags()
