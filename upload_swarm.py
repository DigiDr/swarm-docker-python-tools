import requests
import os
import json
import time
import logging
import threading
import queue
import argparse

START_NODE = 1
END_NODE = 10
START_NODE_PORT = 1633  # first nodes port e.g. 1633
PORT_INTERVAL = 100

STAMP_AMOUNT = 10000000
STAMP_DEPTH = 21

DEBUG_ENDPOINT = 'http://localhost'
LOG_FILES_DIR = '../temp_files/'  # used for temp file creation and logging. Use LOG_FILES_DIR = '' for current DIR.

NUM_THREADS = 10  # number of concurrent operations
FILE_SIZE = 1073741824  # 1073741824 bytes = 1G, 5368709120 = 5G
REPEAT = 1  # number of times to repeat job - i.e. number of uploads per NODE.

GAS_PRICE_WEI = '1000000000000' #for stamp buying

parser = argparse.ArgumentParser(description='STAMPS/PUSH/CHECK FILES')

parser.add_argument('--stamps', action='store_true', help="check and buy STAMPS.")
parser.add_argument('--push', action='store_true', help="PUSH random files to nodes.")
parser.add_argument('--check', action='store_true', help="CHECK status.")
args = parser.parse_args()


# For nodes numbered START_NODE to END_NODE, creates NUM_THREADS threads and queues uploads of random files with size FILE_SIZE. Repeats REPEAT times.
# Calculate relevant ports for START_NODE to END_NODE using START_PORT and PORT_INTERVAL.
# Logs uploads to 'upload_log.json' in LOG_FILES_DIR with JSON formatting for use in other functions.


def init_dict():
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


def node_to_port(node):
    zero_node_port = START_NODE_PORT - PORT_INTERVAL  # node 0 - so - PORT_INTERVAL from node 1 port
    return zero_node_port + (node * PORT_INTERVAL)


def check_init_stamps():
    if not os.path.exists(LOG_FILES_DIR + 'stamp_dict.json'):
        logging.info("No stamp register found: creating.")
        init_dict = {}
        for node in range(START_NODE, END_NODE + 1):
            init_dict[str(node)] = {}
        with open(LOG_FILES_DIR + 'stamp_dict.json', 'w') as file:
            json.dump(init_dict, file)
            file.close()
        logging.info("Wrote initial stamp dict file.")
        return False
    else:
        logging.info("Stamp dict detected..")
        return True


def confirm_stamps(node):
    stamp_dict = load_stamps()
    if stamp_dict[str(node)]:
        logging.info("NODE: {} - found STAMPS, continuing".format(node))
        return True
    else:
        logging.info("NODE: {} - no STAMPS found, skipping".format(node))
        return False


def check_buy_stamps_node(node):
    global lock
    lock.acquire()

    stamp_dict = load_stamps()
    port = node_to_port(node)

    # check node for stamps and update dict
    try:
        response = requests.get(DEBUG_ENDPOINT + ":{}/stamps".format(port))
        if response.json()['stamps'] is None:
            stamp_dict[str(node)] = ""

        elif response.json()['stamps'] is not None:
            if response.json()['stamps'][-1]['utilization'] == STAMP_AMOUNT:
                stamp_dict[str(node)] = ""

            else:
                batch_id = response.json()['stamps'][-1]['batchID']
                stamp_dict[str(node)] = batch_id
                logging.info("NODE: {} - updated stamps".format(node, STAMP_AMOUNT))

        save_stamps(stamp_dict)

    except Exception as e:
        logging.info("NODE: {} - FAIL - couldn't check stamps endpoint: {}".format(node, e))

    lock.release()

    if not stamp_dict[str(node)]:
        port = node_to_port(node)
        try:
            logging.info("NODE: {} - buying {} stamps".format(node, STAMP_AMOUNT))
            headers = {'gas-price': GAS_PRICE_WEI}
            r = requests.post(DEBUG_ENDPOINT + ":{}/stamps/{}/{}".format(port, STAMP_AMOUNT, STAMP_DEPTH),
                              headers=headers)
            if r.status_code == 404 or r.status_code == 500:
                raise Exception()

            logging.info("NODE: {} - bought {} stamps".format(node, STAMP_AMOUNT))
            batch_id = r.json()['batchID']

            # it may have taken a while to buy stamps, and because we are in parrallel, other stamps may have been updated in the meantime, so reload, update and resave.
            lock.acquire()

            stamp_dict = load_stamps()
            stamp_dict[str(node)] = batch_id
            save_stamps(stamp_dict)

            lock.release()

        except Exception as e:
            logging.info("NODE: {} - FAIL - couldn't buy stamps: {}".format(node, e))


def load_stamps():
    with open(LOG_FILES_DIR + 'stamp_dict.json', 'r') as file:
        stamp_dict = json.load(file)
        file.close()
    return stamp_dict


def save_stamps(stamp_dict):
    with open(LOG_FILES_DIR + 'stamp_dict.json', 'w') as file:
        json.dump(stamp_dict, file)
        file.close()


def gen_random_file(node, size):
    logging.info("NODE: {} - generating {} MB file".format(node, size / 1048576))
    int_time = int(time.time())
    filename = "node_{}_timestamp_{}".format(str(node), int_time)
    with open(LOG_FILES_DIR + 'node_{}_timestamp_{}'.format(str(node), int_time), 'wb') as f:
        f.write(os.urandom(size))  # 1073741824 = 1G, 5368709120 = 5G
    return filename


def upload(node, filename, size, batch_id):
    port = node_to_port(node)

    try:

        data = open(LOG_FILES_DIR + filename, 'rb')
        headers = {'Swarm-Postage-Batch-Id': '{}'.format(batch_id), 'swarm-collection': 'false',
                   'Content-Type': 'application/octet-stream'}
        r = requests.post(url=DEBUG_ENDPOINT + ":{}/bzz".format(port), data=data, headers=headers)
        logging.info("NODE: {} - uploading file {}".format(node, filename))

        logging.info(
            "NODE: {} - uploaded file: {} with reference: {} and tag: {}".format(node, filename, r.json()['reference'],
                                                                                 r.headers['Swarm-Tag']))

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


def buy_stamps_create_queue():
    stamp_q = queue.Queue(maxsize=0)
    return stamp_q


def buy_stamps_start_threads(stamp_q, num_threads):
    logging_format = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=logging_format, level=logging.INFO)  # .DEBUG etc

    check_init_stamps()

    for node in range(START_NODE, END_NODE + 1):
        stamp_q.put(node)

    for stamp_t in range(num_threads):
        logging.info("THREAD: {} - starting ".format(stamp_t))
        worker = threading.Thread(target=stamp_buy_threaded, args=(stamp_q, stamp_t))
        worker.setDaemon(True)
        worker.start()


def gen_upload_create_queue():
    q = queue.Queue(maxsize=0)
    return q


def gen_upload_start_threads(q, num_threads, size):
    logging_format = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=logging_format, level=logging.INFO)  # .DEBUG etc

    init_dict()

    # uploading
    for i in range(REPEAT):
        for node in range(START_NODE, END_NODE + 1):
            if confirm_stamps(node):
                q.put(node)
            else:
                continue

    for t in range(num_threads):
        logging.info("THREAD: {} - starting ".format(t))
        worker = threading.Thread(target=gen_upload_threaded, args=(q, t, size))
        worker.setDaemon(True)
        worker.start()
    return q


def stamp_buy_threaded(stamp_q, stamp_t):
    while not stamp_q.empty():
        node = stamp_q.get()
        try:
            logging.info("STAMP THREAD {} - checking stamps on NODE {}".format(stamp_t, node))
            check_buy_stamps_node(node)
        except Exception as e:
            logging.info("NODE: {} - STAMP purchase failed: {}".format(node, e))
    stamp_q.task_done()


def gen_upload_threaded(q, t, size):
    stamp_dict = load_stamps()

    while not q.empty():
        node = q.get()
        batch_id = stamp_dict[str(node)]
        try:
            logging.info("THREAD {} - requested job on NODE {}".format(t, node))
            filename = gen_random_file(node, size)
            file_ref_tag = upload(node, filename, size, batch_id)
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
        done = []
        for upload in ref_tag_dict[str(node)].keys():
            reference, tag_uid = ref_tag_dict[str(node)][upload]['ref'], ref_tag_dict[str(node)][upload]['tag']
            response = requests.get(DEBUG_ENDPOINT + ":{}/tags/{}".format(port, tag_uid))
            if response.json()['total'] == response.json()['synced']:
                done.append({node: reference})
            print("NODE: {}: {}".format(node, response.json()))

    print(done)


def check_status_all_uploads():
    print("Checking all uploads on ALL nodes: ")
    with open(LOG_FILES_DIR + 'upload_log.json', 'r') as file:
        ref_tag_dict = json.load(file)
        file.close()

    all_uploads = []
    for node in range(START_NODE, END_NODE + 1):
        for upload in ref_tag_dict[str(node)].keys():
            pass

    for node in range(START_NODE, END_NODE + 1):
        port = node_to_port(node)
        done = []
        for upload in ref_tag_dict[str(node)].keys():
            reference, tag_uid = ref_tag_dict[str(node)][upload]['ref'], ref_tag_dict[str(node)][upload]['tag']
            response = requests.get(DEBUG_ENDPOINT + ":{}/tags/{}".format(port, tag_uid))
            if response.json()['total'] == response.json()['synced']:
                done.append({node: reference})
            print("NODE {}: {}".format(node, response.json()))

    print(done)


def pull_file(node, ref):
    #TODO
    pass


if __name__ == "__main__":
    if args.stamps == True:
        lock = threading.Lock()
        stamp_q = buy_stamps_create_queue()
        buy_stamps_start_threads(stamp_q, NUM_THREADS)
        stamp_q.join()

    if args.push == True:
        lock = threading.Lock()
        q = gen_upload_create_queue()
        gen_upload_start_threads(q, NUM_THREADS, FILE_SIZE)

        q.join()
    if args.check == True:
        check_status_all_tags()
