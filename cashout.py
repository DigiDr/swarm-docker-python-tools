import requests
import time

#Configure start and end nodes. If running 1 node set START_NODE = 1 and END_NODE = 1

START_NODE = 1
END_NODE = 10

START_NODE_PORT = 1635  # first nodes port e.g. 1635
PORT_INTERVAL = 100

MIN_AMOUNT = 10000000000000

DEBUG_ENDPOINT = 'http://localhost'


def node_to_port(node):
    zero_node_port = START_NODE_PORT - PORT_INTERVAL  # node 0 - so - PORT_INTERVAL from node 1 port
    return zero_node_port + (node * PORT_INTERVAL)


def get_peers(port):
    response = requests.get(DEBUG_ENDPOINT + ":{}/chequebook/cheque".format(port))
    if response.status_code == 404 or response.status_code == 500:
        print("Get peers: {}".format(response.status_code))
        return 0
    last_cheques = response.json()['lastcheques']
    peers = []
    # print(last_cheques)
    for node in last_cheques:
        if 'peer' in node:
            peers.append(node['peer'])
    # print(peers)
    return peers


def get_cumulative_payout(port, peer, peer_index):
    response = requests.get(DEBUG_ENDPOINT + ":{}/chequebook/cheque/{}".format(port, peer))
    if response.status_code == 404 or response.status_code == 500:
        print("Get cumulative: {}".format(response.status_code))
        return 0
    lastreceived = response.json()['lastreceived']
    if lastreceived == None:
        # print(lastreceived)
        return 0
    else:
        # print(lastreceived['payout'])
        return lastreceived['payout']


def get_last_cashed_payout(port, peer, peer_index):
    response = requests.get(DEBUG_ENDPOINT + ":{}/chequebook/cashout/{}".format(port, peer))
    # print(response.json())
    # print(response)
    cashout = response.json()
    if response.status_code == 404 or response.status_code == 500:
        # print("Get last cashed: {}".format(response.status_code))
        # print("** Peer {}: {} **".format(peer_index, cashout['message']))
        return 0
    elif cashout['cumulativePayout'] == None:
        return 0
    else:
        return cashout['cumulativePayout']


def get_uncashed_amount(port, peer, peer_index):
    cumulative_payout = get_cumulative_payout(port, peer, peer_index)
    if cumulative_payout == 0:
        # print(cumulative_payout)
        return cumulative_payout
    else:
        cashed_payout = get_last_cashed_payout(port, peer, peer_index)
        uncashed_amount = cumulative_payout - cashed_payout
        # print(uncashed_amount)
        return uncashed_amount


def cashout(port, peer, peer_index):
    txn_post = requests.post(DEBUG_ENDPOINT + ":{}/chequebook/cashout/{}".format(port, peer))
    txn_hash = txn_post.json()

    if txn_post.status_code == 500:
        print("Error - cannot cash cheque!")
        return
    else:
        print("** Peer {}: {}... cashout in txn https://goerli.etherscan.io/tx/{} **".format(peer_index, peer[0:8],
                                                                                             txn_hash[
                                                                                                 'transactionHash']))

    count = 0
    while True:
        response = requests.get(DEBUG_ENDPOINT + ":{}/chequebook/cashout/{}".format(port, peer))
        cashout_result = response.json()
        if cashout_result['result'] == None:
            print("Waiting... ", end="", flush=True)
            time.sleep(5)
            count += 1
            if count == 12: #try for 1 minute and move on if txn not successful.
                print("Moving on ... Check goerli for issues...")
                break
            continue
        else:
            print("Successful cashout")
            return


def cashout_all_peers(port):
    peers = get_peers(port)
    print("Found {} peers... ".format(len(peers)))
    for peer_index, peer in enumerate(peers):
        uncashed_amount = get_uncashed_amount(port, peer, peer_index + 1)
        print("[ Peer {}: {}... Uncashed: {} ] ... ".format(peer_index + 1, peer[0:8], uncashed_amount), end="",
              flush=True)
        if uncashed_amount > MIN_AMOUNT:
            print("Cashing out!")
            cashout(port, peer, peer_index + 1)
        else:
            print("Next.")
    return


def run_cashout_all_nodes():
    for node in range(START_NODE, END_NODE + 1):
        port = node_to_port(node)
        try:
            print("Running cashout on node: {} ...".format(node), end="", flush=True)
            requests.get('http://localhost:{}'.format(port))
            cashout_all_peers(port)
        except:
            print("Error on node {}".format(node))
            continue


if __name__ == "__main__":
    run_cashout_all_nodes()
