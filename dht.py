from chord import Local, Daemon, repeat_and_sleep, inrange
from remote import Remote
from address import Address
import json

class DHT(object):
    def __init__(self, local_address, remote_address=None):
        self.local_ = Local(local_address, remote_address)

        # Register commands for 'set' and 'get'
        self.local_.register_command("set", self._set)
        self.local_.register_command("get", self._get)

        # Initialize data and daemons
        self.data_ = {}
        self.shutdown_ = False
        self.daemons_ = {}
        self.daemons_['distribute_data'] = Daemon(self, 'distribute_data')
        self.daemons_['distribute_data'].start()

        # Start the Chord node
        self.local_.start()

    def shutdown(self):
        # Shutdown the Chord node
        self.local_.shutdown()
        self.shutdown_ = True

    # Command handler for 'get' operation
    def _get(self, request):
        try:
            data = json.loads(request)
            # Check if the key is present locally
            return json.dumps({'status': 'ok', 'data': self.get(data['key'])})
        except Exception:
            return json.dumps({'status': 'failed'})

    # Command handler for 'set' operation
    def _set(self, request):
        try:
            data = json.loads(request)
            key = data['key']
            value = data['value']
            # Set key-value pair
            self.set(key, value)
            return json.dumps({'status': 'ok'})
        except Exception:
            return json.dumps({'status': 'failed'})

    def get(self, key):
        try:
            return self.data_[key]
        except Exception:
            # If the key is not within our range, find it in successors
            suc = self.local_.find_successor(hash(key))
            if self.local_.id() == suc.id():
                return None
            try:
                response = suc.command('get %s' % json.dumps({'key': key}))
                if not response:
                    raise Exception
                value = json.loads(response)
                if value['status'] != 'ok':
                    raise Exception
                return value['data']
            except Exception:
                return None

    def set(self, key, value):
        # Store key-value pair
        self.data_[key] = value

    @repeat_and_sleep(5)
    def distribute_data(self):
        to_remove = []
        keys = self.data_.keys()
        for key in keys:
            if self.local_.predecessor() and \
               not inrange(hash(key), self.local_.predecessor().id(1), self.local_.id(1)):
                try:
                    node = self.local_.find_successor(hash(key))
                    node.command("set %s" % json.dumps({'key': key, 'value': self.data_[key]}))
                    to_remove.append(key)
                    print("migrated")
                except socket.error:
                    print("error migrating")
                    pass
        for key in to_remove:
            del self.data_[key]
        return True

# Function to create a DHT
def create_dht(lport):
    laddress = map(lambda port: Address('127.0.0.1', port), lport)
    r = [DHT(laddress[0])]
    for address in laddress[1:]:
        r.append(DHT(address, laddress[0]))
    return r

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        dht = DHT(Address("127.0.0.1", sys.argv[1]))
    else:
        dht = DHT(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
    input("Press any key to shutdown")
    print("shutting down..")
    dht.shutdown()
