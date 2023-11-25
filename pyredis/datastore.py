class Datastore:
    def __init__(self):
        self._data = dict()
        # self._lock = Lock()

    def __getitem__(self, key):
        value = self._data[key]
        return value

    def __setitem__(self, key, value):
        self._data[key] = value
