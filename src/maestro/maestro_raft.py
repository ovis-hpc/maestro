from pysyncobj import replicated, SyncObjConsumer

class DaemonDict(SyncObjConsumer):
    def __init__(self):
        super(DaemonDict, self).__init__()
        self.__daemons = {}

    @replicated
    def clear(self):
        self.__daemons.clear()

    @replicated
    def __setitem__(self, mi, value):
        self.__daemons[mi] = value

    @replicated
    def set(self, mi, value):
        self.__daemons[mi] = value

    @replicated
    def addMaestroGroup(self, mi, data):
        self.__daemons[mi] = data

    @replicated
    def setDaemonGroup(self, mi, group, data):
        self.__daemons[mi][group] = data

    @replicated
    def setDaemon(self, mi, group, dmn, data):
        self.__daemons[mi][group][dmn] = data

    @replicated
    def updateDaemonGroup(self, mi, group, data):
        self.__daemons[mi][group].update(data)

    @replicated
    def updateDaemon(self, mi, group, dmn, data):
        self.__daemons[mi][group][dmn].update(data)

    def __getitem__(self, key):
        if key not in self.__daemons:
            return None
        return self.__daemons[key]

    def __len__(self):
        return len(self.__daemons)

    def getMaestroGroup(self, mi):
        return self.__daemons[mi]

    def getDaemonGroup(self, mi, group):
        return self.__daemons[mi][group]

    def items(self):
        return self.__daemons.items()

    def keys(self):
        return self.__daemons.keys()

    def values(self):
        return self.__daemons.values()
