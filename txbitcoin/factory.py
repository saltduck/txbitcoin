from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.python import log

from txbitcoin.protocols import BitcoinProtocol


class BitcoinClientFactory(ReconnectingClientFactory):
    initialDelay = 0.1
    protocol = BitcoinProtocol
    
    def __init__(self):
        self.client = None
        self.addr = None        
        self.deferred = Deferred()

    def buildProtocol(self, addr):
        self.client = self.protocol()
        self.addr = addr
        self.client.factory = self
        self.resetDelay()
        return self.client

    def clientConnectionLost(self, connector, reason):
        log.msg("Lost connection to %s - %s" % (self.addr, reason))
        self.client = None
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.msg("Connection failed to %s - %s" % (self.addr, reason))
        self.client = None
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def connectionMade(self):
        # Only fire deferred after the first connection has been made.
        # This is used in the ConnectedYamClient to keep track of when
        # all factories have connected so that ConnectedYamClient.connect()
        # can return a deferred list of these deferreds.
        log.msg("Connection made")
        if self.deferred is not None:
            self.deferred.callback(self)
            self.deferred = None
