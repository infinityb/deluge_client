import zlib
from itertools import count

import gevent
from gevent import socket, ssl
from gevent.queue import Queue
from gevent.event import AsyncResult

from . import rencode

class DelugeRPCResponse(object):
    RESPONSE = 1
    ERROR = 2
    EVENT = 3
    def __init__(self, wrapped):
        self.wrapped = wrapped

    @property
    def message_type(self):
        return self.wrapped[0]

    @property
    def is_response(self):
        return self.message_type == self.RESPONSE

    @property
    def is_error(self):
        return self.message_type == self.ERROR

    @property
    def is_event(self):
        return self.message_type == self.EVENT

    @property
    def request_id(self):
        assert self.is_response or self.is_error
        return self.wrapped[1]


class DelugeRPCRequest(object):
    def __init__(self, request_id, method_name, args, kwargs):
        self.request_id = request_id
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs
        self.invariant()

    def invariant(self):
        assert isinstance(self.request_id, int)
        assert isinstance(self.method_name, str)
        assert isinstance(self.args, tuple)
        assert isinstance(self.kwargs, dict)

    def format_message(self):
        self.invariant()
        return (self.request_id, self.method_name, self.args, self.kwargs)


def serialize_rpc_request(rpc_req):
    assert isinstance(rpc_req, DelugeRPCRequest)
    return zlib.compress(rencode.dumps((rpc_req.format_message(), )))


def deserialize_rpc_resp(buffer_):
    dobj = zlib.decompressobj()
    try:
        message = dobj.decompress(buffer_)
    except Exception as e:
        return buffer_, None
    try:
        return dobj.unused_data, rencode.loads(message)
    except Exception as e:
        return buffer_, None


class RPCMapper(object):
    def __init__(self):
        self._tags = dict()
        self._tag_gen = count()
        self._rxbuffer = b''
        self._wrbuffer = Queue()
        self._counters = {};
        self._counters['bytes_recv'] = 0;

    def __alloc_tag(self):
        request_id = next(self._tag_gen)
        result = AsyncResult()
        self._tags[request_id] = result
        return request_id, result

    def data_received(self, data):
        self._counters['bytes_recv'] += len(data)
        self._rxbuffer += data
        #print "got data: %r" % (data, )
        while True:
            self._rxbuffer, msg = deserialize_rpc_resp(self._rxbuffer)
            #print "got msg: %r" % (msg, )
            if msg is None:
                break
            self.__process_message(DelugeRPCResponse(msg))

    def get_writable(self):
        return self._wrbuffer

    def call(self, method_name, *a, **kw):
        request_id, queue = self.__alloc_tag()
        rpc_req = DelugeRPCRequest(request_id, method_name, a, kw)
        self._wrbuffer.put(serialize_rpc_request(rpc_req))
        #print "request#%d dispatched" % (request_id, )
        assert request_id in self._tags
        return queue

    def __process_message(self, msg):
        if msg.is_response or msg.is_error:
            self._tags[msg.request_id].set(msg)
            del self._tags[msg.request_id]
            #print "request#%d processed" % (msg.request_id, )
        elif msg.is_event:
            if hasattr(self, 'on_event'):
                self.on_event(msg)
        else:
            import warnings
            warnings.warn('Invalid response type %d in message %r' % (
                msg.response_type, msg))


class Client(RPCMapper):
    def __init__(self, socket):
        super(Client, self).__init__()
        self.__socket = socket

    def __read_loop(self):
        while True:
            buf = self.__socket.recv(4096)
            self.data_received(buf)

    def __write_loop(self):
        for buf in self.get_writable():
            self.__socket.send(buf)

    def start(self):
        self.__rxloop = gevent.Greenlet.spawn(self.__read_loop)
        self.__wrloop = gevent.Greenlet.spawn(self.__write_loop)


class SocketClient(Client):
    def __init__(self, address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        sock = ssl.wrap_socket(sock)
        super(SocketClient, self).__init__(sock)

    def on_event(self, msg):
        pass


