__author__ = 'aub3'
from google.protobuf.internal.decoder import _DecodeVarint
from google.protobuf.internal.encoder import _VarintEncoder
"""
Modified version of https://github.com/indygreg/zippylog/blob/master/lib/py/zippylog/stream.py
"""


class Stream:
    def __init__(self, fh, version=1, is_empty=False, read_mode=True):
        self.fh = fh
        self.is_empty = is_empty
        if read_mode == True:
            version = ord(self.fh.read(1))
        self.varint_encoder = _VarintEncoder()
        self.varint_decoder = _DecodeVarint

    def write(self,encoded):
        if self.is_empty:
            self.fh.write(chr(0x01))
            self.is_empty = False
        l = len(encoded)
        self.varint_encoder(self.fh.write, l)
        self.fh.write(encoded)

    def flush(self):
        '''Flush the underlying stream.'''
        return self.fh.flush()

    def read(self):
        buf = self.fh.read(4)
        if len(buf):
            (size, pos) = self.varint_decoder(buf, 0)
            buf = buf[pos:] + self.fh.read(size - 4 + pos)
            return buf

    def get_messages(self):
        while True:
            s = self.read()
            if s:
                yield s
            else:
                self.close()
                return

    def close(self):
        self.fh.close()