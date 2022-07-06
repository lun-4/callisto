import zlib
import sys

with open(sys.argv[1], "rb") as fd:
    msg = zlib.decompress(fd.read(), 15, 10490000)
    print(msg.decode())
