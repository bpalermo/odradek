"""The protoc embedded in grpcio-tools, run only for its gRPC Python plugin."""

import sys

from grpc_tools import protoc

if __name__ == "__main__":
    sys.exit(protoc.main(sys.argv))
