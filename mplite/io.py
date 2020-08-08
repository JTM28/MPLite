import socket
import struct
import sys
import ast

from typing import Union


def _recvall(conn: socket.socket, n: int) -> Union[bytes, None]:
    """
    Helper Function to ensure the entire message is received from a socket connection

    :param conn: The client connection accepted via socket.accept
    :param n: The size of the message in bytes
    :return: Message in bytes if not None else None
    """
    data = bytearray()
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data



def recv(conn: socket.socket) -> Union[dict, str, Exception]:
    """
    Method for receiving a variable amount of a data via socket

    :param conn: The client socket connection
    :return:
    """
    rawsize = _recvall(conn, 4)  # Retrieve the Message Size
    size = struct.unpack('>I', rawsize)[0]  # Unpack Message (Big-Endian Unsigned Int)
    msg = _recvall(conn, size)

    try:
        # Try to Evaluate Message as Dict
        return ast.literal_eval(msg.decode('utf-8'))

    except SyntaxError:
        return str(msg.decode('utf-8'))

    except Exception as e:
        print(e)


def sendall(conn: socket.socket, data: str.encode):
    """
    :param conn: The socket connection
    :param data: The data as a string UTF-8 Encoded
    """
    # Pack Data W/ Big-Endian Unsigned Int (Standard Byte Size 4)
    packed = struct.pack('>I', len(data)) + data
    conn.sendall(packed)



def connection(port: int) -> socket.socket:
    """
    Create a new connection by initializing a new TCP socket

    :param port: The port to bind the socket to

    :return: A new instance of a TCP socket with its own FD
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', port))
    sock.listen(10000)
    return sock


def from_fd(fd: socket.socket.fileno):
    return socket.socket(fileno=fd)



def _winsocket_to_fd(conn: socket.socket):
    return socket.socket


# def socket_to_fd()




