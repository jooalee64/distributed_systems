"""
CPSC 5520, Seattle University
Jooa Lee 
Lab3 fxp_bytes_subcriber
"""

from array import array 
import ipaddress
from datetime import datetime, timezone, timedelta
import struct

MICROS_PER_SECOND = 1_000_000

def deserialize_price(b: bytes):
    """
    Converts a bytes back into a float representing a price
    :param b (bytes): a byte sequence (serialized) of price
    :return : floats price
    """
    p_a = array('f') # change to 'f' for float(32 bit)
    p_a.frombytes(b[6:10]) #extract only the bytes for the price
    return p_a[0]

def serialize_address(address: tuple[str, int]):
    """
    Convert the host and port into a byte serialize hostname, posrt tuple 
    to send over a socket
    :param addres (tuple): 
    >>> serialize_address('127.0.0.1', 65534)
    b'\\x7f\\x00\\x00\\x01\\xff\\xfe'

    :return : serialized address
    """
    ip_bytes = ipaddress.ip_address(address[0]).packed
    port_bytes = address[1].to_bytes(2, byteorder="big")
    return ip_bytes + port_bytes

def deserialize_utcdatetme(b: bytes):
    """
    Convert a UTC datetime byte stream into datetime object
    :param : An 8 byte stream which contains UTC datetime in microseconds
    :return : a datetime object which represents UTC time
    """
	# unpack the data from big-endian format
    timestamp_in_micros = struct.unpack('>Q', b)[0]
    if timestamp_in_micros < 0:
        raise ValueError("Timestamp cannot be negative")
    
    epoch = datetime(1970, 1,1, tzinfo=timezone.utc)
    return epoch + timedelta(microseconds=timestamp_in_micros)
    


def demarshal_message(b:bytes):
    """
    Parse a byte stream into a list of dictionary containing the quote information

    :param b: bytes which contain multiple quotes, each formatted as 32-byte record
    :return : list of dictionary (quotes)
    """
    if len(b) % 32 != 0:
        raise ValueError(f"Byte stream length {len(b)} is  not a multiple of 32")
    
    num_quotes = len(b)//32
    quotes = []
    for x in range(num_quotes):
        quote_bytes = b[x * 32:(x + 1) * 32]
        if len(quote_bytes) != 32:
            print(f"Warning: Expected 32 bytes for a quote, got {len(quote_bytes)} bytes.")
            continue
        quote = {}
        timestamp_bytes = quote_bytes[10:18]
        quote["time"] = deserialize_utcdatetme(timestamp_bytes)
        currency1 = quote_bytes[0:3].decode("ISO-8859-1").strip()
        currency2 = quote_bytes[3:6].decode("ISO-8859-1").strip()
        # quote["cross"] = quote_bytes[0:6].decode("utf-8").strip() + "/" + quote_bytes[6:10].decode("utf-8").strip()
        # quote["cross"] = currency1 + "/" + currency2
        quote["cross"] = f"{currency1} {currency2}".strip()
        quote["exchange_rate"] = deserialize_price(quote_bytes)
        quotes.append(quote)
    return quotes         