import logging
logging.basicConfig(level=logging.DEBUG)

def getLogger(name: str) -> logging.Logger:
    # bits = name.split("/")
    # filename = bits[len(bits) - 1]
    return logging.getLogger(name)
