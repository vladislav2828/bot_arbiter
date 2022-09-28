import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

handler = RotatingFileHandler('my.log', maxBytes=2000_000, backupCount=3)

format = logging.Formatter('%(asctime)s  ==  %(levelname)s : %(levelno)s  ==  %(funcName)s()  ==  %(message)s')

handler.setFormatter(format)

logger.addHandler(handler)



