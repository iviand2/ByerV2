import logging


loggy = logging.getLogger('Main Log')
loggy.setLevel(logging.INFO)
handler = logging.FileHandler('data/MainLog.txt')
formatter = logging.Formatter('\n\n\n%(name)s %(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
loggy.addHandler(handler)

