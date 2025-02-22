import logging

logging.root.setLevel(logging.INFO)
logging.basicConfig(format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("platform-connector")
