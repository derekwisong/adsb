import geo
import logging

log = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    log.info("Running the ADSB check script")
    geo.test()
