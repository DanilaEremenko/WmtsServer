from http.server import BaseHTTPRequestHandler, HTTPServer
from PIL import Image
import signal
import sys
import json
import pandas as pd
import numpy as np

img = open('Datasets/Cats/1.jpg', 'rb')
img_bytes = img.read()
df = {}


class HttpProcessor(BaseHTTPRequestHandler):
    def do_GET(self):
        global df
        self._send_wmts_response(
            errorCode=200,
            msg="WmtsServer: I got your request server got you request, but I'm still not implemented",
            data=img_bytes)
        print("GET Request got")

    def _send_wmts_response(self, errorCode, msg, data):
        self.send_response(200)
        self.send_header('content-type', 'image/png')
        self.end_headers()
        self.wfile.write(data)


def signal_handler(sig, frame):
    print('Exiting server')
    sys.exit(0)


def get_df(tile_dir):
    import exif_parser as ep
    import os
    from PIL import Image

    tile_dir_abs = os.path.abspath(tile_dir)

    img_names = os.listdir(tile_dir_abs)
    img_names.sort()

    latitudes = []
    longitudes = []
    altitudes = []
    ids = []

    for id, img_name in enumerate(img_names):
        img_gps = ep.get_gps_info(Image.open("%s/%s" % (tile_dir_abs, img_name)))
        ids.append(id)
        latitudes.append(img_gps["Latitude"])
        longitudes.append(img_gps["Longitude"])
        altitudes.append(img_gps["Altitude"])

    return pd.DataFrame(
        {
            'id': ids,
            'img_name': img_names,
            'latitude': latitudes,
            'longitude': longitudes,
            'altitude': altitudes
        })


def main():
    # parsing arguments
    with open('config.json') as fp:
        config_dict = json.load(fp)

    global df
    df = get_df(config_dict['TILE_DIR'])

    signal.signal(signal.SIGINT, signal_handler)
    print("SIGINT handler created")

    serv = HTTPServer(('', config_dict['PORT']), HttpProcessor)
    print("Requests expected on %d port\nWMTS server running..." % config_dict['PORT'])

    serv.serve_forever()


if __name__ == '__main__':
    main()
