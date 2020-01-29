from http.server import BaseHTTPRequestHandler, HTTPServer
import signal
import sys
import json
import pandas as pd
import io
import numpy as np

df = pd.DataFrame()
img_not_found = open('404_page_not_found.jpg', 'rb').read()


class WmtsUseful():

    @staticmethod
    def get_altitude_from_zoom(zoom):
        return 591657550.5 / (2 ** (zoom - 1))

    @staticmethod
    def get_most_near_tile_i(df, latitude, longitude):
        def get_d(x1, y1, x2, y2):
            return np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

        return int(
            df.loc[
                get_d(x1=df['latitude'], y1=df['longitude'], x2=latitude, y2=longitude)
                ==
                min(get_d(x1=df['longitude'], y1=df['latitude'], x2=longitude, y2=latitude))
                ] \
                ['id']
        )


class HttpProcessor(BaseHTTPRequestHandler):
    def _send_wmts_response(self, errorCode, data, content_type):
        self.send_response(200)
        self.send_header('content-type', content_type)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path_splited = self.path.split('/')

        if path_splited[1] != 'tile':
            self._send_wmts_response(
                errorCode=200,
                data='page not exist'.encode(),
                content_type='text/plain'
            )
        else:
            request_prm = {}
            request_prm['altitude'] = WmtsUseful.get_altitude_from_zoom(int(path_splited[2]))
            request_prm['longitude'] = int(path_splited[3]) / 10_000
            request_prm['latitude'] = int(path_splited[4].split('.')[0]) / 10_000

            print('request_prm: altitude = %d,longitude = %d, latitude = %d' % (
                request_prm['altitude'], request_prm['longitude'], request_prm['latitude']
            ))

            if request_prm['altitude'] < df['altitude'].min():

                tile_i = WmtsUseful.get_most_near_tile_i(
                    df=df,
                    longitude=request_prm['longitude'],
                    latitude=request_prm['latitude']
                )

                print('tile_i = %d' % tile_i)

                self._send_wmts_response(
                    errorCode=200,
                    data=df['data'][tile_i],
                    content_type='image/png'
                )
            else:
                self._send_wmts_response(
                    errorCode=200,
                    data=img_not_found,
                    content_type='image/png'
                )


def signal_handler(sig, frame):
    print('Exiting server')
    sys.exit(0)


def get_df(tile_dir, img_size):
    import exif_parser as ep
    import os
    from PIL import Image

    tile_dir_abs = os.path.abspath(tile_dir)

    img_names = os.listdir(tile_dir_abs)
    img_names.sort()
    img_names = img_names[0:5]

    latitudes = []
    longitudes = []
    altitudes = []
    ids = []
    data = []

    for id, img_name in enumerate(img_names):
        img_gps = ep.get_gps_info(Image.open('%s/%s' % (tile_dir_abs, img_name)))
        ids.append(id)
        latitudes.append(img_gps['Latitude'])
        longitudes.append(img_gps['Longitude'])
        altitudes.append(img_gps['Altitude'])

        img = Image.open('%s/%s' % (tile_dir_abs, img_name))
        img.thumbnail(img_size)

        imgByteArr = io.BytesIO()
        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()

        data.append(imgByteArr)

    return pd.DataFrame(
        {
            'id': ids,
            'img_name': img_names,
            'latitude': latitudes,
            'longitude': longitudes,
            'altitude': altitudes,
            'data': data
        })


def main():
    # parsing arguments
    with open('config.json') as fp:
        config_dict = json.load(fp)

    global df
    print('rendering images...')
    df = get_df(config_dict['TILE_DIR'], config_dict['IMG_SIZE'])
    print('df.size = %d' % df.size)

    signal.signal(signal.SIGINT, signal_handler)
    print('SIGINT handler created')

    serv = HTTPServer(('', config_dict['PORT']), HttpProcessor)
    print('Requests expected on %d port\nWMTS server running...' % config_dict['PORT'])

    serv.serve_forever()


if __name__ == '__main__':
    main()
