#!/usr/bin/python2.7
import socket
import sys
import time
import struct
import datetime
import argparse
import signal
import threading
import os
import base64
import json
import errno
import MySQLdb

if sys.version < '3':
    import httplib
    from urlparse import urlparse
    import urllib
else:
    import http.client
    from urllib.parse import urlparse
    import urllib.request
    import urllib.parse
    import urllib.error


def main():
    eseq = 0
    flag = 0
    f = 0
    f_long = 0
    prev_fname = -1
    prev_fname_long = -1
    buf = ''

    global stop_time

    sock = get_socket()
    sec = 0

    while True:
        try:
            data = bytearray(b" " * 2048)
            size = sock.recv_into(data)

            if data and not buf:
                sys.stderr.write('Start capturing\n')
            buf = data[:size]
        except socket.timeout:
            sock.close()
            buf = ''
            sys.stderr.write('No data, reconnecting...\n')
            sock = get_socket()
            continue
        except socket.error as e:
            if e.errno != errno.EINTR:
                raise
            else:
                continue

        if buf[0] == 71:  # UDP 0x47
            data = buf
        else:  # RTP
            header_size = 12 + 4 * (buf[0] & 16)
            seq = (buf[2] << 8) + buf[3]

            if not flag:
                eseq = seq
                flag = 1
            if eseq != seq:
                sys.stderr.write(
                    'RTP: NETWORK CONGESTION - expected %d, received %d\n' % (eseq, seq))
                eseq = seq

            eseq += 1

            if eseq > 65535:
                eseq = 0

            data = buf[header_size:]

        if channel_data[3]:
            """ FILE CAPTURE """
            fname = datetime.datetime.now().strftime(date_format)

            if prev_fname != fname:
                if f:

                    f.close()
                    time_capture2 = int(time.time())
                    time_capture2 = str(time_capture2)
                    CaptureInfo('' + channel_data[3] + time_capture2 + '').start()

                time_capture2 = int(time.time())
                time_capture2 = str(time_capture2)
                f = open('' + channel_data[3] + time_capture2 + '', 'ab')
                sec += sec

            prev_fname = fname

            f.write(data)


class CaptureInfo(threading.Thread):

    def __init__(self, _capture_me_):
        super(CaptureInfo, self).__init__()
        self._capture_me_ = _capture_me_

    def run(self):
        try:
            time_capture = int(time.time())
            time.sleep(1)
            spawn = os.popen('mediainfo --Inform="Video;%Duration%" ' + self._capture_me_)
            stdout = spawn.read()
            cursor.execute('''INSERT into ''' + channel_data[2] + ''' (id, duration) values (%s, %s)''',
                           (time_capture, stdout.rstrip()))
            db.commit()

        except ValueError:
            pass
        except OSError as error:
            sys.stderr.write('Error:' + error.message)


class AsyncRmOldFiles(threading.Thread):

    def __init__(self):
        super(AsyncRmOldFiles, self).__init__()
        self.from_time = int(
            time.mktime((datetime.datetime.now() - datetime.timedelta(hours=args.channel)).timetuple()))

    def run(self):
        for file in os.listdir(channel_data[4]):
            if file.find('.mpg') == len(file) - 4:
                try:
                    if self.from_time > int(
                            time.mktime(datetime.datetime.strptime(file[:-4], date_format_long).timetuple())):
                        rm_file = channel_data[4] + '/' + file
                        sys.stderr.write('Deleting ' + rm_file + '\n')
                        os.remove(rm_file)
                except ValueError:
                    pass
                except OSError as error:
                    sys.stderr.write('Error deleting file ' + file + ' - ' + error.message)


def async_rm_old_files():
    AsyncRmOldFiles().start()


def signal_handler(signal, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def update_stop_time(response):
    global stop_time
    response = json.loads(response.decode("utf-8"))
    if response['results'] and response['results']['stop']:
        stop_time = response['results']['stop']


class AsyncOpenUrl(threading.Thread):

    def __init__(self, url, params, callback, method):
        super(AsyncOpenUrl, self).__init__()
        self.callback = callback
        self.method = method
        self.url = url
        self.params = params

    def run(self):
        url = urlparse(self.url)
        if sys.version < '3':
            params = urllib.urlencode(self.params)
        else:
            params = urllib.parse.urlencode(self.params)
        sys.stderr.write('Sending ' + params + ' to callback url: ' + self.url + '\n')
        if sys.version < '3':
            conn = httplib.HTTPConnection(url.hostname)
        else:
            conn = http.client.HTTPConnection(url.hostname)
        conn.putrequest(self.method, url.path)
        conn.putheader('Connection', 'close')
        if self.method in ['PUT', 'POST']:
            conn.putheader('Content-Type', 'application/x-www-form-urlencoded')
            conn.putheader('Content-Length', str(len(params)))
        if url.username:
            credentials = '%s:%s' % (url.username, url.password)
            if sys.version > '3':
                credentials = bytearray(credentials.encode("utf-8"))
            auth = base64.b64encode(credentials).decode("utf-8")
            conn.putheader("Authorization", "Basic %s" % auth)
        conn.endheaders()
        if sys.version > '3':
            params = bytearray(params.encode("utf-8"))
        conn.send(params)
        resp = conn.getresponse()
        data = resp.read()
        if sys.version < '3':
            sys.stderr.write(data + '\n')
        else:
            sys.stderr.buffer.write(data)
            sys.stderr.write('\n')
        if self.callback:
            self.callback(data)
        conn.close()


def async_open_url(url, params, callback=None, method='PUT'):
    AsyncOpenUrl(url, params, callback, method).start()


def get_socket():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((channel_data[6], channel_data[7]))
        mreq = struct.pack(">4sl", socket.inet_aton(channel_data[6]), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(10)
    except socket.error as e:
        sys.stderr.write(e.strerror + ', waiting 30s\n')
        time.sleep(30)
        return get_socket()

    return sock


if __name__ == '__main__':
    global cursor
    global db
    global channel_data

    parser = argparse.ArgumentParser(description='Stream dump. Can work with rtp and udp streams.')
    parser.add_argument('-c', '--channel', help='channel', default=1)
    args = parser.parse_args()
    stop_time = 0

    db = MySQLdb.connect(host="##.###.###.####", port=3306, user="file_size",
                         passwd="#####", db="file_size")
    cursor = db.cursor()

    cursor.execute("SELECT * FROM channels WHERE id = " + args.channel)

    channel_data = cursor.fetchall()[0]

    date_format_long = '%Y%m%d-%H'
    date_format = '%Y%m%d-%H-%M-%S'

    sys.stderr.write('Multisoft Stream dump\n')
    sys.stderr.write('Using %s:%s\n' % (channel_data[6], channel_data[7]))
    main()
