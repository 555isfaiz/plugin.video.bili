# -*- coding: utf-8 -*-
from http import server as BaseHTTPServer
import re
import json
import socket
import os
import requests
import xbmc
import xbmcvfs

try:
    xbmc.translatePath = xbmcvfs.translatePath
except AttributeError:
    pass

UPSTREAM_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.bilibili.com',
}
# B站的视频服务器偶尔会在传输中途断开连接或卡住, inputstream.adaptive 遇到这种情况会一直缓冲直到手动跳转.
# 所以视频流经由本地代理转发, 上游中断时从断点处重新请求 (依次尝试备用链接), 对播放器来说是一个完整的响应.
MAX_RETRIES = 6
UPSTREAM_TIMEOUT = (5, 10)


class BilibiliRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        self.addon_id = 'plugin.video.bili'
        self.chunk_size = 1024 * 64
        try:
            self.base_path = xbmc.translatePath('special://temp/%s' % self.addon_id).decode('utf-8')
        except AttributeError:
            self.base_path = xbmc.translatePath('special://temp/%s' % self.addon_id)
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, client_address, server)

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        stripped_path = self.path.rstrip('/')
        if self.path.startswith('/stream/'):
            self.proxy_stream()
        elif self.path.endswith('.mpd'):
            file_path = self.get_mpd_path()
            file_chunk = True
            try:
                with open(file_path, 'rb') as f:
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/xml+dash')
                    self.send_header('Content-Length', os.path.getsize(file_path))
                    self.end_headers()
                    while file_chunk:
                        file_chunk = f.read(self.chunk_size)
                        if file_chunk:
                            self.wfile.write(file_chunk)
            except IOError:
                response = 'File Not Found: |{proxy_path}| -> |{file_path}|'.format(proxy_path=self.path, file_path=file_path.encode('utf-8'))
                self.send_error(404, response)
        else:
            self.send_error(404)

    def get_stream_urls(self):
        # /stream/<cid>/<video|audio>/<index>, 链接列表由插件写在 <cid>.json 中
        match = re.match(r'^/stream/(\d+)/(video|audio)/(\d+)$', self.path)
        if not match:
            return []
        cid, kind, index = match.groups()
        try:
            with open(os.path.join(self.base_path, cid + '.json'), 'r') as f:
                streams = json.load(f)
        except (IOError, ValueError):
            return []
        if not isinstance(streams, dict):
            return []
        return streams.get(kind + '/' + index) or []

    def proxy_stream(self, head=False):
        urls = self.get_stream_urls()
        if not urls:
            self.send_error(404)
            return
        start, end = None, ''
        if 'Range' in self.headers:
            match = re.match(r'^bytes=(\d+)-(\d*)$', self.headers['Range'].strip())
            if not match:
                self.send_error(416)
                return
            start, end = int(match.group(1)), match.group(2)
        sent = 0
        expected = None
        headers_sent = False
        last_status = 502
        urls = list(urls)
        for attempt in range(MAX_RETRIES):
            url = urls[attempt % len(urls)]
            offset = (start or 0) + sent
            headers = dict(UPSTREAM_HEADERS)
            if start is not None or sent:
                headers['Range'] = 'bytes={}-{}'.format(offset, end)
            try:
                response = requests.get(url, headers=headers, stream=True, timeout=UPSTREAM_TIMEOUT)
            except requests.RequestException as e:
                xbmc.log('plugin.video.bili: stream request failed at {} bytes, retry {}: {}'.format(sent, attempt + 1, e))
                continue
            try:
                if 'Range' in headers:
                    # 请求了范围就必须返回 206 且起点一致, 否则拼接出来的数据是错的
                    content_range = re.match(r'^bytes (\d+)-', response.headers.get('Content-Range', ''))
                    ok = response.status_code == 206 and content_range and int(content_range.group(1)) == offset
                else:
                    ok = response.status_code == 200
                if not ok:
                    last_status = response.status_code if response.status_code >= 400 else 502
                    xbmc.log('plugin.video.bili: bad stream response {} at {} bytes, retry {}'.format(response.status_code, sent, attempt + 1))
                    if response.status_code == 416:
                        break
                    if len(urls) > 1:
                        # 返回错误或不支持范围请求的链接不再重试
                        urls.remove(url)
                    continue
                if not headers_sent:
                    try:
                        self.send_response(response.status_code)
                        for key in ('Content-Type', 'Content-Length', 'Content-Range', 'Accept-Ranges'):
                            if key in response.headers:
                                self.send_header(key, response.headers[key])
                        self.end_headers()
                    except OSError:
                        return
                    headers_sent = True
                    if 'Content-Length' in response.headers:
                        expected = int(response.headers['Content-Length'])
                    if head:
                        return
                for chunk in response.iter_content(self.chunk_size):
                    try:
                        self.wfile.write(chunk)
                    except OSError:
                        # 播放器关闭了连接 (跳转或停止播放)
                        return
                    sent += len(chunk)
                # 没有 Content-Length 时无法判断是否完整, 只能当作完整
                if expected is None or sent >= expected:
                    return
                xbmc.log('plugin.video.bili: stream closed early at {}/{} bytes, retry {}'.format(sent, expected, attempt + 1))
            except requests.RequestException as e:
                xbmc.log('plugin.video.bili: stream interrupted at {} bytes, retry {}: {}'.format(sent, attempt + 1, e))
            finally:
                response.close()
        if headers_sent:
            xbmc.log('plugin.video.bili: giving up stream at {} bytes'.format(sent))
        else:
            self.send_error(last_status)

    def get_mpd_path(self):
        # 只允许访问临时目录下的 mpd 文件, 防止路径穿越
        return os.path.join(self.base_path, os.path.basename(self.path.split('?')[0]))

    def do_HEAD(self):
        if self.path.startswith('/stream/'):
            self.proxy_stream(head=True)
        elif self.path.endswith('.mpd'):
            file_path = self.get_mpd_path()
            if not os.path.isfile(file_path):
                response = 'File Not Found: |{proxy_path}| -> |{file_path}|'.format(proxy_path=self.path, file_path=file_path.encode('utf-8'))
                self.send_error(404, response)
            else:
                self.send_response(200)
                self.send_header('Content-Type', 'application/xml+dash')
                self.send_header('Content-Length', os.path.getsize(file_path))
                self.end_headers()
        else:
            self.send_error(501)


def get_http_server(address=None, port=None):
    address = address if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', address or '') else '127.0.0.1'
    port = int(port) if port else 54321
    try:
        # 音频和视频流同时请求, 需要多线程
        server = BaseHTTPServer.ThreadingHTTPServer((address, port), BilibiliRequestHandler)
        server.daemon_threads = True
        return server
    except socket.error as e:
        return None
