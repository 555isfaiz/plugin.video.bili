# -*- coding: utf-8 -*-
from http import server as BaseHTTPServer
import re
import json
import socket
import os
import time
import threading
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

# inputstream.adaptive 的缓冲是写死的 (最多约 8 个分段, B站约 40 秒), 而且只用一个连接顺序下载,
# B站服务器变慢时很容易耗尽. 所以代理自己做预加载: 按块用多个连接并行下载当前位置之后的数据, 缓存在内存中.
CACHE_CHUNK = 512 * 1024
CACHE_IDLE_TIMEOUT = 120


def fetch_range(urls, start, end):
    '下载 [start, end] 字节, 失败时依次尝试备用链接. 返回 (数据, 文件总大小)'
    urls = list(urls)
    for attempt in range(MAX_RETRIES):
        url = urls[attempt % len(urls)]
        headers = dict(UPSTREAM_HEADERS, Range='bytes={}-{}'.format(start, end))
        try:
            with requests.get(url, headers=headers, stream=True, timeout=UPSTREAM_TIMEOUT) as response:
                content_range = re.match(r'^bytes (\d+)-(\d+)/(\d+)$', response.headers.get('Content-Range', ''))
                # 必须是 206 且起点一致, 否则拼接出来的数据是错的
                if response.status_code != 206 or not content_range or int(content_range.group(1)) != start:
                    xbmc.log('plugin.video.bili: bad stream response {} for bytes {}-{}, retry {}'.format(response.status_code, start, end, attempt + 1), xbmc.LOGINFO)
                    if response.status_code == 416:
                        unsatisfied = re.match(r'^bytes \*/(\d+)$', response.headers.get('Content-Range', ''))
                        return None, int(unsatisfied.group(1)) if unsatisfied else None
                    if len(urls) > 1:
                        # 返回错误或不支持范围请求的链接不再重试
                        urls.remove(url)
                    continue
                total = int(content_range.group(3))
                expected = min(end, total - 1) - start + 1
                data = b''.join(response.iter_content(64 * 1024))
                if len(data) == expected:
                    return data, total
                xbmc.log('plugin.video.bili: stream closed early at {}/{} bytes, retry {}'.format(len(data), expected, attempt + 1), xbmc.LOGINFO)
        except requests.RequestException as e:
            xbmc.log('plugin.video.bili: stream request failed for bytes {}-{}, retry {}: {}'.format(start, end, attempt + 1, e), xbmc.LOGINFO)
    return None, None


class StreamCache(object):
    '一个视频流的预加载缓存: 后台线程并行下载播放位置之后的数据块'
    CHUNK_TIMEOUT = 120         # 要比 fetch_range 的重试总时长 (最多约 90 秒) 长
    FAILED_RETRY_DELAY = 10

    STATS_INTERVAL = 30

    def __init__(self, name, urls, ahead_bytes, workers):
        self.name = name
        self.urls = urls
        self.ahead_chunks = max(4, ahead_bytes // CACHE_CHUNK)
        self.cond = threading.Condition()
        self.chunks = {}
        self.pending = set()
        self.wanted = {}        # 正在等待的块 -> 等待的请求数, 这些块一定会下载且不会被丢弃
        self.failed = {}        # 下载失败的块 -> 失败时间, 过一段时间再重试
        self.total = None
        self.position = 0
        self.last_used = time.time()
        self.closed = False
        self.reset_stats()
        for _ in range(workers):
            threading.Thread(target=self.worker, daemon=True).start()

    def reset_stats(self):
        self.stats = {'bytes': 0, 'fetch_time': 0.0, 'failures': 0, 'wait_max': 0.0, 'wait_total': 0.0, 'since': time.time()}

    def log_stats(self):
        '定时记录下载速度, 预加载了多少, 播放器等待了多久, 方便排查卡顿'
        stats = self.stats
        elapsed = time.time() - stats['since']
        if elapsed < self.STATS_INTERVAL:
            return
        ahead = sum(1 for i in self.chunks if i >= self.position) * CACHE_CHUNK
        xbmc.log('plugin.video.bili: stream {}: downloaded {:.1f} MB in {:.0f}s ({:.2f} Mbps, {:.2f} Mbps per connection), '
                 'buffered ahead {:.1f} MB, player waited {:.1f}s (max {:.1f}s), failed chunks {}'.format(
                     self.name, stats['bytes'] / 1048576, elapsed, stats['bytes'] * 8 / elapsed / 1e6,
                     stats['bytes'] * 8 / stats['fetch_time'] / 1e6 if stats['fetch_time'] else 0,
                     ahead / 1048576, stats['wait_total'], stats['wait_max'], stats['failures']), xbmc.LOGINFO)
        self.reset_stats()

    def chunk_count(self):
        return (self.total + CACHE_CHUNK - 1) // CACHE_CHUNK if self.total is not None else None

    def can_fetch(self, index):
        if index in self.chunks or index in self.pending:
            return False
        if index in self.failed and time.time() - self.failed[index] < self.FAILED_RETRY_DELAY:
            return False
        return self.total is None or index < self.chunk_count()

    def next_chunk(self):
        # 先下载正在等待的块, 再预取最新播放位置之后的块 (文件大小未知时不预取)
        for index in sorted(self.wanted):
            if self.can_fetch(index):
                return index
        if self.total is None:
            return None
        for index in range(self.position, min(self.position + self.ahead_chunks, self.chunk_count())):
            if self.can_fetch(index):
                return index
        return None

    def in_window(self, index):
        return index in self.wanted or self.position - 1 <= index < self.position + self.ahead_chunks

    def worker(self):
        while True:
            with self.cond:
                index = self.next_chunk()
                while index is None and not self.closed:
                    self.cond.wait(5)
                    if not self.wanted and time.time() - self.last_used > CACHE_IDLE_TIMEOUT:
                        # 停止播放后释放内存
                        self.close_locked()
                    index = self.next_chunk()
                if self.closed:
                    return
                self.pending.add(index)
                urls = self.urls
            data, total = None, None
            fetch_start = time.time()
            try:
                data, total = fetch_range(urls, index * CACHE_CHUNK, (index + 1) * CACHE_CHUNK - 1)
            except Exception as e:
                xbmc.log('plugin.video.bili: stream chunk {} failed: {}'.format(index, e), xbmc.LOGINFO)
            finally:
                with self.cond:
                    self.pending.discard(index)
                    if total is not None:
                        self.total = total
                    if data is None:
                        self.failed[index] = time.time()
                        self.stats['failures'] += 1
                    else:
                        self.stats['bytes'] += len(data)
                        self.stats['fetch_time'] += time.time() - fetch_start
                        self.failed.pop(index, None)
                        if not self.closed and self.in_window(index):
                            self.chunks[index] = data
                    self.cond.notify_all()

    def get_chunk(self, index):
        '取一个数据块, 并把预加载的位置移到这里. 失败时返回 None'
        with self.cond:
            self.last_used = time.time()
            if self.total is not None and index >= self.chunk_count():
                return None
            if index != self.position:
                self.position = index
                # 丢掉播放位置之前和预加载范围之外的块 (跳转之后), 其他请求正在等待的块保留
                for i in list(self.chunks):
                    if not self.in_window(i):
                        del self.chunks[i]
            self.failed.pop(index, None)
            self.wanted[index] = self.wanted.get(index, 0) + 1
            self.cond.notify_all()
            wait_start = time.time()
            try:
                deadline = time.time() + self.CHUNK_TIMEOUT
                while index not in self.chunks:
                    if self.closed or time.time() > deadline:
                        return None
                    if index in self.failed and index not in self.pending:
                        return None
                    if self.total is not None and index >= self.chunk_count():
                        return None
                    self.cond.wait(1)
                return self.chunks[index]
            finally:
                self.wanted[index] -= 1
                if not self.wanted[index]:
                    del self.wanted[index]
                self.last_used = time.time()
                waited = self.last_used - wait_start
                self.stats['wait_total'] += waited
                self.stats['wait_max'] = max(self.stats['wait_max'], waited)
                self.log_stats()

    def close_locked(self):
        self.closed = True
        self.chunks.clear()
        self.cond.notify_all()

    def close(self):
        with self.cond:
            self.close_locked()


_caches = {}
_caches_lock = threading.Lock()


def get_stream_cache(key, urls, ahead_bytes, workers):
    with _caches_lock:
        now = time.time()
        for k in list(_caches):
            # 同一时间只播放一个视频, 其他视频的缓存直接释放
            if k[0] != key[0] or now - _caches[k].last_used > CACHE_IDLE_TIMEOUT:
                if k != key:
                    _caches.pop(k).close()
        cache = _caches.get(key)
        if cache is not None:
            with cache.cond:
                if not cache.closed:
                    # 重新播放同一个视频时链接会更新 (旧链接会过期)
                    cache.urls = urls
                    cache.last_used = now
                    return cache
        cache = _caches[key] = StreamCache('/'.join(key), urls, ahead_bytes, workers)
        xbmc.log('plugin.video.bili: new stream cache {} ({} MB ahead, {} connections)'.format(
            '/'.join(key), ahead_bytes // 1048576, workers), xbmc.LOGINFO)
        return cache


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

    def get_stream_cache(self):
        # /stream/<cid>/<video|audio>/<index>, 链接列表和缓存大小由插件写在 <cid>.json 中
        match = re.match(r'^/stream/(\d+)/(video|audio)/(\d+)$', self.path)
        if not match:
            return None
        cid, kind, index = match.groups()
        try:
            with open(os.path.join(self.base_path, cid + '.json'), 'r') as f:
                info = json.load(f)
            urls = info['streams'][kind + '/' + index]
            buffer_bytes = min(max(int(info.get('buffer_mb', 64)), 16), 512) * 1024 * 1024
        except (IOError, ValueError, KeyError, TypeError):
            return None
        if not urls:
            return None
        if kind == 'video':
            return get_stream_cache((cid, kind, index), urls, buffer_bytes, 3)
        # 音频码率低得多, 用视频缓存的 1/4
        return get_stream_cache((cid, kind, index), urls, buffer_bytes // 4, 2)

    def proxy_stream(self, head=False):
        cache = self.get_stream_cache()
        if not cache:
            self.send_error(404)
            return
        start, end = 0, None
        if 'Range' in self.headers:
            match = re.match(r'^bytes=(\d+)-(\d*)$', self.headers['Range'].strip())
            if not match:
                self.send_error(416)
                return
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else None
            if end is not None and end < start:
                self.send_error(416)
                return
        # 先取起点所在的块, 顺便得到文件大小
        if cache.get_chunk(start // CACHE_CHUNK) is None and cache.total is None:
            self.send_error(502)
            return
        total = cache.total
        if start >= total:
            self.send_error(416)
            return
        end = total - 1 if end is None else min(end, total - 1)
        try:
            if 'Range' in self.headers:
                self.send_response(206)
                self.send_header('Content-Range', 'bytes {}-{}/{}'.format(start, end, total))
            else:
                self.send_response(200)
            self.send_header('Content-Type', 'video/mp4' if '/video/' in self.path else 'audio/mp4')
            self.send_header('Content-Length', str(end - start + 1))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
            if head:
                return
            position = start
            while position <= end:
                index = position // CACHE_CHUNK
                data = cache.get_chunk(index)
                offset = position - index * CACHE_CHUNK
                piece = data[offset:offset + end - position + 1] if data else b''
                if not piece:
                    # 已经发送了 Content-Length, 只能断开连接, 让播放器知道数据不完整
                    xbmc.log('plugin.video.bili: giving up stream at byte {}'.format(position), xbmc.LOGINFO)
                    self.close_connection = True
                    return
                self.wfile.write(piece)
                position += len(piece)
        except OSError:
            # 播放器关闭了连接 (跳转或停止播放)
            return

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
