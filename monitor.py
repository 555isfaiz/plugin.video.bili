# -*- coding: utf-8 -*-
import os
import json
import time
import shutil
import threading
import requests
import xbmc
import xbmcgui
import xbmcvfs
import xbmcaddon

from http_server import get_http_server

try:
    xbmc.translatePath = xbmcvfs.translatePath
except AttributeError:
    pass


class BilibiliMonitor(xbmc.Monitor):
    def __init__(self, *args, **kwargs):
        self.addon_id = 'plugin.video.bili'
        self._httpd_port = int(xbmcaddon.Addon(self.addon_id).getSetting('server_port'))
        # mpd 和视频流代理只给本机的播放器用, 不对局域网开放
        self._httpd_address = '127.0.0.1'
        self.httpd = None
        self.httpd_thread = None

        self.start_httpd()

    def start_httpd(self):
        if not self.httpd:
            self.httpd = get_http_server(address=self._httpd_address, port=self._httpd_port)
            if self.httpd:
                self.httpd_thread = threading.Thread(target=self.httpd.serve_forever)
                self.httpd_thread.daemon = True
                self.httpd_thread.start()

    def shutdown_httpd(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.socket.close()
            self.httpd_thread.join()
            self.httpd_thread = None
            self.httpd = None

    def restart_httpd(self):
        self.shutdown_httpd()
        self.start_httpd()

    def remove_temp_dir(self):
        try:
            path = xbmc.translatePath('special://temp/%s' % self.addon_id).decode('utf-8')
        except AttributeError:
            path = xbmc.translatePath('special://temp/%s' % self.addon_id)

        if os.path.isdir(path):
            try:
                xbmcvfs.rmdir(path, force=True)
            except:
                pass
        if os.path.isdir(path):
            try:
                shutil.rmtree(path)
            except:
                pass

        if os.path.isdir(path):
            return False
        else:
            return True

class BilibiliPlayer(xbmc.Player):
    '跟踪插件发起的播放: 跳转到上次的播放位置, 并定时和停止时上报播放进度'
    REPORT_INTERVAL = 30
    HANDOFF_TIMEOUT = 60
    PROPERTY = 'plugin.video.bili.playing'

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.window = xbmcgui.Window(10000)
        self.lock = threading.Lock()
        self.session = None
        self.played_time = 0
        self.last_report = 0
        self.storage_path = xbmc.translatePath('special://profile/addon_data/plugin.video.bili/.storage/account')

    def onAVStarted(self):
        session = self.take_handoff()
        xbmc.log(f'plugin.video.bili: playback started, session={session}')
        with self.lock:
            self.session = session
            self.played_time = 0
            self.last_report = time.time()
        if session and session['start'] > 0:
            try:
                current = self.getTime()
                if current < session['start'] - 5:
                    xbmc.log(f"plugin.video.bili: at {current:.0f}s, seeking to {session['start']}s")
                    self.seekTime(session['start'])
            except RuntimeError:
                pass

    def take_handoff(self):
        info = self.window.getProperty(self.PROPERTY)
        self.window.clearProperty(self.PROPERTY)
        if not info:
            return None
        try:
            session = json.loads(info)
        except ValueError:
            return None
        if time.time() - session.get('time', 0) > self.HANDOFF_TIMEOUT:
            xbmc.log('plugin.video.bili: ignoring stale playback handoff')
            return None
        try:
            # 确认正在播放的就是插件刚解析的那个视频 (mpd 文件名和视频链接里都带 cid)
            playing_file = self.getPlayingFile()
            if not self.isPlayingVideo() or session['cid'] not in playing_file:
                xbmc.log(f"plugin.video.bili: ignoring handoff for cid {session['cid']}, playing {playing_file[:100]}")
                return None
        except RuntimeError:
            return None
        return session

    def onPlayBackStopped(self):
        self.finish()

    def onPlayBackEnded(self):
        self.finish(ended=True)

    def onPlayBackError(self):
        self.finish()

    def update(self):
        '由服务的主循环定时调用, 停止后无法再获取播放位置, 所以要提前记录'
        with self.lock:
            if not self.session or not self.isPlayingVideo():
                return
            try:
                played_time = int(self.getTime())
                total_time = int(self.getTotalTime())
            except RuntimeError:
                return
            if played_time > 0:
                self.played_time = played_time
            if total_time:
                self.session['total_time'] = total_time
            if time.time() - self.last_report < self.REPORT_INTERVAL:
                return
            self.last_report = time.time()
            session, played_time = self.session, self.played_time
        self.report(session, played_time)

    def finish(self, ended=False):
        with self.lock:
            session, played_time = self.session, self.played_time
            self.session = None
        if not session:
            return
        if ended and session.get('total_time'):
            played_time = session['total_time']
        self.report(session, played_time)

    def get_cookie(self):
        '读取插件保存的登录 cookie (xbmcswift2 的 account 存储)'
        try:
            from xbmcswift2.storage import TimedStorage
            return TimedStorage(self.storage_path).get('cookie', '')
        except Exception:
            return ''

    def report(self, session, played_time):
        # 没有拿到有效的播放位置时不上报, 以免把云端的进度覆盖成 0
        if not session.get('report') or played_time <= 0:
            return
        threading.Thread(target=self.post_heartbeat, args=(session, played_time), daemon=True).start()

    def post_heartbeat(self, session, played_time):
        cookie = self.get_cookie()
        csrf = ''
        if 'bili_jct=' in cookie:
            csrf = cookie.split('bili_jct=')[1].split(';')[0]
        if not csrf:
            xbmc.log('plugin.video.bili: not logged in, history not reported')
            return
        data = {
            'bvid': session['bvid'],
            'cid': session['cid'],
            'played_time': played_time,
            'csrf': csrf,
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://www.bilibili.com',
            'Cookie': cookie,
        }
        try:
            res = requests.post('https://api.bilibili.com/x/click-interface/web/heartbeat', data=data, headers=headers, timeout=10).json()
            xbmc.log(f"plugin.video.bili: report history {session['bvid']} at {played_time}s -> {res.get('code')} {res.get('message')}")
        except Exception as e:
            xbmc.log(f'plugin.video.bili: report history failed: {e}')
