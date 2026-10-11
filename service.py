# -*- coding: utf-8 -*-
from monitor import BilibiliMonitor, BilibiliPlayer


def run():
    sleep_time = 5
    monitor = BilibiliMonitor()
    player = BilibiliPlayer()

    monitor.remove_temp_dir()

    while not monitor.abortRequested():
        if monitor.waitForAbort(sleep_time):
            break
        player.update()

    if monitor.httpd:
        monitor.shutdown_httpd()


if __name__ == '__main__':
    run()
