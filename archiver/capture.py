"""TCP 采集入口：每个连接单独编号，原始字节落盘后再更新索引。

读线程与写线程之间是有界字节队列：写盘跟不上时 put 阻塞，读线程随之
停止 recv，TCP 接收窗口关闭后对端被自然限速——背压作用于读取侧，
不会默默丢帧。队列峰值、阻塞次数与解析器留存峰值都会打印出来。
"""
import argparse
import socket
import sys
import threading
import time
from collections import deque

from .parser import FrameParser, MAX_PAYLOAD
from .store import Archive


class ByteQueue:
    """读线程与写线程之间的有界字节缓冲；满时 put 阻塞形成背压。"""

    def __init__(self, limit):
        if limit <= 0:
            raise ValueError('队列上限必须为正')
        self.limit = limit
        self._items = deque()
        self._size = 0
        self._eof = False
        self._cv = threading.Condition()
        self.block_events = 0  # put 因满而阻塞的次数（可观察的背压）
        self.peak = 0          # 缓存字节数峰值（可观察的上界）

    def put(self, data):
        if len(data) > self.limit:
            raise ValueError('单块字节超过队列上限')
        with self._cv:
            while self._size + len(data) > self.limit and not self._eof:
                self.block_events += 1
                self._cv.wait()
            self._items.append(data)
            self._size += len(data)
            self.peak = max(self.peak, self._size)
            self._cv.notify_all()

    def close(self):
        with self._cv:
            self._eof = True
            self._cv.notify_all()

    def get(self):
        """返回下一块字节；来源结束且队列排空后返回 None。"""
        with self._cv:
            while not self._items and not self._eof:
                self._cv.wait()
            if not self._items:
                return None
            data = self._items.popleft()
            self._size -= len(data)
            self._cv.notify_all()
            return data


def _reader(sock, queue, recv_size, error_box):
    try:
        while True:
            data = sock.recv(recv_size)
            if not data:
                break
            queue.put(data)
    except OSError as e:
        error_box.append(str(e))
    finally:
        queue.close()


def capture_connection(archive, host, port, *, max_payload=MAX_PAYLOAD,
                       max_frame=None, queue_bytes=1 << 20, recv_size=65536,
                       timeout=None, log=sys.stderr):
    """采集一个连接并返回统计信息；每个连接使用新的编号。"""
    sock = socket.create_connection((host, port), timeout=timeout)
    conn = archive.allocate_conn()
    archive.write_journal(conn, state='open', bytes=0, peer=f'{host}:{port}',
                          opened_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    segment = archive.open_segment(conn)
    index = archive.open_index(conn)
    recv_size = max(1, min(recv_size, queue_bytes))
    queue = ByteQueue(queue_bytes)
    errors = []
    thread = threading.Thread(target=_reader, args=(sock, queue, recv_size, errors),
                              daemon=True)
    thread.start()
    parser = FrameParser(conn, max_payload, max_frame)
    frames = rejects = 0
    close_reason = 'peer-eof'
    interrupted = False
    try:
        while True:
            data = queue.get()
            if data is None:
                break
            segment.append(data)
            records = parser.feed(data)
            segment.fsync()                        # 先保证原始段落盘
            index.commit(records, parser.parsed_upto)  # 再更新索引（含 fsync）
            frames += sum(1 for r in records if r['kind'] == 'frame')
            rejects += sum(1 for r in records if r['kind'] == 'reject')
    except KeyboardInterrupt:
        close_reason = 'local-stop'
        interrupted = True
    # 无论对端关闭、接收出错还是本地停止，都把已收字节结算清楚再写关闭日志
    tail = parser.finish()
    segment.fsync()
    index.commit(tail, parser.parsed_upto)
    frames += sum(1 for r in tail if r['kind'] == 'frame')
    rejects += sum(1 for r in tail if r['kind'] == 'reject')
    if errors and not interrupted:
        close_reason = f'recv-error: {errors[0]}'
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()
    thread.join(timeout=5)
    segment.close()
    index.close()
    archive.write_journal(conn, state='closed', bytes=segment.size,
                          close_reason=close_reason,
                          closed_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    stats = {'conn': conn, 'bytes': segment.size, 'frames': frames, 'rejects': rejects,
             'queue_peak': queue.peak, 'queue_limit': queue.limit,
             'backpressure_events': queue.block_events,
             'parser_retained_peak': parser.retained_peak,
             'close_reason': close_reason}
    print(f"conn-{conn:06d}: {stats['bytes']} 字节, {frames} 帧, {rejects} 拒收, "
          f"队列峰值 {queue.peak}/{queue.limit} 字节, 背压 {queue.block_events} 次, "
          f"解析留存峰值 {parser.retained_peak} 字节, 关闭原因 {close_reason}", file=log)
    if interrupted:
        raise KeyboardInterrupt
    return stats


def main(argv=None):
    p = argparse.ArgumentParser(
        description='RTCM3 原始字节采集端：逐连接留存原始字节与帧索引，不做定位解算')
    p.add_argument('host')
    p.add_argument('port', type=int)
    p.add_argument('--archive', required=True, help='档案目录')
    p.add_argument('--connections', type=int, default=1, help='采集连接数（默认 1）')
    p.add_argument('--forever', action='store_true', help='断线后持续重连，直到 Ctrl-C')
    p.add_argument('--retry-delay', type=float, default=1.0, help='连接失败重试间隔（秒）')
    p.add_argument('--queue-bytes', type=int, default=1 << 20,
                   help='读写线程间缓存上限（字节），写盘跟不上时对读取施加背压')
    p.add_argument('--max-payload', type=int, default=MAX_PAYLOAD, help='单帧载荷上限')
    p.add_argument('--max-frame', type=int, default=None,
                   help='单帧总长度上限（默认按载荷上限推出）')
    p.add_argument('--recv-size', type=int, default=65536, help='单次 recv 上限')
    a = p.parse_args(argv)
    archive = Archive(a.archive)
    for action in archive.recover(a.max_payload, a.max_frame):
        print(f'启动恢复: {action}', file=sys.stderr)
    count = 0
    try:
        while a.forever or count < a.connections:
            try:
                capture_connection(archive, a.host, a.port,
                                   max_payload=a.max_payload, max_frame=a.max_frame,
                                   queue_bytes=a.queue_bytes, recv_size=a.recv_size)
            except (ConnectionError, OSError) as e:
                if not a.forever:
                    raise
                print(f'连接失败（{e}），{a.retry_delay}s 后重试', file=sys.stderr)
                time.sleep(a.retry_delay)
                continue
            count += 1
    except KeyboardInterrupt:
        print('本地停止采集', file=sys.stderr)


if __name__ == '__main__':
    main()
