"""核对帧解析器、连接档案、崩溃恢复与背压队列。"""
import io
import json
import random
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from source_lab.rtcm import frame
from source_lab.tcp import start as start_source

from archiver.capture import ByteQueue, capture_connection
from archiver.check import check_archive
from archiver.parser import FrameParser
from archiver.store import Archive

SESSION0 = Path('sources/session0.bin').read_bytes()
SESSION1 = Path('sources/session1.bin').read_bytes()


def parse(data, conn=0, **kw):
    p = FrameParser(conn, **kw)
    return p.feed(data) + p.finish()


class ParserTests(unittest.TestCase):
    def test_session0_records(self):
        recs = parse(SESSION0, conn=1)
        kinds = [(r['kind'], r.get('reason'), r['start'], r['end']) for r in recs]
        self.assertEqual(kinds, [
            ('reject', 'noise', 0, 5),
            ('frame', None, 5, 16),
            ('frame', None, 16, 27),
            ('reject', 'bad-crc', 27, 37),
            ('frame', None, 37, 47),
            ('reject', 'truncated', 47, 51),
        ])
        self.assertEqual(recs[1]['type'], 1077)          # 载荷内含 0xD3，不当新帧
        self.assertEqual(recs[2]['type'], 1077)          # 连续重复帧，保留两次
        self.assertEqual(recs[4]['type'], 1005)
        self.assertEqual([r['seq'] for r in recs], list(range(6)))
        pos = 0
        for r in recs:                                    # 半开区间连续覆盖整个段
            self.assertEqual(r['start'], pos)
            pos = r['end']
        self.assertEqual(pos, len(SESSION0))

    def test_session1_short_payloads_unknown_type(self):
        recs = parse(SESSION1, conn=2)
        self.assertEqual([r['kind'] for r in recs], ['frame', 'frame', 'frame'])
        self.assertEqual([r['type'] for r in recs], [None, None, 1005])

    def test_chunking_independent(self):
        for blob in (SESSION0, SESSION1):
            want = parse(blob)
            for n in range(1, len(blob) + 1):
                p = FrameParser(0)
                got = []
                for i in range(0, len(blob), n):
                    got += p.feed(blob[i:i + n])
                got += p.finish()
                self.assertEqual(got, want, f'分块 {n}')

    def test_random_stream_chunking(self):
        rng = random.Random(7)
        parts = []
        for _ in range(40):
            kind = rng.randrange(5)
            if kind == 0:
                parts.append(frame(bytes(rng.randrange(256)
                                         for _ in range(rng.randrange(30)))))
            elif kind == 1:
                parts.append(bytes(rng.randrange(256) for _ in range(rng.randrange(1, 20))))
            elif kind == 2:
                bad = bytearray(frame(b'\xaa\xbb'))
                bad[-1] ^= 1
                parts.append(bytes(bad))
            elif kind == 3:
                parts.append(b'\xd3')
            else:
                parts.append(b'\xd3\xfc\x00')
        blob = b''.join(parts)
        want = parse(blob)
        for _ in range(20):
            p = FrameParser(0)
            got = []
            i = 0
            while i < len(blob):
                n = rng.randint(1, 50)
                got += p.feed(blob[i:i + n])
                i += n
            got += p.finish()
            self.assertEqual(got, want)
        pos = 0
        for r in want:
            self.assertEqual(r['start'], pos)
            pos = r['end']
        self.assertEqual(pos, len(blob))

    def test_preamble_inside_payload(self):
        data = frame(b'\x43\x50\xd3\x00\x7f') + frame(b'\x01\x02')
        frames = [r for r in parse(data) if r['kind'] == 'frame']
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0]['end'] - frames[0]['start'], 11)

    def test_bad_header_reserved_bits(self):
        recs = parse(b'\xd3\xfc\x00' + frame(b'\x01\x02'))
        self.assertEqual(recs[0]['reason'], 'bad-header')
        self.assertEqual(recs[0]['detail'], 'reserved-bits')
        self.assertEqual((recs[0]['start'], recs[0]['end']), (0, 1))
        self.assertEqual(recs[1]['reason'], 'noise')     # fc 00 两字节
        self.assertEqual(recs[2]['kind'], 'frame')

    def test_length_over_limit(self):
        recs = parse(frame(b'12345'), max_payload=4)
        self.assertEqual(recs[0]['reason'], 'bad-header')
        self.assertEqual(recs[0]['detail'], 'length-over-limit')
        recs = parse(frame(b'12345'), max_frame=8)
        self.assertEqual(recs[0]['detail'], 'length-over-limit')

    def test_duplicate_frames_kept(self):
        f = frame(b'\x01\x02')
        frames = [r for r in parse(f + f) if r['kind'] == 'frame']
        self.assertEqual(len(frames), 2)
        self.assertEqual((frames[0]['start'], frames[1]['start']), (0, len(f)))

    def test_truncated_tail(self):
        recs = parse(b'\xd3')
        self.assertEqual(recs[0]['reason'], 'truncated')
        self.assertIsNone(recs[0]['expected'])
        self.assertEqual(recs[0]['got'], 1)
        recs = parse(b'\xd3\x00\x05\x43')
        self.assertEqual(recs[0]['expected'], 11)
        self.assertEqual(recs[0]['got'], 4)

    def test_retained_bound(self):
        rng = random.Random(20261111)
        blob = bytes(rng.getrandbits(8) for _ in range(4096)) + frame(b'\xd3' * 100)
        p = FrameParser(0)
        i = 0
        while i < len(blob):
            n = rng.randint(1, 64)
            p.feed(blob[i:i + n])
            i += n
            self.assertLessEqual(p.retained, p.max_frame)
        p.finish()
        self.assertLessEqual(p.retained_peak, p.max_frame)

    def test_feed_after_finish(self):
        p = FrameParser(0)
        p.finish()
        with self.assertRaises(ValueError):
            p.feed(b'\xd3')
        self.assertEqual(p.finish(), [])


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'arc'

    def capture(self, config='sources/streams.json', connections=2, **kw):
        server = start_source(config)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        archive = Archive(self.root)
        stats = [capture_connection(archive, '127.0.0.1', server.server_address[1],
                                    log=io.StringIO(), **kw)
                 for _ in range(connections)]
        return archive, stats

    def test_capture_two_connections(self):
        archive, stats = self.capture()
        self.assertEqual([s['conn'] for s in stats], [1, 2])
        code, report = check_archive(self.root)
        self.assertEqual(code, 0, report)
        c1, c2 = report['connections']
        self.assertEqual(c1['frames'], 3)
        self.assertEqual(c1['rejects'], {'noise': 1, 'bad-crc': 1, 'truncated': 1})
        self.assertTrue(any('来源断流' in n for n in c1['notes']))
        self.assertEqual(c2['frames'], 3)
        self.assertEqual(c2['rejects'], {})
        self.assertTrue(any('多个连接' in n for n in report['notes']))
        self.assertTrue(any('同一连接内重复' in n for n in report['notes']))

    def test_empty_connection(self):
        archive, _ = self.capture(connections=3)  # 第三个连接来源立即关闭
        code, report = check_archive(self.root)
        self.assertEqual(code, 0, report)
        c3 = report['connections'][2]
        self.assertEqual((c3['bytes'], c3['frames']), (0, 0))
        self.assertEqual(c3['status'], '完整')

    def test_rebuild_after_index_loss(self):
        archive, _ = self.capture(connections=1)
        archive.index_path(1).unlink()
        code, _ = check_archive(self.root)
        self.assertEqual(code, 1)
        archive.rebuild_all()
        code, report = check_archive(self.root)
        self.assertEqual(code, 0, report)

    def test_crash_window_recovery(self):
        # 模拟崩溃窗口：字节已落盘，索引只更新到第一帧，日志停留 open
        archive = Archive(self.root)
        conn = archive.allocate_conn()
        archive.write_journal(conn, state='open', bytes=0)
        segment = archive.open_segment(conn)
        index = archive.open_index(conn)
        data = frame(b'\x01\x02') + frame(b'\x03\x04')
        segment.append(data)
        segment.fsync()
        p = FrameParser(conn)
        index.commit(p.feed(data[:9]), p.parsed_upto)
        index.close()
        segment.close()
        code, _ = check_archive(self.root)
        self.assertEqual(code, 1)                        # 索引过期
        archive.rebuild_all()
        code, report = check_archive(self.root)
        self.assertEqual(code, 2)                        # 一致但本地未完成
        c = report['connections'][0]
        self.assertEqual(c['frames'], 2)
        self.assertEqual(c['status'], '本地未完成')

    def test_interrupted_marked_on_restart(self):
        archive = Archive(self.root)
        conn = archive.allocate_conn()
        archive.write_journal(conn, state='open', bytes=0)
        segment = archive.open_segment(conn)
        segment.append(frame(b'\x01\x02'))
        segment.fsync()
        segment.close()
        actions = archive.recover()
        self.assertEqual(archive.read_journal(conn)['state'], 'interrupted')
        self.assertTrue(any(a.get('action') == 'marked-interrupted' for a in actions))
        code, report = check_archive(self.root)
        self.assertEqual(code, 2)

    def test_truncated_tail_attribution(self):
        # 日志未关闭 + 结尾半帧 → 不能归因于来源断流
        archive = Archive(self.root)
        conn = archive.allocate_conn()
        archive.write_journal(conn, state='open', bytes=0)
        segment = archive.open_segment(conn)
        index = archive.open_index(conn)
        data = b'\xd3\x00\x05\x43'
        segment.append(data)
        segment.fsync()
        p = FrameParser(conn)
        index.commit(p.feed(data), p.parsed_upto)
        index.close()
        segment.close()
        code, report = check_archive(self.root)
        self.assertEqual(code, 2)
        notes = report['connections'][0]['notes']
        self.assertTrue(any('本地未完成' in n or '不能归因于来源' in n for n in notes))
        self.assertFalse(any('来源断流' in n for n in notes))

    def test_write_sizes_do_not_change_records(self):
        records = []
        for sizes in ([1, 2, 7, 3, 64], [5, 9, 1, 31, 2], [1024]):
            with tempfile.TemporaryDirectory() as d:
                cfg = Path(d) / 's.json'
                cfg.write_text(json.dumps({
                    'sessions': [str(Path('sources/session0.bin').resolve())],
                    'write_sizes': sizes}), encoding='utf8')
                server = start_source(str(cfg))
                try:
                    archive = Archive(Path(d) / 'arc')
                    capture_connection(archive, '127.0.0.1', server.server_address[1],
                                       log=io.StringIO())
                finally:
                    server.shutdown()
                    server.server_close()
                recs, _, error = archive.load_index(1)
                self.assertIsNone(error)
                records.append([{k: v for k, v in r.items() if k not in ('conn', 'seq')}
                                for r in recs])
        self.assertEqual(records[0], records[1])
        self.assertEqual(records[0], records[2])

    def test_cli_entries(self):
        self.capture(connections=1)
        for module in ('archiver.check', 'archiver.rebuild'):
            r = subprocess.run([sys.executable, '-m', module, '--archive', str(self.root)],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


class ByteQueueTests(unittest.TestCase):
    def test_backpressure_blocks_when_full(self):
        q = ByteQueue(10)
        q.put(b'1234567890')
        done = threading.Event()

        def producer():
            q.put(b'x')
            done.set()

        t = threading.Thread(target=producer)
        t.start()
        self.assertFalse(done.wait(0.3))     # 满时 put 阻塞 → 背压
        self.assertEqual(q.get(), b'1234567890')
        self.assertTrue(done.wait(2))
        t.join()
        self.assertGreaterEqual(q.block_events, 1)
        self.assertEqual(q.peak, 10)

    def test_close_wakes_waiters(self):
        q = ByteQueue(4)
        q.close()
        self.assertIsNone(q.get())


if __name__ == '__main__':
    unittest.main()
