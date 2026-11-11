"""连接档案：原始段是唯一事实来源，索引与日志都可由原始段重建或核对。

目录布局：
  archive.json               清单（连接编号计数器）
  segments/conn-NNNNNN.raw   该连接收到的原始字节（O_EXCL 创建，只追加）
  meta/conn-NNNNNN.json      连接日志（open/closed/interrupted）
  index/conn-NNNNNN.jsonl    帧/拒收记录 + commit 行（派生数据，可整体重建）

写入顺序保证：先 fsync 原始段，再写索引，连接关闭日志最后写。
因此索引永不多于原始段；崩溃后总能从原始段重建索引，而未完整写下的
末尾（日志停留 open）不会被报告成完整档案。
"""
import json
import os
import re
import tempfile
from pathlib import Path

from .parser import FrameParser, MAX_PAYLOAD

_CONN_RE = re.compile(r'conn-(\d{6})')


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json(path, obj):
    fd, tmp = tempfile.mkstemp(prefix='.tmp-', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf8') as f:
            json.dump(obj, f, ensure_ascii=False)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    _fsync_dir(path.parent)


class SegmentWriter:
    """单连接原始段写入器：拒绝覆盖已有段，只追加，显式 fsync。"""

    def __init__(self, path):
        self.path = path
        self.fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        self.size = 0

    def append(self, data):
        view = memoryview(data)
        while view:
            n = os.write(self.fd, view)
            view = view[n:]
            self.size += n

    def fsync(self):
        os.fsync(self.fd)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class IndexWriter:
    """索引追加器：每批记录后跟一行 commit；调用方保证原始段已先 fsync。"""

    def __init__(self, path, conn):
        self.path = path
        self.conn = conn
        self.records = 0
        self.f = open(path, 'w', encoding='utf8')

    def commit(self, records, parsed_upto):
        for rec in records:
            self.f.write(json.dumps(rec, ensure_ascii=False) + '\n')
            self.records += 1
        line = {'kind': 'commit', 'conn': self.conn,
                'parsed_upto': parsed_upto, 'records': self.records}
        self.f.write(json.dumps(line, ensure_ascii=False) + '\n')
        self.f.flush()
        os.fsync(self.f.fileno())

    def close(self):
        if not self.f.closed:
            self.f.close()


class Archive:
    def __init__(self, root):
        self.root = Path(root)
        self.segments = self.root / 'segments'
        self.meta = self.root / 'meta'
        self.index = self.root / 'index'
        for d in (self.segments, self.meta, self.index):
            d.mkdir(parents=True, exist_ok=True)

    # ---- 路径与编号 ----
    def segment_path(self, conn):
        return self.segments / f'conn-{conn:06d}.raw'

    def journal_path(self, conn):
        return self.meta / f'conn-{conn:06d}.json'

    def index_path(self, conn):
        return self.index / f'conn-{conn:06d}.jsonl'

    def known_conns(self):
        ids = set()
        for d in (self.segments, self.meta, self.index):
            for p in d.iterdir():
                m = _CONN_RE.fullmatch(p.stem)
                if m:
                    ids.add(int(m.group(1)))
        return sorted(ids)

    def allocate_conn(self):
        """分配新的连接编号；编号只增不复用，允许有缺口。"""
        path = self.root / 'archive.json'
        manifest = {}
        if path.exists():
            try:
                manifest = json.loads(path.read_text(encoding='utf8'))
            except ValueError:
                manifest = {}
        known = self.known_conns()
        nxt = max([manifest.get('next_conn', 1)] + [c + 1 for c in known])
        _atomic_json(path, {'version': 1, 'next_conn': nxt + 1})
        return nxt

    # ---- 连接日志 ----
    def read_journal(self, conn):
        path = self.journal_path(conn)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding='utf8'))

    def write_journal(self, conn, **fields):
        data = self.read_journal(conn) or {}
        data.update(fields)
        data['conn'] = conn
        _atomic_json(self.journal_path(conn), data)

    # ---- 采集写入 ----
    def open_segment(self, conn):
        return SegmentWriter(self.segment_path(conn))

    def open_index(self, conn):
        return IndexWriter(self.index_path(conn), conn)

    # ---- 读取与解析 ----
    def parse_segment(self, conn, max_payload=MAX_PAYLOAD, max_frame=None):
        """重新解析原始段，返回 (流式记录, 结尾结算记录, 段字节数)。"""
        parser = FrameParser(conn, max_payload, max_frame)
        streaming = []
        with open(self.segment_path(conn), 'rb') as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                streaming.extend(parser.feed(chunk))
        return streaming, parser.finish(), parser.parsed_upto

    def load_index(self, conn):
        """返回 (records, commits, error)；error 非空表示索引不可用。"""
        path = self.index_path(conn)
        if not path.exists():
            return None, None, '索引文件不存在'
        records, commits = [], []
        for i, line in enumerate(path.read_text(encoding='utf8').splitlines(), 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                return None, None, f'索引第 {i} 行不是合法 JSON（可能写了一半）'
            if not isinstance(obj, dict) or 'kind' not in obj:
                return None, None, f'索引第 {i} 行缺少 kind 字段'
            (commits if obj['kind'] == 'commit' else records).append(obj)
        return records, commits, None

    # ---- 重建与恢复 ----
    def rebuild_index(self, conn, max_payload=MAX_PAYLOAD, max_frame=None):
        """从原始段整体重建一个连接的索引（原子替换）。"""
        streaming, tail, size = self.parse_segment(conn, max_payload, max_frame)
        records = streaming + tail
        path = self.index_path(conn)
        fd, tmp = tempfile.mkstemp(prefix='.tmp-', dir=str(path.parent))
        try:
            with os.fdopen(fd, 'w', encoding='utf8') as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + '\n')
                commit = {'kind': 'commit', 'conn': conn,
                          'parsed_upto': size, 'records': len(records)}
                f.write(json.dumps(commit, ensure_ascii=False) + '\n')
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        _fsync_dir(path.parent)
        return {'conn': conn, 'records': len(records), 'bytes': size}

    def index_status(self, conn, max_payload=MAX_PAYLOAD, max_frame=None):
        """对比索引与原始段：current / stale / corrupt / missing / no-segment。"""
        if not self.segment_path(conn).exists():
            return 'no-segment'
        records, commits, error = self.load_index(conn)
        if error:
            return 'missing' if '不存在' in error else 'corrupt'
        if not commits:
            return 'corrupt'
        streaming, tail, size = self.parse_segment(conn, max_payload, max_frame)
        last = commits[-1]
        if last.get('parsed_upto') != size or last.get('records') != len(records):
            return 'stale'
        if records != streaming + tail:
            return 'stale'
        return 'current'

    def rebuild_all(self, max_payload=MAX_PAYLOAD, max_frame=None):
        report = []
        for conn in self.known_conns():
            if not self.segment_path(conn).exists():
                report.append({'conn': conn, 'action': 'skipped', 'reason': '缺少原始段'})
                continue
            status = self.index_status(conn, max_payload, max_frame)
            if status == 'current':
                report.append({'conn': conn, 'action': 'kept'})
                continue
            info = self.rebuild_index(conn, max_payload, max_frame)
            info['action'] = 'rebuilt'
            info['was'] = status
            report.append(info)
        return report

    def recover(self, max_payload=MAX_PAYLOAD, max_frame=None):
        """采集进程启动时调用：遗留 open 连接标记为 interrupted，并重建过期索引。"""
        actions = []
        for conn in self.known_conns():
            journal = self.read_journal(conn)
            if journal and journal.get('state') == 'open':
                self.write_journal(conn, state='interrupted',
                                   note='新采集进程接管时该连接仍未关闭，按本地未完成处理')
                actions.append({'conn': conn, 'action': 'marked-interrupted'})
        for item in self.rebuild_all(max_payload, max_frame):
            if item['action'] == 'rebuilt':
                actions.append(item)
        return actions
