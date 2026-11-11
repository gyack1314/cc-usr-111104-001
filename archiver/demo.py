"""端到端演示：合成来源 → 采集 → 检查 → 分块无关性 → 崩溃恢复 → 背压。

只使用本仓库的合成字节与环回地址，不连接真实基站或机器人。
演示目录默认留在系统临时目录中，便于课后查看；可用 --work 指定。
"""
import argparse
import json
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from source_lab.rtcm import frame
from source_lab.tcp import start as start_source

from .store import Archive

REPO = Path(__file__).resolve().parent.parent
PY = sys.executable


def run_cli(args):
    return subprocess.run([PY, '-m'] + args, cwd=REPO, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def banner(text):
    print(f'\n=== {text} ===')


def show(proc):
    print((proc.stdout or '').rstrip())
    return proc.returncode


def trickle_source(listen_sock, chunks):
    """慢速合成来源：发完给定字节后保持连接不断开（等待采集端被杀）。"""
    conn, _ = listen_sock.accept()
    with conn:
        for chunk in chunks:
            conn.sendall(chunk)
            time.sleep(0.05)
        time.sleep(30)


def main(argv=None):
    p = argparse.ArgumentParser(description='采集端端到端演示（全部使用合成字节）')
    p.add_argument('--work', default=None, help='演示目录（默认系统临时目录，演示后保留）')
    a = p.parse_args(argv)
    work = Path(a.work) if a.work else Path(tempfile.mkdtemp(prefix='archiver-demo-'))
    work.mkdir(parents=True, exist_ok=True)
    archive_dir = work / 'archive'
    archive = Archive(archive_dir)

    banner('步骤 1/5：采集两个连接（碎片化写入、噪声、坏校验、载荷内前导、重复帧、中途断开）')
    server = start_source(str(REPO / 'sources' / 'streams.json'))
    try:
        code = show(run_cli(['archiver.capture', '127.0.0.1', str(server.server_address[1]),
                             '--archive', str(archive_dir), '--connections', '2']))
        assert code == 0, f'采集失败，退出码 {code}'
    finally:
        server.shutdown()
        server.server_close()

    banner('步骤 2/5：检查档案（核对原始段、帧位置、CRC，区分来源断流与本地未完成）')
    code = show(run_cli(['archiver.check', '--archive', str(archive_dir)]))
    assert code == 0, f'预期退出码 0，实际 {code}'

    banner('步骤 3/5：分块无关性（同一字节流换不同写入分块，记录必须一致）')
    alt = work / 'streams-alt.json'
    alt.write_text(json.dumps({
        'sessions': [str(REPO / 'sources' / 'session0.bin')],
        'write_sizes': [5, 9, 1, 31, 2]}), encoding='utf8')
    server = start_source(str(alt))
    try:
        code = show(run_cli(['archiver.capture', '127.0.0.1', str(server.server_address[1]),
                             '--archive', str(archive_dir), '--connections', '1']))
        assert code == 0, f'采集失败，退出码 {code}'
    finally:
        server.shutdown()
        server.server_close()

    def normalized(conn):
        records, _, error = archive.load_index(conn)
        assert error is None, error
        return [{k: v for k, v in r.items() if k not in ('conn', 'seq')} for r in records]
    assert normalized(1) == normalized(3), '不同写入分块得到了不同记录'
    print(f'分块无关性验证通过：write_sizes [1,2,7,3,64] 与 [5,9,1,31,2] 产生相同的 '
          f'{len(normalized(1))} 条记录')

    banner('步骤 4/5：崩溃恢复（kill -9 → 本地未完成 → 索引过期 → 离线重建）')
    chunks = [b'\xd3\xfc\x00',            # 非法头（保留位非零）
              frame(b'\x12\x34\xab\xcd'),  # 一个完整帧
              frame(b'\x12\x34')[:2]]      # 半个帧头，随后保持连接
    listen = socket.socket()
    listen.bind(('127.0.0.1', 0))
    listen.listen(1)
    threading.Thread(target=trickle_source, args=(listen, chunks), daemon=True).start()
    proc = subprocess.Popen([PY, '-m', 'archiver.capture', '127.0.0.1',
                             str(listen.getsockname()[1]),
                             '--archive', str(archive_dir), '--connections', '1'],
                            cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    before = set(archive.known_conns())
    conn_id = None
    deadline = time.time() + 15
    while time.time() < deadline:
        new = [c for c in archive.known_conns() if c not in before]
        if new and archive.segment_path(new[0]).exists() \
                and archive.segment_path(new[0]).stat().st_size >= 15:
            conn_id = new[0]
            break
        time.sleep(0.05)
    assert conn_id is not None, '采集进程没有写出预期的原始段'
    time.sleep(0.5)  # 让写线程把索引 commit 落盘
    proc.kill()      # 模拟 kill -9：日志停留在 open
    proc.wait()
    print(f'采集进程已被 kill -9，conn-{conn_id:06d} 的日志停留在 open')
    code = show(run_cli(['archiver.check', '--archive', str(archive_dir)]))
    assert code == 2, f'预期退出码 2（本地未完成），实际 {code}'
    with open(archive.segment_path(conn_id), 'ab') as f:
        f.write(frame(b'\x12\x34')[2:])  # 补全半帧剩余的字节
    print('模拟崩溃窗口：原始段多出 6 字节（半帧被补全），索引没来得及更新')
    code = show(run_cli(['archiver.check', '--archive', str(archive_dir)]))
    assert code == 1, f'预期退出码 1（索引过期），实际 {code}'
    show(run_cli(['archiver.rebuild', '--archive', str(archive_dir)]))
    code = show(run_cli(['archiver.check', '--archive', str(archive_dir)]))
    assert code == 2, f'预期退出码 2（重建后仍属本地未完成），实际 {code}'
    print('崩溃窗口中的字节没有丢，但日志未关闭的连接不会被报告成完整档案')
    listen.close()

    banner('步骤 5/5：背压（小缓存 + 快来源，读取被自然限速，不丢帧）')
    burst = [frame(bytes([i & 0xFF]) * 200) for i in range(200)]  # 200 帧，约 40KB
    listen = socket.socket()
    listen.bind(('127.0.0.1', 0))
    listen.listen(1)

    def burst_source():
        conn, _ = listen.accept()
        with conn:
            for chunk in burst:
                conn.sendall(chunk)
            time.sleep(1)  # 等接收端收完再关闭

    threading.Thread(target=burst_source, daemon=True).start()
    code = show(run_cli(['archiver.capture', '127.0.0.1', str(listen.getsockname()[1]),
                         '--archive', str(archive_dir), '--connections', '1',
                         '--queue-bytes', '4096']))
    assert code == 0, f'采集失败，退出码 {code}'
    listen.close()
    last = max(archive.known_conns())
    records, _, error = archive.load_index(last)
    assert error is None, error
    got = sum(1 for r in records if r['kind'] == 'frame')
    assert got == len(burst), f'期望 {len(burst)} 帧，实际 {got} 帧'
    print(f'背压下 {got} 帧全部落盘，无丢失')

    banner('演示完成')
    print(f'档案保留在 {archive_dir} ，可随时运行：')
    print(f'  python -m archiver.check --archive {archive_dir}')
    print(f'  python -m archiver.rebuild --archive {archive_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
