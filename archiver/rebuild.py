"""离线重建入口：从已持久化的原始段重新生成全部索引。

原始段只读，索引整体重写；用于崩溃后索引过期、损坏或被人为改动的情况。
"""
import argparse

from .parser import MAX_PAYLOAD
from .store import Archive


def main(argv=None):
    p = argparse.ArgumentParser(description='从原始段重建帧索引（原始段不动，只重写派生索引）')
    p.add_argument('--archive', required=True, help='档案目录')
    p.add_argument('--max-payload', type=int, default=MAX_PAYLOAD)
    p.add_argument('--max-frame', type=int, default=None)
    a = p.parse_args(argv)
    archive = Archive(a.archive)
    report = archive.rebuild_all(a.max_payload, a.max_frame)
    if not report:
        print('档案为空，无连接可重建')
        return 0
    for item in report:
        if item['action'] == 'rebuilt':
            print(f"conn-{item['conn']:06d}: 已重建（原状态 {item['was']}），"
                  f"{item['records']} 条记录，{item['bytes']} 字节")
        elif item['action'] == 'kept':
            print(f"conn-{item['conn']:06d}: 索引已是最新，保留")
        else:
            print(f"conn-{item['conn']:06d}: 跳过（{item['reason']}）")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
