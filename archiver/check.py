"""档案检查工具：重新解析原始段，核对索引、帧位置、CRC 与连接日志。

检查器只依据已持久化的原始段与日志下结论：
- 索引是派生数据，必须与原始段的重新解析结果一致，否则提示重建；
- 结尾半帧 + 已正常关闭的日志 → 来源断流；
- 结尾半帧或任何未决状态 + 未关闭的日志 → 本地未完成写入；
- 无来源序号，同一帧字节出现在多个连接时只提示，不去重、不宣称衔接。

退出码：0=全部完整一致；1=存在一致性问题（需重建或有人为改动）；
2=一致但存在未完成连接。
"""
import argparse
import hashlib
import json
import sys
from collections import Counter

from .parser import MAX_PAYLOAD
from .store import Archive

REASON_CN = {'noise': '噪声', 'bad-header': '非法头', 'bad-crc': '坏校验', 'truncated': '截断'}


def check_archive(root, max_payload=MAX_PAYLOAD, max_frame=None):
    """返回 (退出码, 报告字典)。"""
    archive = Archive(root)
    connections = []
    problems = incompletes = 0
    frame_locations = {}  # 帧字节摘要 -> [conn, ...]，用于重复帧提示
    for conn in archive.known_conns():
        entry = {'conn': conn, 'notes': [], 'problems': []}
        connections.append(entry)
        journal = archive.read_journal(conn)
        seg_path = archive.segment_path(conn)
        if journal is None:
            entry['problems'].append('缺少连接日志，无法判断连接是否正常结束')
        if not seg_path.exists():
            entry['problems'].append('缺少原始段')
            entry['status'] = '异常'
            problems += 1
            continue
        size = seg_path.stat().st_size
        raw = seg_path.read_bytes()
        streaming, tail, _ = archive.parse_segment(conn, max_payload, max_frame)
        final = streaming + tail
        state = (journal or {}).get('state', '?')
        entry['state'] = state
        entry['bytes'] = size
        entry['frames'] = sum(1 for r in final if r['kind'] == 'frame')
        entry['rejects'] = dict(Counter(r['reason'] for r in final if r['kind'] == 'reject'))
        # 记录平铺性：半开区间从 0 开始连续覆盖（打开的连接允许尾部有未决字节）
        pos = 0
        for rec in final:
            if rec['start'] != pos:
                entry['problems'].append('记录区间不连续，解析结果不可信')
                break
            pos = rec['end']
        # 帧字节摘要：帧记录声称的范围必须落在段内
        for rec in final:
            if rec['kind'] == 'frame':
                if rec['end'] > size:
                    entry['problems'].append('帧记录超出原始段范围')
                    break
                digest = hashlib.sha256(raw[rec['start']:rec['end']]).hexdigest()
                frame_locations.setdefault(digest, []).append(conn)
        # 索引核对：必须与原始段的重新解析一致
        idx_records, commits, error = archive.load_index(conn)
        if error:
            entry['problems'].append(f'{error}，请运行 python -m archiver.rebuild')
        elif not commits:
            entry['problems'].append('索引缺少 commit 行，请运行 python -m archiver.rebuild')
        else:
            last = commits[-1]
            if last.get('parsed_upto') != size or last.get('records') != len(idx_records):
                entry['problems'].append(
                    f"索引过期（commit 记录到 {last.get('parsed_upto')} 字节，"
                    f'原始段实际 {size} 字节），请运行 python -m archiver.rebuild')
            elif idx_records == final:
                entry['index'] = '一致'
            elif state in ('open', 'interrupted') and idx_records == streaming:
                entry['index'] = '一致（进行时快照）'
            else:
                entry['problems'].append('索引与原始段不一致，请运行 python -m archiver.rebuild')
        # 连接日志与段大小互相印证
        if journal:
            if state == 'closed':
                if journal.get('bytes') != size:
                    entry['problems'].append(
                        f"原始段大小 {size} 与关闭记录 {journal.get('bytes')} 不符，"
                        '本地写入不完整或文件被改动')
                reason = journal.get('close_reason', '')
                if reason.startswith('recv-error'):
                    entry['notes'].append(f'来源连接异常结束：{reason}')
            elif state in ('open', 'interrupted'):
                entry['notes'].append(
                    '连接未正常关闭：本地采集未完成（进程退出或被终止），结尾完整性无法确认')
        # 结尾半帧归因：只有正常关闭的连接才能把截断归因于来源
        last_rec = final[-1] if final else None
        if last_rec and last_rec['kind'] == 'reject' and last_rec['reason'] == 'truncated':
            where = f"[{last_rec['start']},{last_rec['end']})"
            if state == 'closed':
                reason = (journal or {}).get('close_reason', '')
                if reason == 'local-stop':
                    entry['notes'].append(f'本地停止采集时仍有未收完的帧 {where}')
                else:
                    entry['notes'].append(f'来源断流：对端关闭时仍有未收完的帧 {where}')
            else:
                entry['notes'].append(f'结尾半帧 {where} 不能归因于来源：连接日志未正常关闭')
        # 状态归类
        if entry['problems']:
            entry['status'] = '异常'
            problems += 1
        elif state in ('open', 'interrupted', '?'):
            entry['status'] = '本地未完成'
            incompletes += 1
        else:
            entry['status'] = '完整'
    notes = []
    same_conn = sum(1 for cs in frame_locations.values() if len(cs) > len(set(cs)))
    cross_conn = sum(1 for cs in frame_locations.values() if len(set(cs)) > 1)
    if same_conn:
        notes.append(f'{same_conn} 组帧字节在同一连接内重复出现，已按位置分别保留')
    if cross_conn:
        notes.append(f'{cross_conn} 组帧字节出现在多个连接中；'
                     '无来源序号，不去重、不宣称跨连接衔接')
    code = 1 if problems else (2 if incompletes else 0)
    return code, {'root': str(archive.root), 'connections': connections,
                  'notes': notes, 'exit': code}


def print_report(report, out=sys.stdout):
    conns = report['connections']
    print(f"档案 {report['root']}：{len(conns)} 个连接", file=out)
    if not conns:
        print('（档案为空）', file=out)
    for e in conns:
        rejects = e.get('rejects') or {}
        rtxt = '无' if not rejects else ' '.join(
            f'{REASON_CN.get(k, k)}{v}' for k, v in sorted(rejects.items()))
        print(f"conn-{e['conn']:06d}  状态={e.get('status', '?')}  "
              f"字节={e.get('bytes', '?')}  帧={e.get('frames', '?')}  拒收={rtxt}  "
              f"索引={e.get('index', '?')}", file=out)
        for n in e.get('notes', []):
            print(f'  备注: {n}', file=out)
        for p in e.get('problems', []):
            print(f'  问题: {p}', file=out)
    for n in report['notes']:
        print(f'跨连接备注: {n}', file=out)
    verdict = {0: '全部完整一致', 1: '存在一致性问题', 2: '一致但存在未完成连接'}
    print(f"结论: {verdict[report['exit']]}（退出码 {report['exit']}）", file=out)


def main(argv=None):
    p = argparse.ArgumentParser(
        description='核对档案的原始段、帧位置、CRC 与连接日志，区分来源断流与本地未完成')
    p.add_argument('--archive', required=True, help='档案目录')
    p.add_argument('--max-payload', type=int, default=MAX_PAYLOAD)
    p.add_argument('--max-frame', type=int, default=None)
    p.add_argument('--json', action='store_true', help='输出 JSON 报告')
    a = p.parse_args(argv)
    code, report = check_archive(a.archive, a.max_payload, a.max_frame)
    if a.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_report(report)
    return code


if __name__ == '__main__':
    sys.exit(main())
