"""RTCM3 帧边界解析：只认 0xD3 前导、保留位、十位长度与 CRC-24Q，不做定位解算。

解析结果只取决于字节序列本身，与 feed() 的分块方式无关；载荷中的 0xD3
不会被当成新帧。除候选帧外不缓存字节，跨 feed() 留存的字节数不超过
max_frame，可用 retained / retained_peak 观察这一上界。

拒收规则（每条记录都是连接内绝对偏移的半开区间 [start, end)）：
- noise       扫描帧头时遇到的非 0xD3 字节，按连续段记录；
- bad-header  0xD3 后保留位非零或长度超限，只拒收 1 字节，从下一字节重新扫描；
- bad-crc     头部合法的候选帧校验不符，拒收整个候选区间，从候选末尾继续扫描；
- truncated   字节流结束时候选帧未收完（头部不全时 expected 为 None）。
"""
from source_lab.rtcm import crc24q

PREAMBLE = 0xD3
HEADER_LEN = 3
CRC_LEN = 3
MAX_PAYLOAD = 1023  # 十位长度字段的上限

_SCAN, _HEAD, _BODY = range(3)


class FrameParser:
    """把任意分块的字节流解析成帧记录与拒收记录。"""

    def __init__(self, conn=0, max_payload=MAX_PAYLOAD, max_frame=None):
        if not 0 <= max_payload <= MAX_PAYLOAD:
            raise ValueError('max_payload 必须在 0..1023 之间')
        if max_frame is None:
            max_frame = HEADER_LEN + max_payload + CRC_LEN
        if max_frame < HEADER_LEN + CRC_LEN:
            raise ValueError('max_frame 放不下最小帧')
        self.conn = conn
        self.max_payload = max_payload
        self.max_frame = max_frame
        self._buf = bytearray()   # 仅候选帧字节；噪声字节记录区间后立即丢弃
        self._base = 0            # _buf[0] 在连接字节流中的绝对偏移
        self._state = _SCAN
        self._noise_start = None  # 未结噪声段起点
        self._need = 0            # 候选帧总长度（头部合法后确定）
        self._seq = 0
        self._finished = False
        self.retained_peak = 0    # 跨 feed() 留存字节数峰值

    @property
    def retained(self):
        """当前跨 feed() 留存的字节数，上界为 max_frame。"""
        return len(self._buf)

    @property
    def parsed_upto(self):
        """已消费的字节数（等于应已写入原始段的字节数）。"""
        return self._base + len(self._buf)

    def feed(self, data):
        """喂入任意长度的下一块字节，返回本次产生的记录列表。"""
        if self._finished:
            raise ValueError('解析器已结束，不能再喂入字节')
        if not data:
            return []
        self._buf += data
        out = []
        while True:
            if self._state is _SCAN:
                i = self._buf.find(PREAMBLE)
                if i < 0:
                    if self._buf:  # 空缓冲区不开启噪声段，否则会产生空区间
                        if self._noise_start is None:
                            self._noise_start = self._base
                        self._base += len(self._buf)
                        self._buf.clear()
                    break
                if i > 0 or self._noise_start is not None:
                    start = self._noise_start if self._noise_start is not None else self._base
                    out.append(self._record('reject', start, self._base + i, reason='noise'))
                    self._noise_start = None
                if i:
                    del self._buf[:i]
                    self._base += i
                self._state = _HEAD
            elif self._state is _HEAD:
                if len(self._buf) < HEADER_LEN:
                    break
                b1, b2 = self._buf[1], self._buf[2]
                length = ((b1 & 0x03) << 8) | b2
                if b1 & 0xFC:
                    out.append(self._bad_header('reserved-bits', length))
                    continue
                total = HEADER_LEN + length + CRC_LEN
                if length > self.max_payload or total > self.max_frame:
                    out.append(self._bad_header('length-over-limit', length))
                    continue
                self._need = total
                self._state = _BODY
            else:  # _BODY
                if len(self._buf) < self._need:
                    break
                cand = bytes(self._buf[:self._need])
                length = self._need - HEADER_LEN - CRC_LEN
                stored = int.from_bytes(cand[HEADER_LEN + length:], 'big')
                actual = crc24q(cand[:HEADER_LEN + length])
                if stored == actual:
                    payload = cand[HEADER_LEN:HEADER_LEN + length]
                    mtype = (payload[0] << 4 | payload[1] >> 4) if length >= 2 else None
                    out.append(self._record('frame', self._base, self._base + self._need,
                                            payload_len=length, type=mtype,
                                            crc=f'{stored:06x}'))
                else:
                    out.append(self._record('reject', self._base, self._base + self._need,
                                            reason='bad-crc', payload_len=length,
                                            crc_stored=f'{stored:06x}',
                                            crc_actual=f'{actual:06x}'))
                del self._buf[:self._need]
                self._base += self._need
                self._state = _SCAN
        self.retained_peak = max(self.retained_peak, len(self._buf))
        return out

    def finish(self):
        """字节流结束：结算未完的噪声段或半帧。重复调用返回空列表。"""
        if self._finished:
            return []
        self._finished = True
        end = self._base + len(self._buf)
        if self._state is _SCAN:
            if self._noise_start is not None and self._noise_start < end:
                return [self._record('reject', self._noise_start, end, reason='noise')]
            return []
        expected = self._need if self._state is _BODY else None
        return [self._record('reject', self._base, end, reason='truncated',
                             expected=expected, got=len(self._buf))]

    def _bad_header(self, detail, length):
        rec = self._record('reject', self._base, self._base + 1,
                           reason='bad-header', detail=detail, declared_len=length)
        del self._buf[0]
        self._base += 1
        self._state = _SCAN
        return rec

    def _record(self, kind, start, end, **fields):
        rec = {'kind': kind, 'conn': self.conn, 'seq': self._seq, 'start': start, 'end': end}
        rec.update(fields)
        self._seq += 1
        return rec
