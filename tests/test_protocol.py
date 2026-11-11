"""检查合成来源发送的字节与帧封装。"""
import socket,unittest
from pathlib import Path
from source_lab.rtcm import crc24q,frame
from source_lab.tcp import start
class ProtocolTests(unittest.TestCase):
    def test_crc_vector(self):self.assertEqual(crc24q(b'123456789'),0xCDE703)
    def test_stream(self):
        s=start('sources/streams.json')
        try:
            for i in range(2):
                with socket.create_connection(s.server_address,timeout=2) as c:
                    parts=[]
                    while True:
                        b=c.recv(3)
                        if not b:break
                        parts.append(b)
                self.assertEqual(b''.join(parts),Path(f'sources/session{i}.bin').read_bytes())
        finally:s.shutdown();s.server_close()
    def test_max_payload(self):
        self.assertEqual(len(frame(b'x'*1023)),1029)
        with self.assertRaises(ValueError):frame(b'x'*1024)
