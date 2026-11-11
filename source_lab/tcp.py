"""顺序连接读取不同合成段，写入分片不代表消息边界。"""
import argparse,json,socketserver,threading
from pathlib import Path
def start(config,port=0):
    path=Path(config);value=json.loads(path.read_text(encoding='utf8'));lock=threading.Lock();position=[0]
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            with lock:i=position[0];position[0]+=1
            if i>=len(value['sessions']):return
            data=(path.parent/value['sessions'][i]).read_bytes();offset=0;j=0
            while offset<len(data):
                size=value['write_sizes'][j%len(value['write_sizes'])];self.request.sendall(data[offset:offset+size]);offset+=size;j+=1
    server=socketserver.ThreadingTCPServer(('127.0.0.1',port),Handler);server.daemon_threads=True
    threading.Thread(target=server.serve_forever,daemon=True).start();return server
def main():
    p=argparse.ArgumentParser();p.add_argument('config');p.add_argument('--port',type=int,default=8766);a=p.parse_args();s=start(a.config,a.port)
    print(s.server_address[1],flush=True)
    try:threading.Event().wait()
    except KeyboardInterrupt:pass
    finally:s.shutdown();s.server_close()
if __name__=='__main__':main()
