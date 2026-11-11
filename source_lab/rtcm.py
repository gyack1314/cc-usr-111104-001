"""合成RTCM传输帧，不包含定位解算。"""
def crc24q(data):
    crc=0
    for byte in data:
        crc^=byte<<16
        for _ in range(8):
            crc<<=1
            if crc&0x1000000:crc^=0x1864CFB
    return crc&0xFFFFFF
def frame(payload):
    if len(payload)>1023:raise ValueError('载荷过长')
    data=bytes([0xD3,len(payload)>>8,len(payload)&255])+payload
    return data+crc24q(data).to_bytes(3,'big')
