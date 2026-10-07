#!/usr/bin/env python3
"""
把 Loyalsoldier/v2ray-rules-dat 的 .dat 转成 text
用法: python3 convert_geo.py <geoip.dat> <geosite.dat> <code> <out_dir>
"""

import sys
import os

def read_varint(data, pos):
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7f) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return result, pos

def iter_fields(data):
    pos = 0
    while pos < len(data):
        try:
            tag, pos = read_varint(data, pos)
        except Exception:
            break
        field_num = tag >> 3
        wire_type = tag & 7

        if wire_type == 0:
            val, pos = read_varint(data, pos)
            yield field_num, wire_type, val
        elif wire_type == 1:
            if pos + 8 > len(data): return
            val = data[pos:pos+8]; pos += 8
            yield field_num, wire_type, val
        elif wire_type == 2:
            l, pos = read_varint(data, pos)
            if pos + l > len(data): return
            val = data[pos:pos+l]; pos += l
            yield field_num, wire_type, val
        elif wire_type == 5:
            if pos + 4 > len(data): return
            val = data[pos:pos+4]; pos += 4
            yield field_num, wire_type, val
        else:
            return

# ============ GeoIP ============

def parse_cidr(data):
    ip, prefix = None, 0
    for fn, wt, val in iter_fields(data):
        if fn == 1 and wt == 2: ip = val
        elif fn == 2 and wt == 0: prefix = val
    return ip, prefix

def parse_geoip_entry(data):
    code, cidrs = None, []
    for fn, wt, val in iter_fields(data):
        if fn == 1 and wt == 2:
            code = val.decode('utf-8', errors='replace')
        elif fn == 2 and wt == 2:
            cidrs.append(parse_cidr(val))
    return code, cidrs

def format_ip(ip_bytes, prefix):
    if ip_bytes is None: return None
    if len(ip_bytes) == 4:
        return f"{ip_bytes[0]}.{ip_bytes[1]}.{ip_bytes[2]}.{ip_bytes[3]}/{prefix}"
    if len(ip_bytes) == 16:
        parts = [f"{ip_bytes[i]:02x}{ip_bytes[i+1]:02x}" for i in range(0, 16, 2)]
        return f"{':'.join(parts)}/{prefix}"
    return None

def convert_geoip(path, want_code):
    data = open(path, 'rb').read()
    out = []
    for fn, wt, val in iter_fields(data):
        if fn == 1 and wt == 2:
            code, cidrs = parse_geoip_entry(val)
            if code == want_code:
                for ip, prefix in cidrs:
                    line = format_ip(ip, prefix)
                    if line: out.append(line)
    return out

# ============ GeoSite ============
# Domain.Type: 0=Plain, 1=Regex, 2=Domain, 3=Full

def parse_domain(data):
    typ, value = 0, None
    for fn, wt, val in iter_fields(data):
        if fn == 1 and wt == 0: typ = val
        elif fn == 2 and wt == 2:
            value = val.decode('utf-8', errors='replace')
    return typ, value

def parse_geosite_entry(data):
    code, domains = None, []
    for fn, wt, val in iter_fields(data):
        if fn == 1 and wt == 2:
            code = val.decode('utf-8', errors='replace')
        elif fn == 2 and wt == 2:
            domains.append(parse_domain(val))
    return code, domains

def convert_geosite(path, want_code):
    data = open(path, 'rb').read()
    out = []
    for fn, wt, val in iter_fields(data):
        if fn == 1 and wt == 2:
            code, domains = parse_geosite_entry(val)
            if code == want_code:
                for typ, value in domains:
                    if value is None: continue
                    if typ == 1: out.append(f"regexp:{value}")
                    elif typ == 2: out.append(f"domain:{value}")
                    elif typ == 3: out.append(f"full:{value}")
                    else: out.append(value)  # Plain
    return out

# ============ main ============

def main():
    if len(sys.argv) < 5:
        print("用法: convert_geo.py <geoip.dat> <geosite.dat> <code> <out_dir>", file=sys.stderr)
        sys.exit(1)

    geoip_path, geosite_path, code, out_dir = sys.argv[1:5]
    os.makedirs(out_dir, exist_ok=True)

    print(f"[GeoIP] 解析 {geoip_path} (code={code})...", file=sys.stderr)
    geoip_lines = sorted(set(convert_geoip(geoip_path, code)))
    print(f"[GeoIP] ✅ {len(geoip_lines)} 条", file=sys.stderr)
    with open(os.path.join(out_dir, "geoip_cn.txt"), "w") as f:
        f.write("\n".join(geoip_lines) + "\n")

    print(f"[GeoSite] 解析 {geosite_path} (code={code})...", file=sys.stderr)
    geosite_lines = sorted(set(convert_geosite(geosite_path, code)))
    print(f"[GeoSite] ✅ {len(geosite_lines)} 条", file=sys.stderr)
    with open(os.path.join(out_dir, "geosite_cn.txt"), "w") as f:
        f.write("\n".join(geosite_lines) + "\n")

if __name__ == "__main__":
    main()
