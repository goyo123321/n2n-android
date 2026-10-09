package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// readTCPPacket 从 TCP 连接读一个 STUN 包（兼容 RFC 5766 §7.2.2 / RFC 6062 §7）。
//
// 被 turn_lite.go 的 readLoopTCP 使用（TURN over TCP transport 模式）。
//
// ★ 本文件曾包含完整的 RFC 6062 TURNTCPAllocation（400+ 行），
//   但数据面从未使用它。已删除，只保留 readTCPPacket。
func readTCPPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	// RFC 5766 §7.2.2：TURN over TCP 包前 2 bit 是 0b01
	if header[0]&0xC0 == 0x40 {
		length := int(binary.BigEndian.Uint16(header[2:4]))
		totalAfterHeader := length + ((4 - length%4) & 3)
		body := make([]byte, totalAfterHeader)
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
		full := make([]byte, 4+length)
		copy(full, header)
		copy(full[4:], body[:length])
		return full, nil
	}

	// STUN 原生（20 字节 header + body）
	rest := make([]byte, 16)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return nil, err
	}

	bodyLen := int(binary.BigEndian.Uint16(header[2:4]))
	if bodyLen > 65535-20 {
		return nil, fmt.Errorf("非法 STUN bodyLen: %d", bodyLen)
	}

	body := make([]byte, bodyLen)
	if bodyLen > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
	}
	full := make([]byte, 20+bodyLen)
	copy(full, header)
	copy(full[4:], rest)
	copy(full[20:], body)
	return full, nil
}
