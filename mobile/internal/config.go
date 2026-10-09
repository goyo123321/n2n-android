 type PeerInfo struct {
 	ClientID      string
 	VirtualIP     string
 	PubIP         string
 	PubPort       int
-	LanIP         string
+	LanIPs        []string  // ★ 对端的所有局域网 IP
 	LanPort       int
 	...
 }
