package common

import (
	"fmt"
	"net"
	"strings"
)

//collectEntry 要收集的日志的配置项的结构体
type CollectEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

//获取本地的IP函数
func GetOutboundIp() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	fmt.Println(localAddr.String())
	ip = strings.Split(localAddr.IP.String(), ":")[0]
	return
}
