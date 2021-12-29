package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

//初始化全局的init
func Init(address []string, chanSize int64) (err error) {
	// 1. 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ACK
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 分区
	config.Producer.Return.Successes = true                   // 确认

	// 2. 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("kafka:producer closed, err:", err)
		return
	}
	//初始化通道
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	//起一个后台goroutine去通道中读取数据
	go sendMsg()
	return
}
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed,err:", err)
				return
			}
			logrus.Infof("send msg to kafka success , pid :%v offset :%v", pid, offset)
		}
	}
}
func MsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
