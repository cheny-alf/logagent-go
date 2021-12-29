package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logagent/kafka"
	"logagent/tailfile"
	"strings"
	"time"
)

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

//真真的业务逻辑

//tailobj --> log --> client -->kafka
func run() (err error) {
	for {
		//循环读数据
		line, ok := <-tailfile.TailObj.Lines
		if !ok {
			logrus.Warnf("tail file close reopen,filename: %s", tailfile.TailObj.Filename)
			time.Sleep(time.Second)
			continue
		}
		//抛弃掉空行 跳过
		if len(strings.Trim(line.Text, "\r")) == 0 {
			continue
		}
		//利用通道 将同步的代码 改为异步
		//把读出来的一行日志 包装程kafka里面的数据类型放到通道中
		msg := &sarama.ProducerMessage{}
		msg.Topic = "web_log"
		msg.Value = sarama.StringEncoder(line.Text)
		kafka.MsgChan(msg)

	}
}

func main() {
	var configObj = new(Config)
	//1.读取配置文件go ini
	//cfg, err := ini.Load("./conf/config.ini")
	//if err != nil {
	//	logrus.Error("load config failed err:%v", err)
	//}
	//kafkaAddr := cfg.Section("kafka").Key("address").String()
	//fmt.Println(kafkaAddr)
	err := ini.MapTo(configObj, ".\\conf\\config.ini")
	if err != nil {
		logrus.Error("load config failed err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	//2.初始化（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.ChanSize)
	if err != nil {
		logrus.Error("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success")
	//3。根据配置文件中的日志路径使用tail去收集日志
	err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	if err != nil {
		logrus.Error("init tailfile failed,err : ", err)
		return
	}
	logrus.Info("init tailfile success")

	//4.把日志通过sarama发往kafka

	err = run()
	if err != nil {
		logrus.Error("run failed , err :%v", err)
		return
	}
}
