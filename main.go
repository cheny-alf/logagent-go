package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logagent/common"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/tailfile"
)

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

//真真的业务逻辑

func run() {
	select {}
}

func main() {

	//获取本机IP 为后续的etcd获取文件做基础
	ip, err := common.GetOutboundIp()
	if err != nil {
		logrus.Errorf("get ip failed err:%v", err)
		return
	}
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
		logrus.Errorf("load config failed err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	//2.初始化（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success")

	/**
	改进！！
	*/
	//初始化etcd链接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed, err:%v", err)
		return
	}
	//从etcd中拉取要收集日志的配置项
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)
	allconf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("get conf from  etcd failed, err:%v", err)
		return
	}
	fmt.Println(allconf)
	//监控etcd中configObj.EtcdConfig.CollectKey对应值的变化
	go etcd.WatchConf(collectKey)
	//3。根据配置文件中的日志路径使用tail去收集日志
	err = tailfile.Init(allconf)
	if err != nil {
		logrus.Errorf("init tailfile failed,err : ", err)
		return
	}
	logrus.Info("init tailfile success")
	run()
}
