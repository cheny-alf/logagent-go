package tailfile

import (
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logagent/common"
	"logagent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	path  string
	topic string
	tObj  *tail.Tail
}

func newTailTask(path, topic string) *tailTask {

	tt := &tailTask{
		path:  path,
		topic: topic,
	}
	return tt
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.tObj, err = tail.TailFile(t.path, cfg)
	return
}

func Init(allConf []common.CollectEntry) (err error) {

	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		err := tt.Init() //创建日志收集任务
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
			continue
		}
		logrus.Infof("create a new tail task for path:%s success", conf.Path)
		go tt.run()
	}
	return
}

func (t *tailTask) run() {
	//读取日志，发到kafka
	logrus.Infof("collect for path:%s is running", t.path)
	for {
		//循环读数据
		line, ok := <-t.tObj.Lines
		if !ok {
			logrus.Warnf("tail file close reopen,filename: %s", t.path)
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
		msg.Topic = t.topic
		msg.Value = sarama.StringEncoder(line.Text)
		kafka.MsgChan(msg)
	}
}
