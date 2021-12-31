package tailfile

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"logagent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	path   string
	topic  string
	tObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}

func newTailTask(path, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
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

func (t *tailTask) run() {
	//读取日志，发到kafka
	logrus.Infof("collect for path:%s is running", t.path)
	for {
		select {
		case <-t.ctx.Done(): //只要调用t.cancel() 就会收到
			logrus.Infof("path:%s is stoping", t.path)
			return
		case line, ok := <-t.tObj.Lines:
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
		//循环读数据

	}
}
