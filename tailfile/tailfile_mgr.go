package tailfile

import (
	"github.com/sirupsen/logrus"
	"logagent/common"
)

//tailTask 的管理者
type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask       //所有的tailtask任务
	collectEntryList []common.CollectEntry      //所有配置项
	confChan         chan []common.CollectEntry //等待新配置的通道
}

var (
	ttMgr *tailTaskMgr
)

func Init(allConf []common.CollectEntry) (err error) {
	//allConf 里面存了若干个日志收集项
	//针对每一个日志收集项目创建对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry), //这里做了一个阻塞的管道
	}
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		err := tt.Init() //创建日志收集任务
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
			continue
		}
		logrus.Infof("create a new tail task for path:%s success", conf.Path)
		//把创建的tailTask存储起来，方便后续管理
		ttMgr.tailTaskMap[tt.path] = tt
		go tt.run()
	}
	go ttMgr.watch() //在后台等新的配置下发
	return
}
func (t *tailTaskMgr) watch() {
	for {
		//派一个小弟等着新的配置来
		newConf := <-t.confChan //从通道中取到值 说明新的配置来了
		//配置来了之后 应该管理一下之前启动的那些tailTask
		logrus.Infof("get new conf from etcd ,conf:%v。start manage tailTask ", newConf)
		for _, conf := range newConf {
			//1. 原本配置文件中已经存在的任务就不用动
			if t.isExist(conf) {
				continue
			}
			//2. 原本没有的配置文件要新创建一个tailtask任务
			tt := newTailTask(conf.Path, conf.Topic)
			err := tt.Init() //创建日志收集任务
			if err != nil {
				logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
				continue
			}
			logrus.Infof("create a new tail task for path:%s success", conf.Path)
			//把创建的tailTask存储起来，方便后续管理
			ttMgr.tailTaskMap[tt.path] = tt
			//起一个后台的goroutine去收集日志
			go tt.run()
		}
		//3. 原本有的现在没有的配置文件要把他的tailtask停止掉
		//找出tailTaskmap中存在，但是在newConf不存在的那些tailtask，把他们都停掉
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if !found {
				//这个tailtask要停掉
				logrus.Infof("the task collect path:%s need to stop", task.path)
				delete(t.tailTaskMap, key) //从map中删掉
				task.cancel()
			}
		}
	}
}
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
