package tailfile

import (
	"github.com/sirupsen/logrus"
	"logAgent/common"
)

var (
	ttMgr *TailTaskMgr
)

type TailTaskMgr struct {
	TailTaskMap      map[string]*tailTask       // 所有tailtask
	collectEntryList []common.CollectEntry      // 所有配置项
	confChan         chan []common.CollectEntry // 等待新配置的通道
}

func Init(allConfs []common.CollectEntry) (err error) {
	ttMgr = &TailTaskMgr{
		TailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConfs,
		confChan:         make(chan []common.CollectEntry),
	}
	for _, conf := range allConfs {
		tt, err := NewTailTask(conf.Path, conf.Topic)
		if err != nil {
			logrus.Errorf("new tail obj for path:%s, topic:%s, failed", conf.Path, conf.Topic)
			continue
		}
		logrus.Infof("create a tail task for path:%s, topic:%s", conf.Path, conf.Topic)
		ttMgr.TailTaskMap[tt.Path] = tt
		go tt.run()
	}
	go ttMgr.Watch()
	return
}

func (t *TailTaskMgr) Watch() {
	for {
		newConf := <-t.confChan
		logrus.Info("get new etcd conf:%v, start watch task...", newConf)
		for _, conf := range newConf {
			// 原来有的：不调整
			if t.isExist(conf) {
				continue
			}
			// 原来没有的：新建
			tt, err := NewTailTask(conf.Path, conf.Topic)
			if err != nil {
				logrus.Errorf("new tail obj for path:%s, topic:%s, failed", conf.Path, conf.Topic)
				continue
			}
			t.TailTaskMap[conf.Path] = tt
			// 原来有，现在无：需删除
		}
	}
}

func (t *TailTaskMgr) isExist(conf common.CollectEntry) bool {
	_, ok := t.TailTaskMap[conf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
