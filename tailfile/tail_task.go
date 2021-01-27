package tailfile

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/nxadm/tail"
	"github.com/sirupsen/logrus"
	"logAgent/kafka"
	"strings"
	"time"
)

type tailTask struct {
	Path   string
	Topic  string
	TObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTailTask(path, topic string) (*tailTask, error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	tailObj, err := tail.TailFile(path, cfg)
	if err != nil {
		logrus.Errorf("new tail obj failed,err:%v\n", err)
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &tailTask{
		ctx:    ctx,
		cancel: cancel,
		Path:   path,
		Topic:  topic,
		TObj:   tailObj,
	}, nil
}

// 收集日志 发向kafka
func (t *tailTask) run() {
	logrus.Infof("task:%s is collecting for path:%s", t.Topic, t.Path)
	for {
		select {
		case <-t.ctx.Done():
			logrus.Infof("task path:%s is stop...", t.Path)
			return
		case line, ok := <-t.TObj.Lines:
			if !ok {
				logrus.Error("tail file :%v failed", t.TObj.Filename)
				time.Sleep(time.Second)
				continue
			}
			// 如果空行 略过
			if len(strings.Trim(line.Text, "\r")) == 0 {
				continue
			}
			fmt.Println("msg is:", line.Text)
			// 将读出来的一行日志 包装成kafka的msg，放入通道
			msg := &sarama.ProducerMessage{
				Topic: t.Topic,
				Value: sarama.StringEncoder(line.Text),
			}
			kafka.Send2MsgChan(msg)
		}
	}
}
