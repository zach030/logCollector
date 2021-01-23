package etcd

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"testing"
)

func TestInit(t *testing.T) {
	err := Init([]string{"127.0.0.1:2379"})
	value := `[{"path":"D:/AllProjects/Go/kafka_demo/tailf/demo.log","topic":"shopping"},{"path":"D:/AllProjects/Go/kafka_demo/tailf/etcd.log","topic":"web_log"}]`
	_, err = client.Put(context.Background(), "collect_log_conf", value)
	if err != nil {
		logrus.Errorf("put value to etcd failed:%v", err)
		return
	}

	gr, err := client.Get(context.Background(), "collect_log_conf")
	if err != nil {
		logrus.Errorf("get value from etcd failed:%v", err)
		return
	}
	for _, ev := range gr.Kvs {
		fmt.Printf("key:%s value:%s\n", ev.Key, ev.Value)
	}
}

func TestWatch(t *testing.T) {
	Watch()
}