package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"logAgent/common"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

var (
	client *clientv3.Client
)

func Init(addr []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   addr,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		logrus.Errorf("connect to etcd failed,err:%v", err)
		return
	}
	return
}

// 拉取日志收集配置项的函数
func GetConf(key string) (collectLists []*common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	gr, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from by key:%s, etcd failed:%v", key, err)
		return nil, err
	}
	if len(gr.Kvs) == 0 {
		logrus.Errorf("get conf length = 0, from by key:%s, etcd failed:%v", key, err)
		return nil, err
	}
	ret := gr.Kvs[0]
	err = json.Unmarshal(ret.Value, &collectLists)
	if err != nil {
		logrus.Errorf("unmarshal conf to struct failed:%v", err)
		return nil, err
	}
	cancel()
	return collectLists,nil
	//for _, ev := range gr.Kvs {
	//	fmt.Printf("key:%s value:%s\n", ev.Key, ev.Value)
	//}
}

func Watch() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		logrus.Errorf("connect to etcd failed,err:%v", err)
		return
	}
	defer cli.Close()
	watchChan := cli.Watch(context.Background(), "name")
	for wresp := range watchChan {
		for _, evt := range wresp.Events {
			fmt.Printf("type:%s key:%s value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
		}
	}
}
