package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
	"logAgent/etcd"
	"logAgent/kafka"
	"logAgent/tailfile"
)

var (
	configObj = new(Config)
)

type Config struct {
	KafkaConfig `ini:"kafka"`
	Collect     `ini:"collect"`
	EtcdConfig  `ini:"etcd"`
}

// 日志收集客户端
func main() {
	// 读配置文件
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("[Error]----load config failed, err:%v\n", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	// 初始化连接kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("[Error]-----init kafka failed,err:%v\n", err)
		return
	}
	logrus.Info("[Kafka]------init kafka success!")
	// 从etcd中拉取要收集的日志的配置项
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("[Error]-----init etcd failed,err:%v\n", err)
		return
	}
	allConfs,err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("[Error]-----get conf from etcd failed,err:%v\n", err)
		return
	}
	// 将日志发往chan
	err = tailfile.Init(allConfs)
	if err != nil {
		logrus.Errorf("[Error]-----start tail collect client failed,err:%v\n", err)
		return
	}
	select {

	}
}
