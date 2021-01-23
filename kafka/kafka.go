package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"sync"
)

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

//初始化全局的kafka client
func Init(address []string, chanSize int64) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Errorf("producer closed,err:%v\n", err)
		return
	}
	// 初始化msgchan
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	go sendMsg()
	return
}

//向外暴露msgchan
func Send2MsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}

// 从通道中 读取消息,发送给kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warningf("send msg to kafka failed,err:%v", err)
				return
			}
			logrus.Infof("send msg to kafka success, pid:%v, offset:%v", pid, offset)
		}
	}
}

func consumeMsg() {
	// 生成消费者
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		logrus.Errorf("failed to start consumer:%v\n", err)
		return
	}
	// 根据topic 获取分区list
	partitionList, err := consumer.Partitions("shopping")
	if err != nil {
		logrus.Errorf("failed to get list of partitionList:%v\n", err)
		return
	}
	var wg sync.WaitGroup
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition("shopping", int32(partition), sarama.OffsetNewest)
		if err != nil {
			logrus.Errorf("failed to start consumer for partition:%v ,err:%v\n", pc, err)
			return
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				logrus.Infof("Partition:%v, offset:%v, key:%s, value:%s\n", msg.Partition,
					msg.Offset, msg.Key, msg.Value)
			}
		}(pc)
	}
	wg.Wait()
}
