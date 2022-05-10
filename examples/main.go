package main

import (
	"fmt"
	"github.com/run-bigpig/ygadata-go-sdk/ygadata"
	"log"
)

func main()  {
	config := ygadata.BatchConfig{
		ServerUrl: "https://datalog.66yytx.com/logagent",
		Project: "123333",
		User: "data",
		Sk: "11111111",
		AutoFlush: true,//自动上传
		BatchSize: 100,//
		Interval:  10,//每格十秒上传一次
	}
	consumer, err := ygadata.NewBatchConsumerWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	yga:=ygadata.New(consumer)
	defer yga.Close()
	properties:=map[string]interface{}{"level":1}
	err =yga.Track("test1","testuseeee","apptest","ios","2021-02-15 10:00:00","login",1,properties)
	if err!=nil {
		log.Println("事件上报添加失败",err)
		return
	}
	yga.Flush()
	for  {

	}
}
