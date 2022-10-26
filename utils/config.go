package utils

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	TotalMessageSize int
	Topics           []string
	MessageSize      int
	Threads          int
	FlowCtrl         bool
	Interval         int
	SchemaId         int
	Brokers          []string
	MethodId         int
	Eip              string
	MessageNum       int
	Rond2Stop        float64
}

func NewConfByFile(path string) *Config {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Read conf file %s with error %v", path, err)
	}
	msgSize := viper.GetInt("required.recordnum") // RowNumPerFile
	msgNum := viper.GetInt("required.sndnum")
	
	config := &Config{
		Topics:           viper.GetStringSlice("required.topics"),
		MessageSize:      msgSize,
		Threads:          viper.GetInt("required.threadsnum"),
		MessageNum:       msgNum, // FileNum
		Rond2Stop:        viper.GetFloat64("required.runtostop"),
		FlowCtrl:         viper.GetBool("optional.flow"),
		Interval:         viper.GetInt("optional.flowinterval"),
		SchemaId:         viper.GetInt("required.schemaname"),
		Brokers:          viper.GetStringSlice("required.brokerips"),
		MethodId:         viper.GetInt("test.usemethod"),
		Eip:              viper.GetString("required.eip"),
		TotalMessageSize: msgNum * msgSize,
	}
	return config
}

func (c *Config) Validate() {

	if len(c.Topics) == 0 || c.SchemaId <= 0 {
		log.Fatalln("缺少必填项：topic or schema,请修改config")
	}
	if c.Rond2Stop > 0 && c.MessageNum > 0 {
		log.Fatalln("总时长和总发送数量sndnum不能同时大于0,请修改config")
	}
	if c.FlowCtrl && c.Interval <= 0 {
		log.Fatalln("流量控制时长flowinterval必须大于0,请修改config")
	}
	if c.MessageNum > 0 {
		if c.Threads > c.MessageNum {
			c.Threads = c.MessageNum
		}
		if c.MessageNum%c.Threads != 0 {
			log.Fatalln("本版本仅支持sndnum是threadsnum的倍数,请修改config")
		}
	}
}
