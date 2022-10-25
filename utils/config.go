package utils

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	TotalRecords int
	Topics    []string
	Records   int
	Threads   int
	FlowCtrl  bool
	Interval  int
	SchemaId  int
	Brokers   []string
	MethodId  int
	Eip       string
	SndNum    int
	Rond2Stop float64
}

func NewConfByFile(path string) *Config {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Read conf file %s with error %v", path, err)
	}
	numPerFile := viper.GetInt("required.recordnum")  // RowNumPerFile
	numFile := viper.GetInt("required.sndnum")
	
	config := &Config{
		Topics:    viper.GetStringSlice("required.topics"),
		Records:   numPerFile,
		Threads:   viper.GetInt("required.threadsnum"),
		SndNum:    numFile,    // FileNum
		Rond2Stop: viper.GetFloat64("required.runtostop"),
		FlowCtrl:  viper.GetBool("optional.flow"),
		Interval:  viper.GetInt("optional.flowinterval"),
		SchemaId:  viper.GetInt("required.schemaname"),
		Brokers:   viper.GetStringSlice("required.brokerips"),
		MethodId:  viper.GetInt("test.usemethod"),
		Eip:       viper.GetString("required.eip"),
		TotalRecords: numFile * numPerFile ,
	}
	return config
}

func (c *Config) Validate() {

	if len(c.Topics) == 0 || c.SchemaId <= 0 {
		log.Fatalln("缺少必填项：topic or schema,请修改config")
	}
	if c.Rond2Stop > 0 && c.SndNum > 0 {
		log.Fatalln("总时长和总发送数量sndnum不能同时大于0,请修改config")
	}
	if c.FlowCtrl && c.Interval <= 0 {
		log.Fatalln("流量控制时长flowinterval必须大于0,请修改config")
	}
	if c.SndNum > 0 {
		if c.Threads > c.SndNum {
			c.Threads = c.SndNum
		}
		if c.SndNum%c.Threads != 0 {
			log.Fatalln("本版本仅支持sndnum是threadsnum的倍数,请修改config")
		}
	}
}
