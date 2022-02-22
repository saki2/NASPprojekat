package Configuration

import (
	"encoding/json"
	"fmt"
	"os"
)

type Configuration struct {

	WalSegmentSize uint64				`json:"WalSegmentSize"`	// Number of appends per segment

	MemtableCapacity uint64				`json:"MemtableCapacity"`

	MemtableMaxHeight int				`json:"MemtableMaxHeight"`

	BloomFalsePositiveRate float64 		`json:"BloomFalsePositiveRate"`

	LRUCapacity int						`json:"LRUCapacity"`

	LSMMaxLevel int						`json:"LSMMaxLevel"`

	MaxRequestPerInterval int			`json:"MaxRequestPerInterval"`
	Interval int64						`json:"Interval"`

}

func LoadConfig() *Configuration {
	config := Configuration{}
	configFile, err := os.Open("./Configuration/ConfigFile.json")
	if err != nil {
		return nil
	} 	// Configuration file is non-existent
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	if err != nil {
		return nil
	}
	return &config
}

func SetConfig() {

}

func (config *Configuration) Check() {
	fmt.Println(config.WalSegmentSize, config.MemtableCapacity, config.BloomFalsePositiveRate, config.LRUCapacity, config.MaxRequestPerInterval, config.Interval)
}

