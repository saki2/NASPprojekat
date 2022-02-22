package Initialization

import (
	"os"
	"project/structures/LSM"
	"strconv"
)

func CreateDataFiles() {
	// Creates the original directories where data will be stored
	// Function is only called once when the system starts
	if _, err := os.Stat("./Data"); os.IsNotExist(err) {
		err := os.Mkdir("./Data", 0755)
		if err != nil {
			panic(err.Error())
		}
		if _, err := os.Stat("./Data/SSTable"); os.IsNotExist(err) {
			err := os.Mkdir("./Data/SSTable", 0755)
			if err != nil {
				panic(err.Error())
			}
			for i := 1; i <= LSM.MAX_LEVEL; i++ {
				if _, err := os.Stat("./Data/SSTable/Level" + strconv.Itoa(i)); os.IsNotExist(err) {
					err := os.Mkdir("./Data/SSTable/Level"+strconv.Itoa(i), 0755)
					if err != nil {
						panic(err.Error())
					}
				}
			}
		}
	}
}
