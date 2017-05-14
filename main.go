package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//DataEntry is the line converted to mongodb data
type DataEntry struct {
	DESC_MUNI       string
	DES_DISTRIC     string
	ANYMES_CAL      string
	TipoSubministro string
	Consumo         string
	Uso             string
	ContratoCOD     string
}
type Contract struct {
	ContratoCOD string
	Data        []DataEntry
}

//MongoConfig stores the configuration of mongodb to connect
type MongoConfig struct {
	Ip         string `json:"ip"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

var mongoConfig MongoConfig

func readMongoConfig() {
	file, e := ioutil.ReadFile("mongoConfig.json")
	if e != nil {
		fmt.Println("error:", e)
	}
	content := string(file)
	json.Unmarshal([]byte(content), &mongoConfig)
}

func getSession() (*mgo.Session, error) {
	session, err := mgo.Dial("mongodb://" + mongoConfig.Ip)
	if err != nil {
		panic(err)
	}
	//defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	return session, err
}
func getCollection(session *mgo.Session) *mgo.Collection {

	c := session.DB(mongoConfig.Database).C(mongoConfig.Collection)
	return c
}

func lineToStruct(line string) DataEntry {
	val := strings.Split(line, ";")
	//fmt.Println(val)
	dataEntry := DataEntry{val[0], val[1], val[2], val[3], val[4], val[5], val[6]}
	return dataEntry
}

func saveDataEntryToMongo(c *mgo.Collection, dataEntry DataEntry) {
	//first, check if the user (ContratoCOD) already exists
	result := Contract{}
	err := c.Find(bson.M{"contratocod": dataEntry.ContratoCOD}).One(&result)
	if err != nil {
		//user not found, so let's add a new entry
		var arrDataEntry []DataEntry
		arrDataEntry = append(arrDataEntry, dataEntry)
		contract := Contract{dataEntry.ContratoCOD, arrDataEntry}
		err = c.Insert(contract)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		result.Data = append(result.Data, dataEntry)
		err = c.Update(bson.M{"contratocod": dataEntry.ContratoCOD}, result)
		if err != nil {
			log.Fatal(err)
		}
	}

}
func main() {
	readMongoConfig()

	session, err := getSession()
	if err != nil {
		log.Fatal(err)
	}
	c := getCollection(session)

	//read line by line
	file, err := os.Open("datasets/bigdata.txt")
	if err != nil {
		fmt.Println("no file found in the configured path")
	}
	fscanner := bufio.NewScanner(file)
	i := 0
	for fscanner.Scan() {
		line := fscanner.Text()
		newDataEntry := lineToStruct(line)
		fmt.Println("line " + "\x1b[35;1m" + strconv.Itoa(i) + "\x1b[0m" + ": " + "\x1b[36;1m" + newDataEntry.ContratoCOD + "\x1b[0m")
		fmt.Println(newDataEntry)
		fmt.Println("consum: " + "\x1b[32;1m" + newDataEntry.Consumo + "\x1b[0m")
		saveDataEntryToMongo(c, newDataEntry)
		i++
	}
}
