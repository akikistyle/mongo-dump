package main

import (
	"bytes"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type Config struct {
	IsCron   bool   `json:"isCron"`   //need in a task
	CronSpec string `json:"cronSpec"` //task spec
	DBs      []DB   `json:"configs"`
}

type DB struct {
	Host string `json:"host"` //db host
	User string `json:"user"` //db user
	Pwd  string `json:"pwd"`  //db password
	Db   string `json:"db"`   //database
	Out  string `json:"out"`  //output file directory
}

func (c *Config) ReadConfig() error {
	data, err := ioutil.ReadFile("conf.json")
	if err != nil {
		return errors.Wrap(err, "read config error")
	}

	err = json.Unmarshal(data, c)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal config")
	}
	return nil
}

func dirExists(path string) (bool, error) {
	var err error
	_, err = os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (c *Config) Dump() error {
	binOk, err := dirExists("./bin")
	if err != nil {
		return errors.Wrap(err, "unable to find mongodb bin")
	}
	if !binOk {
		return errors.New("unable to find mongodb bin, please put in 'mongodb/bin'")
	}
	cmdStr := ""
	goos := runtime.GOOS
	switch goos {
	case "windows":
		cmdStr = ".\\bin\\mongo-dump"
	case "linux":
		cmdStr = "./bin/mongo-dump"
	}
	for _, v := range c.DBs {
		od := v.Out
		od = outputPattern(od)
		cmd := exec.Command(cmdStr, "-h", v.Host, "-u", v.User, "-p", v.Pwd, "--authenticationDatabase", v.Db, "-d", v.Db, "-o", od)
		var out bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err != nil {
			return errors.Wrap(err, "command error")
		}
		log.Printf("Host: %s DB: %s is complete\n", v.Host, v.Db)
	}
	return nil
}

func outputPattern(out string) string {

	if strings.Index(out, "${date}") > -1 {
		out = strings.Replace(out, "${date}", time.Now().Format("20060102"), -1)
	}

	return out
}

func (c *Config) Task() {
	cr := cron.New()
	cr.AddFunc(c.CronSpec, func() {
		err := c.Dump()
		if err != nil {
			log.Println("Dump error:", err)
		}
	})
	cr.Start()
}

func main() {
	var err error
	conf := &Config{}
	err = conf.ReadConfig()
	if err != nil {
		log.Panicln("Read config error:", err)
	}

	if !conf.IsCron {
		//run once
		err = conf.Dump()
		if err != nil {
			log.Panicln("Dump error:", err)
		}
	} else {
		//task
		go conf.Task()
		select {}
	}
}
