package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/evalphobia/logrus_sentry"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"supercronic/cron"
	"supercronic/crontab"
	"supercronic/log/hook"
	"sync"
	"syscall"
	"time"
)

type SentryConfig struct {
	Dsn         string `json:"dsn" yaml:"dsn"`
	Environment string `json:"environment" yaml:"environment"`
}
type Config struct {
	Json        bool         `json:"json" yaml:"json"`
	Debug       bool         `json:"debug" yaml:"debug"`
	Prefix      string       `json:"prefix" yaml:"prefix"`
	SplitLogs   bool         `json:"split-logs" yaml:"split-logs"`
	Overlapping bool         `json:"overlapping" yaml:"overlapping"`
	Sentry      SentryConfig `json:"sentry" yaml:"sentry"`
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] CRONTAB\n\nAvailable options:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	confObject := Config{
		Json:        false,
		Debug:       false,
		Prefix:      "supercronic",
		Overlapping: false,
		SplitLogs:   false,
		Sentry: SentryConfig{
			Dsn:         "",
			Environment: "",
		},
	}

	config := flag.String("config", "", "path to config file")

	debug := flag.Bool("debug", false, "enable debug logging")
	json := flag.Bool("json", false, "enable JSON logging")
	test := flag.Bool("test", false, "test crontab (does not run jobs)")
	splitLogs := flag.Bool("split-logs", false, "split log output into stdout/stderr")
	sentryDSN := flag.String("sentry-dsn", "", "enable Sentry error logging, using provided DSN")
	sentryAlias := flag.String("sentryDsn", "", "alias for sentry-dsn")
	sentryEnv := flag.String("sentryEnv", "", "environment tag for sentry-dsn")
	prefix := flag.String("prefix", "supercronic", "prefix for the logs(stored in the field 'prefix' if json is enabled)")

	overlapping := flag.Bool("overlapping", false, "enable tasks overlapping")
	flag.Parse()

	if *config != "" {
		yamlFile, err := ioutil.ReadFile(*config)
		if err != nil {
			log.Fatalf("#%v ", err)
		}
		err = yaml.Unmarshal(yamlFile, &confObject)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}
	confObject.Debug = *debug || confObject.Debug
	confObject.Json = *json || confObject.Json
	confObject.SplitLogs = *splitLogs || confObject.SplitLogs
	confObject.Overlapping = *overlapping || confObject.Overlapping
	if *sentryDSN != "" {
		confObject.Sentry.Dsn = *sentryDSN
	}

	if *sentryAlias != "" {
		confObject.Sentry.Dsn = *sentryAlias
	}
	if *sentryEnv != "" {
		confObject.Sentry.Environment = *sentryEnv
	}
	if *prefix != "" {
		confObject.Prefix = *prefix
	}

	var sentryDsn string

	if confObject.Sentry.Dsn != "" {
		sentryDsn = confObject.Sentry.Dsn
	}

	if confObject.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if confObject.Json {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&prefixed.TextFormatter{FullTimestamp: true})
	}

	if confObject.SplitLogs {
		hook.RegisterSplitLogger(
			logrus.StandardLogger(),
			os.Stdout,
			os.Stderr,
		)
	}

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
		return
	}
	generalLogger := logrus.WithField("prefix", confObject.Prefix)
	crontabFileName := flag.Args()[0]

	var sentryHook *logrus_sentry.SentryHook
	if sentryDsn != "" {
		sentryLevels := []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
		}
		sh, err := logrus_sentry.NewSentryHook(sentryDsn, sentryLevels)
		if err != nil {
			generalLogger.Fatalf("Could not init sentry logger: %s", err)
		} else {
			if confObject.Sentry.Environment != "" {
				sh.SetEnvironment(confObject.Sentry.Environment)
			}
			sh.Timeout = 5 * time.Second
			sentryHook = sh
		}

		if sentryHook != nil {
			logrus.StandardLogger().AddHook(sentryHook)
		}
	}

	for true {
		generalLogger.Infof("read crontab: %s", crontabFileName)
		tab, err := readCrontabAtPath(crontabFileName)

		if err != nil {
			generalLogger.Fatal(err)
			break
		}

		if *test {
			generalLogger.Info("crontab is valid")
			os.Exit(0)
			break
		}

		var wg sync.WaitGroup
		exitCtx, notifyExit := context.WithCancel(context.Background())

		for _, job := range tab.Jobs {
			cronLogger := generalLogger.WithFields(logrus.Fields{
				"job.schedule": job.Schedule,
				"job.command":  job.Command,
				"job.position": job.Position,
			})

			cron.StartJob(&wg, tab.Context, job, exitCtx, cronLogger, confObject.Overlapping)
		}

		termChan := make(chan os.Signal, 1)
		signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2)

		termSig := <-termChan

		if termSig == syscall.SIGUSR2 {
			generalLogger.Infof("received %s, reloading crontab", termSig)
		} else {
			generalLogger.Infof("received %s, shutting down", termSig)
		}
		notifyExit()

		generalLogger.Info("waiting for jobs to finish")
		wg.Wait()

		if termSig != syscall.SIGUSR2 {
			generalLogger.Info("exiting")
			break
		}
	}
}

func readCrontabAtPath(path string) (*crontab.Crontab, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	return crontab.ParseCrontab(file)
}
