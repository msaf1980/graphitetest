package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"runtime/pprof"

	"github.com/msaf1980/cyclicbarrier"
)

const (
	maxBuf = 1000
)

var cb *cyclicbarrier.CyclicBarrier

var running = true

type config struct {
	Addr string
	//Connections int
	Workers      int // TCP Workers
	Duration     time.Duration
	MetricPerCon int
	BatchSend    int
	RateLimit    []int32
	SendDelay    time.Duration
	ConTimeout   time.Duration
	SendTimeout  time.Duration
	UWorkers     int // UDP Workers
	UBatchSend   int

	MetricPrefix string // Prefix for generated metric name
	Verbose      bool

	StatFile string // write connections stat to file
	CPUProf  string // write cpu profile info to file
}

const header = "timestamp\tConId\tProto\tType\tStatus\tElapsed\tSize\n"

func ParseArgs() (config, error) {
	var (
		config      config
		conTimeout  int
		sendTimeout int
		sendDelay   int
		host        string
		port        int
		duration    string
		err         error
		rateLimit   string
	)

	flag.StringVar(&host, "host", "127.0.0.1", "hostname")
	flag.IntVar(&port, "port", 2003, "port")
	flag.IntVar(&config.Workers, "workers", 10, "TCP workers")
	flag.StringVar(&duration, "duration", "60s", "total test duration")
	flag.IntVar(&config.MetricPerCon, "metric", 1, "send metric count in one TCP connection")
	//flag.IntVar(&config.BatchSend, "batch", 1, "send metric count in one TCP send")
	flag.IntVar(&conTimeout, "t", 100, "TCP connect timeout (ms)")
	flag.IntVar(&sendTimeout, "s", 500, "TCP send timeout (ms)")
	flag.IntVar(&config.UWorkers, "uworkers", 0, "UDP workers (default 0)")
	//flag.IntVar(&config.UBatchSend, "ubatch", 1, "send metric count in one UDP send")
	flag.StringVar(&config.MetricPrefix, "prefix", "test", "metric prefix")

	flag.StringVar(&rateLimit, "rate", "", "rate limit, format: rate or minRate:maxRate:increment ")
	flag.IntVar(&sendDelay, "delay", 0, "send delay (ms)")

	flag.BoolVar(&config.Verbose, "verbose", false, "verbose")

	flag.StringVar(&config.StatFile, "stat", "grelaytest.csv", "stat file (appended)")

	flag.StringVar(&config.CPUProf, "cpuprofile", "", "write cpu profile to file")

	flag.Parse()
	if host == "" {
		host = "127.0.0.1"
	}
	if port < 1 {
		return config, fmt.Errorf("Invalid port value: %d", port)
	}
	if config.Workers < 1 {
		return config, fmt.Errorf("Invalid TCP workers value: %d", config.Workers)
	}
	if config.MetricPerCon < 1 {
		return config, fmt.Errorf("Invalid TCP metric value: %d", config.MetricPerCon)
	}
	/*
		if config.BatchSend < 1 {
			return config, errors.New(fmt.Sprintf("Invalid TCP metric batchsend value: %d\n", config.BatchSend))
		}
	*/
	if sendTimeout < 1 {
		return config, fmt.Errorf("Invalid TCP send timeout value: %d", sendTimeout)
	}
	if conTimeout < 1 {
		return config, fmt.Errorf("Invalid TCP connection timeout value: %d", conTimeout)
	}
	if config.UWorkers < 0 {
		return config, fmt.Errorf("Invalid UDP workers value: %d", config.Workers)
	}
	config.Duration, err = time.ParseDuration(duration)
	if err != nil || config.Duration < 1000000000 {
		return config, fmt.Errorf("Invalid test duration: %s", duration)
	}
	if sendDelay < 0 {
		return config, fmt.Errorf("Invalid delay value: %d", sendDelay)
	}

	/*
		if config.UBatchSend < 1 {
			return config, errors.New(fmt.Sprintf("Invalid UDP metric batchsend value: %d\n", config.UBatchSend))
		}
	*/
	config.Addr = fmt.Sprintf("%s:%d", host, port)
	config.SendTimeout = time.Duration(sendTimeout) * time.Millisecond
	config.ConTimeout = time.Duration(conTimeout) * time.Millisecond
	config.SendDelay = time.Duration(sendDelay) * time.Millisecond

	if rateLimit != "" {
		rateS := strings.Split(rateLimit, ":")
		if len(rateS) == 1 {
			config.RateLimit = make([]int32, 1)
			i, err := strconv.ParseInt(rateS[0], 10, 32)
			if err != nil {
				return config, fmt.Errorf("Invalid rate format: %s is not a number", rateS[0])
			}
			config.RateLimit[0] = int32(i)
		} else if len(rateS) == 3 {
			minRate, err := strconv.ParseInt(rateS[0], 10, 32)
			maxRate, err := strconv.ParseInt(rateS[0], 10, 32)

		} else {
			return config, fmt.Errorf("Invalid rate format: %s", rateLimit)
		}

	}

	return config, nil
}

func main() {
	config, err := ParseArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if _, err := os.Stat(config.StatFile); err == nil || !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "file %s already exist\n", config.StatFile)
		os.Exit(1)
	}

	file, err := os.OpenFile(config.StatFile, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	_, err = w.WriteString(header)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}

	if config.CPUProf != "" {
		f, err := os.Create(config.CPUProf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	result := make(chan ConStat, (config.Workers+config.UWorkers)*1000)
	workers := config.Workers
	uworkers := config.UWorkers

	cb = cyclicbarrier.New(config.Workers + config.UWorkers + 1)

	for i := 0; i < config.Workers; i++ {
		go TcpWorker(i, config, result)
	}
	for i := 0; i < config.UWorkers; i++ {
		go UDPWorker(i, config, result)
	}

	//start := time.Now()
	go func() {
		time.Sleep(config.Duration)
		log.Printf("Shutting down")
		running = false
	}()

	var stat = map[Proto]map[NetOper]map[NetErr]int64{}

	start := time.Now()

	log.Printf("Starting TCP workers: %d, UDP %d\n", config.Workers, config.UWorkers)
	cb.Await()

LOOP:
	for {
		select {
		case r := <-result:
			if r.TimeStamp == 0 {
				if r.Proto == TCP {
					workers--
					//if workers == 0 {
					//duration = time.Since(start)
					//}
				} else {
					uworkers--
					//if uworkers == 0 {
					//uduration = time.Since(start)
					//}
				}
				if workers <= 0 && uworkers <= 0 {
					break LOOP
				}
			} else {
				fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%d\t%d\n", r.TimeStamp/1000, r.Id,
					ProtoToString(r.Proto), NetOperToString(r.Type),
					NetErrToString(r.Error), r.Elapsed/1000, r.Size)

				sProto, ok := stat[r.Proto]
				if !ok {
					sProto = map[NetOper]map[NetErr]int64{}
					stat[r.Proto] = sProto
				}
				sOper, ok := sProto[r.Type]
				if !ok {
					sOper = map[NetErr]int64{}
					sProto[r.Type] = sOper
				}
				_, ok = sOper[r.Error]
				if !ok {
					sOper[r.Error] = 1
				} else {
					sOper[r.Error]++
				}
			}
		}
	}
	duration := time.Since(start)
	err = w.Flush()
	if err != nil {
		panic(err)
	}
	log.Printf("Shutdown, results writed to %s. Test duration %s", config.StatFile, duration)

	// Print stat
	for proto, opers := range stat {
		for oper, errors := range opers {
			for error, s := range errors {
				fmt.Printf("%s.%s.%s %d (%d/s)\n", ProtoToString(proto), NetOperToString(oper), NetErrToString(error),
					s, s/(duration.Nanoseconds()/1000000000))
			}
		}
	}

	//tcpStat(stat, con, duration, config)

	//if duduration > 0 {
	//log.Printf("UDP Workers: %d, total send: %d / %.2f sec = %d rps",
	//config.UWorkers, ucon, float64(uduration)/float64(time.Second),
	//time.Duration(ucon)*time.Second/uduration)
	//}
}
