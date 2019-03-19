package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/montanaflynn/stats"
	"log"
	"net"
	"os"
	"regexp"
	"sort"
	"time"
)

const (
	maxBuf = 1000
)

var cb *CB

type Config struct {
	Addr         string
	Connections  int
	Workers      int
	Count        int
	MetricPerCon int
	BatchSend    int
	ConTimeout   time.Duration
	SendTimeout  time.Duration
	UWorkers     int
	UCount       int
	UBatchSend   int
	SendDelay    time.Duration
	MetricPrefix string
}

type ConStat struct {
	Time  time.Duration
	Error error
	Size  int
}

type Result struct {
	Id      int
	Tcp     bool
	Connect ConStat
	Send    []ConStat
}

func ResultNew(id int, tcp bool, count int) *Result {
	r := new(Result)
	r.Id = id
	r.Tcp = tcp
	r.Send = make([]ConStat, count)
	return r
}

func (r *Result) ResultZero() {
	for i := range r.Send {
		r.Send[i].Size = 0
		r.Send[i].Time = 0
		r.Send[i].Error = nil
	}
}

func (r *Result) Duration() time.Duration {
	if r.Connect.Time <= 0 {
		return 0
	}
	d := r.Connect.Time
	for i := range r.Send {
		if r.Send[i].Time > 0 {
			d += r.Send[i].Time
		}
	}
	return d
}

type summaryResult struct {
	Count  int
	Avg    float64
	Min    float64
	Max    float64
	Median float64
	P75    float64
	P90    float64
	P95    float64
}

func copyFloatSlice(src []float64, n int) []float64 {
	dst := make([]float64, n)
	for i := 0; i < n; i++ {
		dst[i] = src[i]
	}
	return dst
}

func getStat(data []float64) summaryResult {
	var r summaryResult
	sort.Float64s(data)
	r.Count = len(data)
	r.Min, _ = stats.Min(data)
	r.Max, _ = stats.Max(data)
	r.Median, _ = stats.Median(data)
	r.P75, _ = stats.Percentile(data, 0.75)
	r.P90, _ = stats.Percentile(data, 0.9)
	r.P95, _ = stats.Percentile(data, 0.95)
	return r
}

func TcpWorker(id int, config Config, out chan<- Result) {
	r := ResultNew(id, true, config.MetricPerCon)

	defer func(ch chan<- Result) {
		r.ResultZero()
		r.Connect.Time = 0
		ch <- *r
	}(out)

	metricPrefix := fmt.Sprintf("TCP %s.worker%d", config.MetricPrefix, id)
	cb.Await()
	log.Printf("Started TCP worker %d\n", id)

	for i := 0; i < config.Count; i += config.MetricPerCon {
		r.ResultZero()
		start := time.Now()
		con, conError := net.DialTimeout("tcp", config.Addr, config.ConTimeout)
		duration := time.Since(start)
		if conError == nil {
			rw := bufio.NewReadWriter(bufio.NewReader(con), bufio.NewWriter(con))
			r.Connect.Time = duration
			r.Connect.Error = nil
			timeStamp := time.Now().Unix()
			n := i
		LOOP_INT:
			for j := 0; j < config.MetricPerCon && n < config.Count; j++ {
				metricString := fmt.Sprintf("%s.%d %d %d\n", metricPrefix, i, n, timeStamp)
				con.SetDeadline(time.Now().Add(config.SendTimeout))
				start := time.Now()
				sended, err := rw.WriteString(metricString)
				duration := time.Since(start)
				r.Send[j].Size = sended
				if err == nil {
					r.Send[j].Time = duration
				} else {
					r.Send[j].Time = -duration
					r.Send[j].Error = err
					con.Close()
					break LOOP_INT
				}
				n++
				if config.SendDelay > 0 {
					time.Sleep(config.SendDelay)
				}
			}
			con.Close()
		} else {
			r.Connect.Error = conError
			r.Connect.Time = -duration
			if config.SendDelay > 0 {
				time.Sleep(config.SendDelay)
			}
		}
		out <- *r
	}
	log.Printf("Ended TCP worker %d, %d metrics\n", id, config.Count)
}

func UDPWorker(id int, config Config, out chan<- Result) {
	r := ResultNew(id, false, 1)

	defer func(ch chan<- Result) {
		r.ResultZero()
		r.Connect.Time = 0
		ch <- *r
	}(out)

	metricPrefix := fmt.Sprintf("%s.udpworker%d", config.MetricPrefix, id)
	cb.Await()
	log.Printf("Started UDP worker %d\n", id)

	for i := 0; i < config.UCount; i++ {
		r.ResultZero()
		timeStamp := time.Now().Unix()
		metricString := fmt.Sprintf("%s.%d %d %d\n", metricPrefix, i, i, timeStamp)

		start := time.Now()
		con, conError := net.Dial("udp", config.Addr)
		if conError == nil {
			fmt.Fprintf(con, metricString)
			duration := time.Since(start)
			r.Connect.Time = duration
			r.Connect.Error = nil
			r.Send[0].Size = len(metricString)
			con.Close()
		} else {
			r.Connect.Time = -time.Since(start)
			r.Connect.Error = conError
		}
		if config.SendDelay > 0 {
			time.Sleep(config.SendDelay)
		}

		out <- *r
	}
	log.Printf("Ended UDP worker %d, %d metrics\n", id, config.Count)
}

func printStat(s summaryResult, prefix string, duration time.Duration, rateName string) {
	if s.Count == 0 {
		fmt.Printf("%s: %d\n", prefix, s.Count)
	} else {
		var rate string
		if duration > 0 {
			rate = fmt.Sprintf(" (%d %s)", time.Duration(s.Count)*time.Second/duration, rateName)
		}
		fmt.Printf("%s: %d%s, time min %.3f, max %.3f, median %.3f, p75 %.3f, p90 %.3f, p95 %.3f\n",
			prefix, s.Count, rate,
			s.Min, s.Max, s.Median,
			s.P75, s.P90, s.P95)
	}
}

func tcpStat(stat []Result, con int, testDuration time.Duration, duration time.Duration, config Config) {
	var cps time.Duration
	if duration > 0 {
		cps = time.Duration(con) * time.Second / duration
	}
	log.Printf("TCP Workers: %d, duration: %.2f sec, total connections: %d / %.2f sec = %d cps",
		config.Workers, float64(testDuration)/float64(time.Second),
		con, float64(duration)/float64(time.Second), cps)

	conSuccess := make([]float64, con)
	conSendTime := make([]float64, config.Count*config.Workers)
	conSendSize := make([]float64, config.Count*config.Workers)

	conSendErr := 0
	conRefused := 0
	conTimeout := 0
	conResolve := 0
	conError := 0

	ncon := 0
	nsend := 0

	for i := 0; i < con; i++ {
		if !stat[i].Tcp {
			continue
		}
		if stat[i].Connect.Error == nil {
			//log.Printf("ID %d connect %.2f ms", stat[i].Id, float64(stat[i].Connect.Time)/float64(time.Millisecond))
			conSuccess[ncon] = float64(stat[i].Connect.Time) / float64(time.Millisecond)
			ncon++
			var sendTime time.Duration
			var sendSize int
		LOOP_INT:
			for j := range stat[i].Send {
				err := stat[i].Send[j].Error
				if err != nil {
					conSendErr++
					break LOOP_INT
				} else if stat[i].Send[j].Time == 0 {
					break LOOP_INT
				} else {
					//fmt.Printf("Ok: time %d, size %d\n", stat[i].Send[j].Time/time.Microsecond, stat[i].Send[j].Size)
					sendTime += stat[i].Send[j].Time
					sendSize += stat[i].Send[j].Size
				}
			}
			if sendTime > 0 {
				conSendTime[nsend] = float64(sendTime)
				conSendSize[nsend] = float64(sendSize)
				nsend++
			}
		} else {
			//log.Printf("ID %d connect error %.2f ms, %s\n", stat[i].Id, float64(-stat[i].Connect.Time)/float64(time.Millisecond), stat[i].Connect.Error)
			err := stat[i].Connect.Error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				conTimeout++
			} else if match, _ := regexp.MatchString(".*lookup.*", err.Error()); match {
				conResolve++
			} else if match, _ := regexp.MatchString(".*connection refused.*", err.Error()); match {
				conRefused++
			} else {
				conError++
			}
		}
	}
	conSuccess = copyFloatSlice(conSuccess, ncon)
	conResult := getStat(conSuccess)
	printStat(conResult, "connection success", duration, "cps")
	fmt.Printf("connection error: refused %d, timeout %d, lookup %d, other %d\n",
		conRefused, conTimeout, conResolve, conError)

	conSendTime = copyFloatSlice(conSendTime, nsend)
	conSendTimeRes := getStat(conSendTime)
	printStat(conSendTimeRes, "send", duration, "rps")

	conSendSize = copyFloatSlice(conSendSize, nsend)
	conSendSizeRes := getStat(conSendSize)
	printStat(conSendSizeRes, "size", 0, "")
	fmt.Printf("send error: %d\n", conSendErr)
}

func ParseArgs() (Config, error) {
	var (
		config      Config
		conTimeout  int
		sendTimeout int
		sendDelay   int
		host        string
		port        int
	)

	flag.StringVar(&host, "host", "127.0.0.1", "hostname")
	flag.IntVar(&port, "port", 2003, "port")
	flag.IntVar(&config.Workers, "workers", 10, "TCP workers")
	flag.IntVar(&config.Count, "count", 1000, "total sended metrics per TCP worker")
	flag.IntVar(&config.MetricPerCon, "metric", 1, "send metric count in one TCP connection")
	//flag.IntVar(&config.BatchSend, "batch", 1, "send metric count in one TCP send")
	flag.IntVar(&conTimeout, "t", 10, "TCP connect timeout (ms)")
	flag.IntVar(&sendTimeout, "s", 100, "TCP send timeout (ms)")
	flag.IntVar(&config.UWorkers, "uworkers", 0, "UDP workers (default 0)")
	flag.IntVar(&config.UCount, "ucount", 1000, "total sended metrics per UDP worker")
	//flag.IntVar(&config.UBatchSend, "ubatch", 1, "send metric count in one UDP send")
	flag.StringVar(&config.MetricPrefix, "prefix", "test", "metric prefix")

	flag.IntVar(&sendDelay, "delay", 0, "send delay (ms)")

	flag.Parse()
	if host == "" {
		host = "127.0.0.1"
	}
	if port < 1 {
		return config, errors.New(fmt.Sprintf("Invalid port value: %d\n", port))
	}
	if config.Workers < 1 {
		return config, errors.New(fmt.Sprintf("Invalid TCP workers value: %d\n", config.Workers))
	}
	if config.Count < 1 {
		return config, errors.New(fmt.Sprintf("Invalid TCP count value: %d\n", config.Count))
	}
	if config.MetricPerCon < 1 {
		return config, errors.New(fmt.Sprintf("Invalid TCP metric value: %d\n", config.MetricPerCon))
	}
	/*
		if config.BatchSend < 1 {
			return config, errors.New(fmt.Sprintf("Invalid TCP metric batchsend value: %d\n", config.BatchSend))
		}
	*/
	if sendTimeout < 1 {
		return config, errors.New(fmt.Sprintf("Invalid TCP send timeout value: %d\n", sendTimeout))
	}
	if conTimeout < 1 {
		return config, errors.New(fmt.Sprintf("Invalid TCP connection timeout value: %d\n", conTimeout))
	}
	if config.UWorkers < 0 {
		return config, errors.New(fmt.Sprintf("Invalid UDP workers value: %d\n", config.Workers))
	}
	if config.UCount < 1 {
		return config, errors.New(fmt.Sprintf("Invalid UDP count value: %d\n", config.Count))
	}
	if sendDelay < 0 {
		return config, errors.New(fmt.Sprintf("Invalid delay value: %d\n", sendDelay))
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
	return config, nil
}

func main() {
	config, error := ParseArgs()
	if error != nil {
		fmt.Print(error)
		os.Exit(1)
	}

	statSize := config.Count*config.Workers/config.MetricPerCon +
		config.Count*config.Workers%config.MetricPerCon +
		config.UCount*config.UWorkers
	bufSize := maxBuf
	if statSize < maxBuf {
		bufSize = statSize
	}

	result := make(chan Result, bufSize)
	workers := config.Workers
	uworkers := config.UWorkers
	stat := make([]Result, statSize)

	log.Printf("Starting workers: %d\n", config.Workers)

	cb = NewCB(config.Workers + config.UWorkers + 1)

	for i := 0; i < config.Workers; i++ {
		go TcpWorker(i, config, result)
	}
	for i := 0; i < config.UWorkers; i++ {
		go UDPWorker(i, config, result)
	}

	start := time.Now()
	cb.Await()

	con := 0
	ucon := 0
	var (
		testDuration  time.Duration
		duration      time.Duration
		testUduration time.Duration
		uduration     time.Duration
	)

LOOP:
	for {
		select {
		case r := <-result:
			if r.Connect.Time == 0 {
				if r.Tcp {
					workers--
					if workers == 0 {
						testDuration = time.Since(start)
					}
				} else {
					uworkers--
					if uworkers == 0 {
						testUduration = time.Since(start)
					}
				}
				if workers <= 0 && uworkers <= 0 {
					break LOOP
				}
			} else {
				stat[con+ucon] = r
				if r.Tcp {
					con++
				} else {
					ucon++
				}
			}
		}
	}

	for i := range stat {
		if stat[i].Tcp {
			if stat[i].Connect.Time > 0 {
				duration += stat[i].Duration()
			} else {
				duration -= stat[i].Connect.Time
			}
		} else {
			if stat[i].Connect.Time > 0 {
				uduration += stat[i].Duration()
			} else {
				uduration -= stat[i].Connect.Time
			}
		}
	}

	tcpStat(stat, con, testDuration, duration, config)

	if uduration > 0 {
		log.Printf("UDP Workers: %d, duration %.2f sec, total send: %d / %.2f sec = %d rps",
			config.UWorkers, float64(testUduration)/float64(time.Second),
			ucon, float64(uduration)/float64(time.Second),
			time.Duration(ucon)*time.Second/uduration)
	}
}
