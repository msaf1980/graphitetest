package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

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

type Worker struct {
	out      chan string   // A channel to communicate to the routine
	Interval time.Duration // The interval with which to run the Action
	period   time.Duration // The actual period of the wait
}

func TcpWorker(id int, config Config, out chan<- Result) {
	r := ResultNew(id, true, config.MetricPerCon)

	metricPrefix := fmt.Sprintf("%s.worker%d", config.MetricPrefix, id)
	cb.Await()
	if config.Verbose {
		log.Printf("Started TCP worker %d\n", id)
	}

	for running {
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
	if config.Verbose {
		log.Printf("Ended TCP worker %d, %d metrics\n", id, config.Count)
	}
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
	if config.Verbose {
		log.Printf("Ended UDP worker %d, %d metrics\n", id, config.Count)
	}
}
