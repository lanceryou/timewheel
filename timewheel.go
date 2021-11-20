package timewheel

import (
	"sync"
	"time"
)

// AddOptions Add function options
type AddOptions struct {
	runFirst bool
	every    bool
}

// AddOption add option
type AddOption func(*AddOptions)

// WithRunFirst will run quickly, and add to the slot
func WithRunFirst(r bool) AddOption {
	return func(options *AddOptions) {
		options.runFirst = r
	}
}

// WithEvery will add to the slot after run
func WithEvery(e bool) AddOption {
	return func(options *AddOptions) {
		options.every = e
	}
}

type rounds struct {
	roundMap map[string]round
}

func (r *rounds) add(rd round) {
	r.roundMap[rd.key] = rd
}

func (r *rounds) remove(key string) {
	delete(r.roundMap, key)
}

// get run tasks
func (r *rounds) tasks() (ts []round) {
	for _, v := range r.roundMap {
		v.circle--
		if v.circle <= 0 {
			ts = append(ts, v)
		}
	}
	return
}

type round struct {
	after    time.Duration
	runFirst bool
	every    bool
	circle   int64
	key      string
	fn       func()
}

// TimeWheel
type TimeWheel struct {
	wg       sync.WaitGroup
	tk       *time.Ticker
	interval time.Duration
	quit     chan struct{}
	stop     chan struct{}

	buckets int64
	pos     int64
	slots   []*rounds

	keyPosMap map[string]round
	ac        chan round
	rc        chan string
}

// NewTimeWheel new timewheel
func NewTimeWheel(interval time.Duration, buckets int64) *TimeWheel {
	tw := &TimeWheel{
		interval:  interval,
		buckets:   buckets,
		quit:      make(chan struct{}),
		stop:      make(chan struct{}),
		keyPosMap: make(map[string]round),
		tk:        time.NewTicker(interval),
		slots:     make([]*rounds, buckets),
		ac:        make(chan round),
		rc:        make(chan string),
	}

	for i := range tw.slots {
		tw.slots[i] = &rounds{roundMap: make(map[string]round)}
	}

	go tw.run()
	return tw
}

// Add add fn with key and run fn after timeout
func (w *TimeWheel) Add(key string, fn func(), after time.Duration, opts ...AddOption) {
	var opt AddOptions
	for _, o := range opts {
		o(&opt)
	}
	w.ac <- round{
		after:    after,
		key:      key,
		fn:       fn,
		every:    opt.every,
		runFirst: opt.runFirst,
	}
}

// Remove remove key
func (w *TimeWheel) Remove(key string) {
	w.rc <- key
}

// add
func (w *TimeWheel) add(r round) {
	idx := int64(r.after / w.interval)
	r.circle = idx / w.buckets
	pos := (w.pos + idx) % w.buckets

	// remove older key
	w.remove(r.key)
	w.slots[pos].add(r)
	w.keyPosMap[r.key] = r
}

// remove
func (w *TimeWheel) remove(key string) {
	r, ok := w.keyPosMap[key]
	if !ok {
		return
	}
	idx := int64(r.after / w.interval)
	pos := idx % w.buckets
	w.slots[pos].remove(key)
	delete(w.keyPosMap, key)
}

// Stop stop run all added fn, if fn running, wait stop
func (w *TimeWheel) Stop() {
	close(w.quit)
	<-w.stop
}

func (w *TimeWheel) run() {
	for {
		select {
		case <-w.tk.C:
			w.onTicker()
		case ar := <-w.ac:
			if ar.runFirst {
				w.runTask(ar)
			} else {
				w.add(ar)
			}
		case key := <-w.rc:
			w.remove(key)
		case <-w.quit:
			w.tk.Stop()
			w.wg.Wait()
			close(w.stop)
			return
		}
	}
}

func (w *TimeWheel) onTicker() {
	w.pos = (w.pos + 1) % w.buckets
	tasks := w.slots[w.pos].tasks()
	for _, task := range tasks {
		w.runTask(task)
	}
}

func (w *TimeWheel) runTask(r round) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		r.fn()
	}()
	if r.every {
		w.add(r)
	} else {
		w.remove(r.key)
	}
}
