package timewheel

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestTimeWheel_Add
func TestTimeWheel_Add(t *testing.T) {
	ta := []struct {
		runFirst  bool
		every     bool
		val       int64
		expect    int64
		timeInter time.Duration
		ticker    time.Duration
		after     time.Duration
		bucket    int64
	}{
		{
			every:     true,
			expect:    2,
			timeInter: time.Millisecond * 200,
			after:     time.Millisecond * 200,
			ticker:    time.Millisecond * 400,
			bucket:    10,
		},
		{
			runFirst:  true,
			every:     true,
			expect:    3,
			timeInter: time.Millisecond * 200,
			after:     time.Millisecond * 200,
			ticker:    time.Millisecond * 400,
			bucket:    10,
		},
		{
			expect:    1,
			timeInter: time.Millisecond * 200,
			after:     time.Millisecond * 200,
			ticker:    time.Millisecond * 400,
			bucket:    10,
		},
		{
			runFirst:  true,
			expect:    1,
			timeInter: time.Millisecond * 200,
			after:     time.Millisecond * 200,
			ticker:    time.Millisecond * 400,
			bucket:    10,
		},
		{
			every:     true,
			expect:    1,
			timeInter: time.Millisecond * 200,
			after:     time.Millisecond * 400,
			ticker:    time.Millisecond * 400,
			bucket:    2,
		},
		{
			every:     true,
			runFirst:  true,
			expect:    2,
			timeInter: time.Millisecond * 200,
			after:     time.Millisecond * 400,
			ticker:    time.Millisecond * 400,
			bucket:    2,
		},
	}

	for i, twd := range ta {
		tw := NewTimeWheel(twd.timeInter, twd.bucket)
		tk := time.NewTicker(twd.ticker)

		tmp := twd
		tw.Add(fmt.Sprintf("%d", i), func() {
			atomic.AddInt64(&tmp.val, 1)
		}, twd.after, WithEvery(twd.every), WithRunFirst(twd.runFirst))

		<-tk.C
		tw.Stop()
		if tmp.val != twd.expect {
			t.Errorf("expect %v, but val %v", twd.expect, tmp.val)
		}
	}
}

func TestTimeWheel_Remove(t *testing.T) {
	tw := NewTimeWheel(time.Millisecond*100, 10)
	var val int64
	tw.Add("test", func() {
		atomic.AddInt64(&val, 1)
	}, time.Millisecond*100, WithEvery(true), WithRunFirst(true))
	tw.Remove("test")
	tk := time.NewTicker(time.Second)
	<-tk.C
	if val != 1 {
		t.Errorf("remove expect 1, but %v", val)
	}
}
