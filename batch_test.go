package batch

import (
	"fmt"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	b := New().WithTranslator(make(testTranslator))
	now, _ := time.Parse(time.RFC822, time.RFC822)
	now = now.UTC()
	t.Run("first column is timestamp", func(t *testing.T) {
		b.Reset()
		b.Add(12, now, [][]byte{
			[]byte("hello"),
			[]byte("world"),
		})
		f, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		want := `0:0:2006 => c(1191406982922240012)
0:0:200601 => c(1191406982922240012)
0:0:20060102 => c(1191406982922240012)
0:0:2006010215 => c(1191406982922240012)
1:0: => c(1048588, 2097164)
18446744073709551615:0: => c(12)
`
		got := f.String()
		if want != got {
			t.Errorf("expected =>\n%s \ngot=> %v", want, got)
		}
	})
}

type testTranslator map[string]uint64

func (t testTranslator) Translate(column uint64, key []byte) uint64 {
	k := fmt.Sprintf("%d:%x", column, key)
	a, ok := t[k]
	if !ok {
		a = uint64(len(t)) + 1
		t[k] = a
	}
	return a

}
