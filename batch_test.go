package batch

import (
	"fmt"
	"os"
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
		got := f.String()

		// os.WriteFile("testdata/batch.txt", []byte(got), 0600)
		w, err := os.ReadFile("testdata/batch.txt")
		if err != nil {
			t.Fatal(err)
		}
		want := string(w)
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
