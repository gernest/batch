package batch

import (
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

const (
	Exists = nilSentinel
)

const nilSentinel = ^uint64(0)

type Row struct {
	ID      uint64
	Columns []uint64
	Values  [][]byte
}

type Translator interface {
	Translate(column uint64, value []byte) uint64
}

type Batch struct {
	ids       []uint64
	rowIDs    map[int][]uint64
	columns   []uint64
	translate Translator
	frags     Fragments
	rowIDSets map[uint64][][]uint64
}

func New() *Batch {
	return &Batch{
		rowIDs:    make(map[int][]uint64),
		frags:     make(Fragments),
		rowIDSets: make(map[uint64][][]uint64),
	}
}

func (b *Batch) Reset() {
	b.frags = make(Fragments)
	b.ids = b.ids[:0]
	clear(b.rowIDs)
	clear(b.rowIDSets)
	b.columns = b.columns[:0]
	b.translate = nil
}

func (b *Batch) WithTranslator(t Translator) *Batch {
	b.translate = t
	return b
}

func (b *Batch) WithColumns(cols []uint64) *Batch {
	b.columns = append(b.columns[:0], cols...)
	return b
}

func (b *Batch) Add(record *Row) {
	b.ids = append(b.ids, record.ID)
	curPos := len(b.ids) - 1
	for i := range record.Values {
		for len(b.rowIDs[i]) < curPos {
			b.rowIDs[i] = append(b.rowIDs[i], nilSentinel)
		}
		rowIDs := b.rowIDs[i]
		rowID := b.translate.Translate(record.Columns[i], record.Values[i])
		b.rowIDs[i] = append(rowIDs, rowID)
	}
}

func (b *Batch) Build() Fragments {
	o := b.makeFragments()
	b.frags = make(Fragments)
	return o
}

func (b *Batch) makeFragments() Fragments {
	shardWidth := b.shardWidth()

	// create _exists fragments
	var curBM *roaring.Bitmap
	curShard := ^uint64(0) // impossible sentinel value for shard.
	for _, col := range b.ids {
		if col/shardWidth != curShard {
			curShard = col / shardWidth
			curBM = b.frags.GetOrCreate(curShard, Exists, "")
		}
		curBM.DirectAdd(col % shardWidth)
	}

	for i, rowIDs := range b.rowIDs {
		if len(rowIDs) == 0 {
			continue // this can happen when the values that came in for this field were string slices
		}
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		for j := range b.ids {
			col := b.ids[j]
			row := nilSentinel
			if len(rowIDs) > j {
				// this is to protect against what i believe is a bug in the idk.DeleteSentinel logic in handling nil entries
				// this will prevent a crash by assuming missing entries are nil entries which i think is ok
				// TODO (twg) find where the nil entry was not added on the idk side ~ingest.go batchFromSchema method
				row = rowIDs[j]
			}

			if col/shardWidth != curShard {
				curShard = col / shardWidth
				// the API treats "" as standard
				curBM = b.frags.GetOrCreate(curShard, b.columns[i], "")
			}
			if row != nilSentinel {
				curBM.DirectAdd(row*shardWidth + (col % shardWidth))
			}
		}
	}

	for field, rowIDSets := range b.rowIDSets {
		if len(rowIDSets) == 0 {
			continue
		} else if len(rowIDSets) < len(b.ids) {
			// rowIDSets is guaranteed to have capacity == to b.ids,
			// but if the last record had a nil for this field, it
			// might not have the same length, so we re-slice it to
			// ensure the lengths are the same.
			rowIDSets = rowIDSets[:len(b.ids)]
		}
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		for j := range b.ids {
			col, rowIDs := b.ids[j], rowIDSets[j]
			if col/shardWidth != curShard {
				curShard = col / shardWidth
				curBM = b.frags.GetOrCreate(curShard, field, "")
			}
			if len(rowIDs) == 0 {
				continue
			}
			for _, row := range rowIDs {
				curBM.DirectAdd(row*shardWidth + (col % shardWidth))
			}
		}
	}
	return b.frags
}

func (b *Batch) shardWidth() uint64 {
	return shardwidth.ShardWidth
}

type Fragments map[FragmentKey]map[string]*roaring.Bitmap

type FragmentKey struct {
	Shard uint64
	Field uint64
}

func (f Fragments) GetOrCreate(shard uint64, field uint64, view string) *roaring.Bitmap {
	key := FragmentKey{shard, field}
	viewMap, ok := f[key]
	if !ok {
		viewMap = make(map[string]*roaring.Bitmap)
		f[key] = viewMap
	}
	bm, ok := viewMap[view]
	if !ok {
		bm = roaring.NewBTreeBitmap()
		viewMap[view] = bm
	}
	return bm
}

func (f Fragments) GetViewMap(shard uint64, field uint64) map[string]*roaring.Bitmap {
	key := FragmentKey{shard, field}
	viewMap, ok := f[key]
	if !ok {
		return nil
	}
	// Remove any views which have an empty bitmap.
	// TODO: Ideally we would prevent allocating the empty bitmap to begin with,
	// but the logic is a bit tricky, and since we don't want to spend too much
	// time on it right now, we're leaving that for a future exercise.
	for k, v := range viewMap {
		if v.Count() == 0 {
			delete(viewMap, k)
		}
	}
	return viewMap
}

func (f Fragments) DeleteView(shard uint64, field uint64, view string) {
	vm := f.GetViewMap(shard, field)
	if vm == nil {
		return
	}
	delete(vm, view)
}
