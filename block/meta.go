package block

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hexablock/hexatype"
)

// MetaBlock is a metadata block. It contains an id that points
// to an actual data block i.e. tree, index, data and key-value
// metadata
type MetaBlock struct {
	*baseBlock
	m map[string]string // Metadata
}

func NewMetaBlock(uri *URI, hasher hexatype.Hasher) *MetaBlock {

	return &MetaBlock{
		baseBlock: &baseBlock{
			hasher: hasher,
			uri:    uri,
			typ:    BlockTypeMeta,
			size:   uint64(hasher.Size() + 1), // hash size plus the new line
		},
		m: make(map[string]string),
	}
}

func (blk *MetaBlock) SetMetadata(m map[string]string) {
	for k, v := range m {
		blk.m[k] = v
	}
	blk.Hash()
}

func (blk *MetaBlock) Hash() []byte {
	h := blk.hasher.New()
	h.Write(blk.MarshalBinary())
	sh := h.Sum(nil)

	// Update internal cache
	blk.id = sh[:]
	return blk.id
}

func (blk *MetaBlock) UnmarshalBinary(b []byte) error {
	blk.size = uint64(len(b))
	s := string(b)

	lines := strings.Split(s, "\n")
	l := len(lines)
	if l == 0 {
		return fmt.Errorf("invalid MetaBlock data")
	}

	for _, line := range lines {
		kvp := strings.Split(line, "=")
		if len(kvp) != 2 {
			return fmt.Errorf("invalid metadata: '%s'", line)
		}
		blk.m[kvp[0]] = kvp[1]
	}

	blk.Hash()

	return nil
}

func (blk *MetaBlock) MarshalBinary() []byte {
	keys := blk.sortedKeys()
	lines := make([]string, 0, len(blk.m))
	for _, k := range keys {
		lines = append(lines, fmt.Sprintf("%s=%s", k, blk.m[k]))
	}
	return []byte(strings.Join(lines, "\n"))
}

func (blk *MetaBlock) sortedKeys() []string {
	keys := make([]string, 0, len(blk.m))
	for k := range blk.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
