package store

import (
	"strings"
	"time"
)

// ListOptions selects a page of keys.
type ListOptions struct {
	Prefix    string
	Delimiter string
	// After is an exclusive lower bound: the marker, start-after value or
	// the last key or common prefix of the previous page.
	After   string
	MaxKeys int
}

// ObjectSummary is one key in a listing.
type ObjectSummary struct {
	Key     string
	Size    int64
	ETag    string
	ModTime time.Time
}

// ListResult is one page of a listing. Keys and common prefixes together
// number at most MaxKeys.
type ListResult struct {
	Objects        []ObjectSummary
	CommonPrefixes []string
	IsTruncated    bool
	// NextMarker is the last key or common prefix returned, set when the
	// listing is truncated. Passing it as After continues the listing.
	NextMarker string

	visited int // index entries examined, for tests
}

// successor returns the smallest string greater than every string that
// starts with p, or false if there is none (p is all 0xff bytes).
func successor(p string) (string, bool) {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// commonPrefix returns the common prefix key rolls up into, if any.
func commonPrefix(key, prefix, delim string) (string, bool) {
	if delim == "" {
		return "", false
	}
	i := strings.Index(key[len(prefix):], delim)
	if i < 0 {
		return "", false
	}
	return key[:len(prefix)+i+len(delim)], true
}

// ListObjects lists keys in UTF-8 binary order. With a delimiter, keys that
// contain it after the prefix are rolled up into common prefixes, and the
// scan jumps past each common prefix instead of visiting its keys, so a
// page costs O(page size × log n) however many keys the prefixes hide.
func (s *Store) ListObjects(bucketName string, opt ListOptions) (ListResult, error) {
	b, err := s.getBucket(bucketName)
	if err != nil {
		return ListResult{}, err
	}
	var res ListResult
	if opt.MaxKeys <= 0 {
		return res, nil
	}
	start := opt.Prefix
	if opt.After != "" && opt.After >= start {
		if !strings.HasPrefix(opt.After, opt.Prefix) && opt.After > opt.Prefix {
			// Past every key with the prefix.
			return res, nil
		}
		if cp, ok := commonPrefix(opt.After, opt.Prefix, opt.Delimiter); ok && strings.HasPrefix(opt.After, opt.Prefix) {
			// Resuming after a common prefix (or a key inside one, which
			// the previous page reported as that prefix).
			next, ok := successor(cp)
			if !ok {
				return res, nil
			}
			start = next
		} else {
			start = opt.After + "\x00"
		}
	}
	count := 0
	done := false
	for !done {
		restart := ""
		done = true
		b.index.ascend(start, func(e *entry) bool {
			res.visited++
			if !strings.HasPrefix(e.key, opt.Prefix) {
				return false // keys are sorted: nothing further matches
			}
			if count == opt.MaxKeys {
				res.IsTruncated = true
				return false
			}
			if cp, ok := commonPrefix(e.key, opt.Prefix, opt.Delimiter); ok {
				res.CommonPrefixes = append(res.CommonPrefixes, cp)
				res.NextMarker = cp
				count++
				next, ok := successor(cp)
				if ok {
					restart, done = next, false
				}
				return false
			}
			res.Objects = append(res.Objects, ObjectSummary{
				Key: e.key, Size: e.size, ETag: e.etag, ModTime: time.Unix(0, e.modTime).UTC(),
			})
			res.NextMarker = e.key
			count++
			return true
		})
		start = restart
	}
	if !res.IsTruncated {
		res.NextMarker = ""
	}
	return res, nil
}
