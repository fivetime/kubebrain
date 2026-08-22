package contextsort

import (
	"context"
	"errors"
)

// Slice sorts items while checking ctx throughout the merge and copy-back
// phases. Equal elements retain their input order.
func Slice[T any](ctx context.Context, items []T, less func(T, T) bool) error {
	if ctx == nil || less == nil {
		return errors.New("sort context and comparison are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(items) < 2 {
		return nil
	}
	buffer := make([]T, len(items))
	source, destination := items, buffer
	inBuffer := false
	for width := 1; width < len(items); {
		for start := 0; start < len(items); start += 2 * width {
			if err := ctx.Err(); err != nil {
				return err
			}
			middle := min(start+width, len(items))
			end := min(start+2*width, len(items))
			left, right, output := start, middle, start
			for left < middle || right < end {
				if err := ctx.Err(); err != nil {
					return err
				}
				if right >= end || (left < middle && !less(source[right], source[left])) {
					destination[output] = source[left]
					left++
				} else {
					destination[output] = source[right]
					right++
				}
				output++
			}
		}
		source, destination = destination, source
		inBuffer = !inBuffer
		if width > len(items)/2 {
			break
		}
		width *= 2
	}
	if inBuffer {
		for i := range items {
			if err := ctx.Err(); err != nil {
				return err
			}
			items[i] = source[i]
		}
	}
	return nil
}
