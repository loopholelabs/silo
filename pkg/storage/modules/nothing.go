package modules

/**
 * Nothing storage - eg /dev/null
 *
 */

type Nothing struct {
	size uint64
}

func NewNothing(size uint64) *Nothing {
	return &Nothing{
		size: size,
	}
}

func (i *Nothing) ReadAt(buffer []byte, offset int64) (int, error) {
	return len(buffer), nil
}

func (i *Nothing) WriteAt(buffer []byte, offset int64) (int, error) {
	return len(buffer), nil
}

func (i *Nothing) Flush() error {
	return nil
}

func (i *Nothing) Size() uint64 {
	return i.size
}

func (i *Nothing) Close() error {
	return nil
}

func (i *Nothing) CancelWrites(offset int64, length int64) {
	// TODO: Implement
}
