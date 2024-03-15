package modules

/**
 * Nothing storage - eg /dev/null
 *
 */

type Nothing struct {
}

func NewNothing() *Nothing {
	return &Nothing{}
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
	return 0
}

func (i *Nothing) Close() error {
	return nil
}
