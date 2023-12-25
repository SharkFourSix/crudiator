package crudiator

// Defines how to query a table. i.e select everything at once or paginate.
//
// Default is NONE
type PaginationStrategy int

const (
	// Do not paginate the query
	NONE PaginationStrategy = iota

	// Use offset pagination where an entire table is scanned in order to get to a specific offset.
	//
	// The larger the offset, the slower the query.
	OFFSET

	// Use keyset pagination where an indexed column is used for comparison.
	//
	// Provides faster pagination better than OFFSET based.
	KEYSET
)

// Passed to the 'Retrieve()' function to be used for pagination
//
// Only applicable if pagination has been configured through 'MustPaginate()' function.
type Pageable interface {
	// Returns the calculated offset. Applicable in 'OFFSET' mode
	Offset() int

	// Returns the page size
	Size() int

	// Returns the indexable value to be used. Applicable in 'KEYSET' mode
	KeysetValue() any
}

type OffsetPaging struct {
	PageOffset int
	PageSize   int
}

func (of OffsetPaging) Offset() int {
	return of.PageOffset
}

func (of OffsetPaging) Size() int {
	return of.PageSize
}

func (of OffsetPaging) KeysetValue() any {
	return nil
}

func NewOffsetPaging(page, size int) Pageable {
	return &OffsetPaging{PageOffset: page * size, PageSize: size}
}

type KeysetPaging struct {
	Value    any
	PageSize int
}

func (kp KeysetPaging) Offset() int {
	return 0
}

func (kp KeysetPaging) Size() int {
	return kp.PageSize
}

func (kp KeysetPaging) KeysetValue() any {
	return kp.Value
}

func NewKeysetPaging(value any, size int) Pageable {
	return &KeysetPaging{Value: value, PageSize: size}
}
