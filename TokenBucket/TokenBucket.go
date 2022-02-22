package TokenBucket

const (
	DEFAULT_MAX_REQUEST = 20
	DEFAULT_INTERVAL = 10
)

var MAX_REQ int
var INTERVAL int64

func SetDefaultParam() {
	MAX_REQ = DEFAULT_MAX_REQUEST
	INTERVAL = DEFAULT_INTERVAL
}

type TokenBucket struct {
	MaxReq int
	Interval int64
	AvailableReq int
	LastReset int64
}

func NewTokenBucket() *TokenBucket {
	t := new(TokenBucket)
	t.MaxReq = MAX_REQ
	t.Interval = INTERVAL
	return t
}

