package daog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	txrequest "github.com/soulgm/daog/tx"
	"github.com/soulgm/daog/utils"
)

type tcStatus int

const (
	TraceID         = "trace-id"
	goroutineID     = "Goroutine-Id"
	ctxValues       = "Ctx-Values"
	tcStatusInit    = tcStatus(1)
	tcStatusInvalid = tcStatus(4)
)

var invalidTcStatus = errors.New("invalid tc status")
var metRecover = errors.New("met recover")

/*
NewTransContext 创建一个单库单表的事务执行上下文
txRequest 指明了事务级别，事务级别参照 txrequest.RequestStyle
traceId 可以是nil，它代表一次业务请求，建议设置一个合理的值，它可以标记在执行的sql上，可以有效帮助排查问题
*/
func NewTransContext(datasource Datasource, txRequest txrequest.RequestStyle, traceId string) (*TransContext, error) {
	var conn *sql.Conn
	var err error
	gid := utils.QuickGetGoroutineId()
	ctx := buildContext(gid, traceId)

	connCtx, cancelFunc := context.WithTimeout(context.Background(), datasource.acquireConnTimeout())
	defer cancelFunc()

	if conn, err = datasource.getDB(ctx).Conn(connCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			GLogger.Info(ctx, "get connection timeout")
			return nil, errors.New("get connection timeout")
		} else {
			GLogger.Error(ctx, err)
		}
		return nil, err
	}

	tc := &TransContext{
		txRequest: txRequest,
		status:    tcStatusInit,
		ctx:       ctx,
		conn:      conn,
		LogSql:    datasource.IsLogSQL(),
	}
	err = tc.begin()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return tc, nil
}

// TcCreatorFunc 创建事务上下文的回调函数
type TcCreatorFunc func() (*TransContext, error)

// WorkFn 定义不返回业务内容的函数
type WorkFn func(tc *TransContext) error

// WorkFnWithT 定义返回业务内容的函数
type WorkFnWithT[T any] func(tc *TransContext) (T, error)

/*
AutoTrans 自动在事务内完成业务逻辑的包装函数，不返回业务返回值， 通过 tCreatorFunc 自动构建事务上下文， 然后执行 workFn 业务逻辑
如果不使用 AutoTrans 或者 AutoTransWithResult 你需要自行写一个defer 匿名函数用于最终提交或回滚事务，并且需要提前定义err变量，在业务执行过程中每个操作返回的err都需要赋值给err,而且每一步都需要判断err。如下：
var err error
tc,err := NewTransContext(...)

	if err != nil {
		 return err
	}

	defer func() {
		tc.CompleteWithPanic(err, recover())
	}

err = run1(tc, ...)

	if err != nil {
		 return err
	}

err = run1(tc, ...)

	if err != nil {
		 return err
	}

return nil
*/
func AutoTrans(tCreatorFunc TcCreatorFunc, workFn WorkFn) error {
	tc, err := tCreatorFunc()
	if err != nil {
		return err
	}
	return wrapTrans(tc, workFn)
}

// AutoTransWithResult 自动在事务内完成业务逻辑的包装函数，需要业务返回值， 通过 tCreatorFunc 自动构建事务上下文， 然后执行 workFn 业务逻辑
func AutoTransWithResult[T any](tCreatorFunc TcCreatorFunc, workFnT WorkFnWithT[T]) (T, error) {
	tc, err := tCreatorFunc()
	var v T
	if err != nil {
		return v, err
	}
	return wrapTransWithResult(tc, workFnT)
}

func wrapTrans(tc *TransContext, workFn WorkFn) error {
	var err error
	defer func() {
		tc.CompleteWithPanic(err, recover())
	}()
	err = workFn(tc)
	return err
}

// wrapTransWithResult 与WrapTrans类似，不同的是业务处理函数可以有返回值
func wrapTransWithResult[T any](tc *TransContext, workFnT WorkFnWithT[T]) (T, error) {
	var err error
	defer func() {
		tc.CompleteWithPanic(err, recover())
	}()
	ret, err := workFnT(tc)
	return ret, err
}

/*
TransContext 事务的上下文，描述了数据事务，所有在该事务内执行的数据库操作都需要被提交或者回滚，保持原子性。在daog里要想执行数据库操作必须要确定TransContext，
他是数据操作的起点，一旦一个事务确定，对应的数据库连接确定，底层物理事务确定，同时它内部维护一个状态，用于记录事务的创建、提交/回滚, TransContext最终需要被调用
CompleteWithPanic 来进入终态，进入终态后，其生命周期即完成
*/
type TransContext struct {
	txRequest txrequest.RequestStyle
	tx        driver.Tx
	conn      *sql.Conn
	status    tcStatus
	ctx       context.Context
	LogSql    bool
	ExtInfo   map[string]any
}

func (tc *TransContext) begin() (err error) {
	if tc.txRequest == txrequest.RequestNone {
		return
	}
	tc.tx, err = tc.conn.BeginTx(context.Background(), &sql.TxOptions{
		ReadOnly: tc.txRequest == txrequest.RequestReadonly,
	})
	if err != nil {
		return
	}
	// todo [what is TransBegunInterceptor]
	return
}

func (tc *TransContext) check() error {
	if tc.status != tcStatusInit {
		return invalidTcStatus
	}
	return nil
}

/*
CompleteWithPanic 事务最终完成，可能是提交，也可能是会管，生命周期结束.
fetal参数指明它是否遇到了一个panic，fetal是对应recover()返回的信息
如果 fetal != nil 则回滚
否则
如果 e == nil 则提交
否则 回滚
*/
func (tc *TransContext) CompleteWithPanic(e error, fetal any) {
	if fetal != nil {
		tc.complete(metRecover)
		panic(fetal)
	}
	tc.complete(e)
}

// complete 事务最终完成，可能是提交，也可能是回滚，生命周期结束. e == nil, 提交事务，否则回滚
func (tc *TransContext) complete(e error) {
	if e != nil {
		GLogger.Error(tc.ctx, e)
	}
	if tc.status == tcStatusInvalid {
		return
	}
	if tc.txRequest == txrequest.RequestNone {
		closeConn(tc)
		tc.status = tcStatusInvalid
		return
	}
	if tc.status == tcStatusInit {
		var err error
		if e != nil {
			err = tc.tx.Rollback()
		} else {
			err = tc.tx.Commit()
		}
		if err != nil {
			GLogger.Error(tc.ctx, err)
		}
		closeConn(tc)
		tc.status = tcStatusInvalid
	}
}

func closeConn(tc *TransContext) {
	if err := tc.conn.Close(); err != nil {
		GLogger.Error(tc.ctx, err)
	}
}

func buildContext(gid uint64, traceId string) context.Context {
	mp := map[string]any{}
	mp[goroutineID] = gid
	mp[TraceID] = traceId
	return context.WithValue(context.Background(), ctxValues, mp)
}

// GetGoroutineIdFromContext 从 context.Context 中读取启动事务的 goroutine id
func GetGoroutineIdFromContext(ctx context.Context) uint64 {
	values := ctx.Value(ctxValues)
	if values == nil {
		return 0
	}

	if v, ok := values.(map[string]any); ok {
		if vv, okk := v[goroutineID]; okk {
			if vvv, okk := vv.(uint64); okk {
				return vvv
			}
		}
	}

	return 0
}

// GetTraceIdFromContext 从 context.Context 中读取trace id
func GetTraceIdFromContext(ctx context.Context) string {
	values := ctx.Value(ctxValues)
	if values == nil {
		return ""
	}

	if v, ok := values.(map[string]any); ok {
		if vv, okk := v[TraceID]; okk {
			if vvv, okk := vv.(string); okk {
				return vvv
			}
		}
	}
	return ""
}
