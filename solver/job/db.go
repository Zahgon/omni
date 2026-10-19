// Package job provides a simple event log job database.
package job

import (
	"context"
	"sync"

	"github.com/omni-network/omni/lib/contracts/solvernet"
	"github.com/omni-network/omni/lib/errors"
	"github.com/omni-network/omni/lib/umath"

	"github.com/ethereum/go-ethereum/core/types"

	ormv1alpha1 "cosmossdk.io/api/cosmos/orm/v1alpha1"
	"cosmossdk.io/core/store"
	"cosmossdk.io/orm/model/ormdb"
	"cosmossdk.io/orm/types/ormerrors"
	dbm "github.com/cosmos/cosmos-db"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// New returns a new job DB backed by the given cosmos db.
func New(db dbm.DB) (*DB, error) {
	schema := &ormv1alpha1.ModuleSchemaDescriptor{SchemaFile: []*ormv1alpha1.ModuleSchemaDescriptor_FileEntry{
		{Id: 1, ProtoFileName: File_solver_job_job_proto.Path()},
	}}

	storeSvc := dbStoreService{DB: db}

	modDB, err := ormdb.NewModuleDB(schema, ormdb.ModuleDBOptions{KVStoreService: storeSvc})
	if err != nil {
		return nil, errors.Wrap(err, "create ormdb module db")
	}

	dbStore, err := NewJobStore(modDB)
	if err != nil {
		return nil, errors.Wrap(err, "create store")
	}

	return &DB{
		table: dbStore.JobTable(),
	}, nil
}

// DB implements an event log job database.
// Current iteration simply stores event logs, deduping by chainID, blockHeight, and eventIndex.
// No specific job scheduling supported, since a single goroutine per job is assumed.
// That goroutine simply retries until the job is done, then deletes it.
type DB struct {
	table JobTable
	mu    sync.RWMutex
}

// All returns all jobs in the database.
func (db *DB) All(ctx context.Context) ([]*Job, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter, err := db.table.List(ctx, JobPrimaryKey{})
	if err != nil {
		return nil, errors.Wrap(err, "list all ids")
	}
	defer iter.Close()

	var jobs []*Job
	for iter.Next() {
		job, err := iter.Value()
		if err != nil {
			return nil, errors.Wrap(err, "get value")
		}

		jobs = append(jobs, proto.Clone(job).(*Job)) //nolint:forcetypeassert // Type known
	}

	return jobs, nil
}

// Delete removes a job from the database.
func (db *DB) Delete(ctx context.Context, id uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	err := db.table.DeleteBy(ctx, JobIdIndexKey{}.WithId(id))
	if err != nil {
		return errors.Wrap(err, "delete job")
	}

	return nil
}

// Exists returns true if the job exists in the database.
func (db *DB) Exists(ctx context.Context, id uint64) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	ok, err := db.table.Has(ctx, id)
	if err != nil {
		return false, errors.Wrap(err, "has job")
	}

	return ok, nil
}

// Insert adds a new job to the database returning the created job.
// It is idempotent, and will not insert the same job twice, instead returning the existing job.
// It however errors if re-inserting reorged events, this isn't supported/expected.
func (db *DB) Insert(ctx context.Context, chainID uint64, height uint64, tx string, index uint64, orderID []byte, status uint64) (*Job, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if j, ok, err := db.getUnique(ctx, chainID, height, tx, index); err != nil {
		return nil, err
	} else if ok {
		return j, nil
	}

	j := &Job{
		ChainId:    chainID,
		Height:     height,
		EventIndex: index,
		TxString:   tx,
		OrderId:    orderID,
		Status:     status,
		CreatedAt:  timestamppb.Now(),
	}

	id, err := db.table.InsertReturningId(ctx, j)
	if err != nil {
		return nil, errors.Wrap(err, "insert job")
	}

	resp := proto.Clone(j).(*Job) //nolint:revive,forcetypeassert // Type known
	resp.Id = id

	return resp, nil
}

// InsertLog adds a new job to the database returning the created job.
// It is idempotent, and will not insert the same job twice, instead returning the existing job.
// It however errors if re-inserting reorged events, this isn't supported/expected.
func (db *DB) InsertLog(ctx context.Context, chainID uint64, elog types.Log) (*Job, error) {
	index, err := umath.ToUint64(elog.Index)
	if err != nil {
		return nil, err
	}

	orderID, status, err := solvernet.ParseEvent(elog)
	if err != nil {
		return nil, err
	}

	statusU64, err := umath.ToUint64(status)
	if err != nil {
		return nil, err
	}

	return db.Insert(ctx, chainID, elog.BlockNumber, elog.TxHash.String(), index, orderID[:], statusU64)
}

func (db *DB) getUnique(ctx context.Context, chainID uint64, height uint64, tx string, index uint64) (*Job, bool, error) {
	j, err := db.table.GetByChainIdHeightTxStringEventIndex(ctx, chainID, height, tx, index)
	if ormerrors.IsNotFound(err) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, errors.Wrap(err, "get unique job")
	}

	return j, true, nil
}

// dbStoreService wraps a cosmos-db instance and provides it via OpenKVStore.
type dbStoreService struct {
	dbm.DB
}

func (db dbStoreService) OpenKVStore(context.Context) store.KVStore {
	return db.DB
}
