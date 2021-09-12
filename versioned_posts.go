package optimistic_locking

import (
	"context"
	"database/sql"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func NewVersionedPosts(path string) (*VersionedPosts, error) {
	posts, err := NewBrokenPosts(path)
	if err != nil {
		return nil, err
	}

	return &VersionedPosts{
		BrokenSqlitePosts: posts,
	}, nil
}

type VersionedPosts struct {
	*BrokenSqlitePosts
}

func (s *VersionedPosts) Save(ctx context.Context, post *Post) (err error) {
	if post.UUID == "" {
		post.Version = uuid.New().String()
		return s.BrokenSqlitePosts.Save(ctx, post)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				log.Err(txErr).Str("uuid", post.UUID).Msg("failed to rollback transaction")
			}
		}
	}()

	result, err := findWith(ctx, tx, post.UUID)
	if err != nil {
		return err
	}

	if result.Version != post.Version {
		return fmt.Errorf("version mismatch: you're trying to update post with version %v but the current DB version is %v", post.Version, result.Version)
	}

	post.Version = uuid.New().String()

	if err := assertAffected(sq.Update(PostsTable).
		Where(sq.Eq{"uuid": post.UUID}).
		Set("title", post.Title).
		Set("content", post.Content).
		Set("version", post.Version).
		RunWith(tx).
		ExecContext(ctx)); err != nil {
		return err
	}

	return tx.Commit()
}
