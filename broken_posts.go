package optimistic_locking

import (
	"context"
	"database/sql"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

func NewBrokenPosts(path string) (*BrokenSqlitePosts, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &BrokenSqlitePosts{
		db: db,
	}, err
}

type BrokenSqlitePosts struct {
	db *sql.DB
}

func (s *BrokenSqlitePosts) Delete(ctx context.Context, uuid string) (bool, error) {
	result, err := sq.Delete(PostsTable).Where(sq.Eq{"uuid": uuid}).RunWith(s.db).ExecContext(ctx)
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (s *BrokenSqlitePosts) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, CreatePostsTable)
	return err
}

func (s *BrokenSqlitePosts) Close() error {
	return s.db.Close()
}

func (s *BrokenSqlitePosts) Find(ctx context.Context, uuid string) (*Post, error) {
	return findWith(ctx, s.db, uuid)
}

func findWith(ctx context.Context, runner sq.BaseRunner, uuid string) (*Post, error) {
	scanner := sq.Select("uuid", "title", "content", "version").
		From(PostsTable).
		Where("uuid = ?", uuid).
		RunWith(runner).
		QueryRowContext(ctx)

	post := &Post{}

	if err := scanner.Scan(&post.UUID, &post.Title, &post.Content, &post.Version); err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.Wrapf(err, "could not find post with UUID: %v", uuid)
		}

		return nil, err
	}

	return post, nil
}

func (s *BrokenSqlitePosts) Save(ctx context.Context, post *Post) (err error) {
	if post.UUID == "" {
		post.UUID = uuid.New().String()

		return assertAffected(sq.Insert(PostsTable).
			Columns("uuid", "title", "content", "version").
			Values(post.UUID, post.Title, post.Content, post.Version).
			RunWith(s.db).
			PlaceholderFormat(sq.Dollar).
			ExecContext(ctx))
	}

	return assertAffected(sq.Update(PostsTable).
		Where(sq.Eq{"uuid": post.UUID}).
		Set("title", post.Title).
		Set("content", post.Content).
		Set("version", post.Version).
		RunWith(s.db).
		ExecContext(ctx))
}

func assertAffected(r sql.Result, err error) error {
	if err != nil {
		return err
	}

	affected, err := r.RowsAffected()
	if err != nil {
		return err
	}

	if affected != 1 {
		return fmt.Errorf("expected only %v row to be affected but %v rows were affected", 1, affected)
	}

	return nil
}

func (s *BrokenSqlitePosts) List(ctx context.Context) ([]*Post, error) {
	posts := make([]*Post, 0, 10)

	rows, err := sq.Select("uuid", "title", "content", "version").
		From(PostsTable).
		RunWith(s.db).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		post := &Post{}
		if err := rows.Scan(&post.UUID, &post.Title, &post.Content, &post.Version); err != nil {
			return nil, err
		}

		posts = append(posts, post)
	}

	return posts, nil
}
