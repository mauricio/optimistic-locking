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

func NewBrokenPosts(path string) (Posts, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &sqlitePosts{
		db: db,
	}, err
}

type sqlitePosts struct {
	db *sql.DB
}

func (s *sqlitePosts) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, CreatePostsTable)
	return err
}

func (s *sqlitePosts) Close() error {
	return s.db.Close()
}

func (s *sqlitePosts) Find(ctx context.Context, uuid string) (*Post, error) {
	scanner := sq.Select("uuid", "title", "content").
		From(PostsTable).
		Where("uuid = ?", uuid).
		RunWith(s.db).
		QueryRowContext(ctx)

	post := &Post{}

	if err := scanner.Scan(&post.UUID, &post.Title, &post.Content); err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.Wrapf(err, "could not find post with UUID: %v", uuid)
		}

		return nil, err
	}

	return post, nil
}

func (s *sqlitePosts) Save(ctx context.Context, post *Post) (err error) {
	var result sql.Result

	if post.UUID == "" {
		post.UUID = uuid.New().String()
		result, err = sq.Insert(PostsTable).
			Columns("uuid", "title", "content").
			Values(post.UUID, post.Title, post.Content).
			RunWith(s.db).
			PlaceholderFormat(sq.Dollar).
			ExecContext(ctx)
	} else {
		result, err = sq.Update(PostsTable).
			Where(sq.Eq{"uuid": post.UUID}).
			Set("title", post.Title).
			Set("content", post.Content).
			ExecContext(ctx)
	}

	if err != nil {
		return err
	}

	return assertAffected(result, 1)
}

func assertAffected(r sql.Result, expected int) error {
	affected, err := r.RowsAffected()
	if err != nil {
		return err
	}

	if affected != 1 {
		return fmt.Errorf("expected only %v row to be affected but %v rows were affected", expected, affected)
	}

	return nil
}

func (s *sqlitePosts) List(ctx context.Context) ([]*Post, error) {
	posts := make([]*Post, 0, 10)

	rows, err := sq.Select("uuid", "title", "content").
		From(PostsTable).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		post := &Post{}
		if err := rows.Scan(&post.UUID, &post.Title, &post.Content); err != nil {
			return nil, err
		}

		posts = append(posts, post)
	}

	return posts, nil
}
