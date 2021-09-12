package optimistic_locking

import (
	"context"
	"io"
)

const (
	PostsTable       = "posts"
	CreatePostsTable = `DROP TABLE IF EXISTS posts;
CREATE TABLE posts (uuid VARCHAR NOT NULL PRIMARY KEY, title TEXT NOT NULL, content TEXT NOT NULL);`
)

type Post struct {
	UUID    string
	Title   string
	Content string
}

type Posts interface {
	io.Closer
	Find(ctx context.Context, uuid string) (*Post, error)
	Save(ctx context.Context, post *Post) error
	List(ctx context.Context) ([]*Post, error)
	Migrate(ctx context.Context) error
}
