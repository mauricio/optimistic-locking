package optimistic_locking

import (
	"context"
	"io"
)

const (
	PostsTable       = "posts"
	CreatePostsTable = `DROP TABLE IF EXISTS posts;
CREATE TABLE posts (uuid VARCHAR NOT NULL PRIMARY KEY, title TEXT NOT NULL, content TEXT NOT NULL, version VARCHAR NOT NULL);`
)

type Post struct {
	UUID    string
	Title   string
	Content string
	Version string
}

type Posts interface {
	io.Closer
	// Find tries to find a post, returns an error if the post does not exist
	Find(ctx context.Context, uuid string) (*Post, error)
	Save(ctx context.Context, post *Post) error
	List(ctx context.Context) ([]*Post, error)
	// Delete deletes a post and returns whether it actually found the post to be deleted or not.
	Delete(ctx context.Context, uuid string) (bool, error)
	Migrate(ctx context.Context) error
}
