package optimistic_locking

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var (
	testCtx = context.Background()
)

func withPosts(t *testing.T, f func(t *testing.T, p Posts)) {
	file, err := ioutil.TempFile("", "prefix")
	if err != nil {
		t.Fatalf("failed to create tmp file: %v", err)
	}
	defer os.Remove(file.Name())

	posts, err := NewBrokenPosts(file.Name())
	if err != nil {
		t.Fatalf("failed to create Posts: %v", err)
	}

	if err := posts.Migrate(testCtx); err != nil {
		t.Fatalf("failed to migrate database: %v", err)
	}

	f(t, posts)
}

func TestSqlitePosts_Save(t *testing.T) {
	tt := []struct {
		name       string
		post       *Post
		expectPost bool
		err        string
	}{
		{
			name: "finds the post it has inserted",
			post: &Post{
				Title:   "sample title",
				Content: "sample content",
			},
			expectPost: true,
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			withPosts(t, func(t *testing.T, p Posts) {
				require.NoError(t, p.Save(testCtx, ts.post))

				found, err := p.Find(testCtx, ts.post.UUID)
				if ts.err != "" {
					assert.EqualError(t, err, ts.err)
				} else {
					assert.NoError(t, err)
				}

				if ts.expectPost {
					assert.Equal(t, ts.post, found)
				} else {
					assert.Nil(t, found)
				}
			})
		})
	}
}
