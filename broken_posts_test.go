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

func TestSqlitePosts_SaveCreating(t *testing.T) {
	withPosts(t, func(t *testing.T, p Posts) {
		post := &Post{
			Title:   "sample post",
			Content: "sample content",
		}

		require.NoError(t, p.Save(testCtx, post))

		found, err := p.Find(testCtx, post.UUID)
		require.NoError(t, err)

		assert.Equal(t, post, found)

		post.Content = "Updated content"
		require.NoError(t, p.Save(testCtx, post))

		found, err = p.Find(testCtx, post.UUID)
		require.NoError(t, err)
		assert.Equal(t, post, found)
	})
}
