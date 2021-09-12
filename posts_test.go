package optimistic_locking

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

var (
	testCtx = context.Background()
)

func withAllPosts(t *testing.T, f func(t *testing.T, p Posts)) {
	tt := []struct {
		name    string
		factory func(path string) (Posts, error)
	}{
		{
			name: "broken posts",
			factory: func(path string) (Posts, error) {
				return NewBrokenPosts(path)
			},
		},
		{
			name: "versioned posts",
			factory: func(path string) (Posts, error) {
				return NewVersionedPosts(path)
			},
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			withPosts(t, ts.factory, f)
		})
	}
}

func withPosts(t *testing.T, factory func(path string) (Posts, error), callback func(t *testing.T, p Posts)) {
	file, err := ioutil.TempFile("", "prefix")
	if err != nil {
		t.Fatalf("failed to create tmp file: %v", err)
	}
	defer os.Remove(file.Name())

	posts, err := factory(file.Name())
	if err != nil {
		t.Fatalf("failed to create Posts: %v", err)
	}

	if err := posts.Migrate(testCtx); err != nil {
		t.Fatalf("failed to migrate database: %v", err)
	}

	callback(t, posts)
}

func TestSqlitePosts_Save(t *testing.T) {
	withAllPosts(t, func(t *testing.T, p Posts) {
		post := samplePost(t)

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

func TestSqlitePosts_Delete(t *testing.T) {
	withAllPosts(t, func(t *testing.T, p Posts) {
		result, err := p.Delete(testCtx, "post that is not there")
		require.NoError(t, err)
		assert.False(t, result)

		post := samplePost(t)

		require.NoError(t, p.Save(testCtx, post))

		result, err = p.Delete(testCtx, post.UUID)
		require.NoError(t, err)
		assert.True(t, result)

		_, err = p.Find(testCtx, post.UUID)
		assert.EqualError(t, err, fmt.Sprintf("could not find post with UUID: %v: sql: no rows in result set", post.UUID))
	})
}

func randomString(t *testing.T) string {
	bytes := make([]byte, 128)
	_, err := rand.Read(bytes)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(bytes)
}

func TestSqlitePosts_List(t *testing.T) {
	withAllPosts(t, func(t *testing.T, p Posts) {
		total := 10
		posts := make([]*Post, 0, total)

		for x := 0; x < total; x++ {
			post := samplePost(t)

			require.NoError(t, p.Save(testCtx, post))

			posts = append(posts, post)
		}

		result, err := p.List(testCtx)
		require.NoError(t, err)

		assert.EqualValues(t, posts, result)
	})
}

func samplePost(t *testing.T) *Post {
	return &Post{
		Title:   randomString(t),
		Content: randomString(t),
	}
}

func TestVersionedPosts_Save(t *testing.T) {
	tt := []struct {
		name    string
		err     string
		factory func(path string) (Posts, error)
	}{
		{
			name: "with versioned posts",
			err:  "version mismatch: you're trying to update post with version",
			factory: func(path string) (Posts, error) {
				return NewVersionedPosts(path)
			},
		},
		{
			name: "with broken posts",
			factory: func(path string) (Posts, error) {
				return NewBrokenPosts(path)
			},
		},
	}

	for _, ts := range tt {
		t.Run(ts.name, func(t *testing.T) {
			withPosts(t, ts.factory, func(t *testing.T, p Posts) {
				post := samplePost(t)

				require.NoError(t, p.Save(testCtx, post))

				post.Title = "new title"
				require.NoError(t, p.Save(testCtx, post))

				savedPost, err := p.Find(testCtx, post.UUID)
				require.NoError(t, err)

				savedPost.Content = "new content"
				require.NoError(t, p.Save(testCtx, savedPost))

				post.Content = "this will overwrite"
				err = p.Save(testCtx, post)

				if ts.err != "" {
					require.NotNil(t, err)
					assert.Contains(t, err.Error(), ts.err)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}
