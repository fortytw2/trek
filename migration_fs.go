package trek

import (
	"fmt"
	"io/fs"
	"path/filepath"
)

func GetMigrations(from fs.FS) ([]Migration, error) {
	var migrations []Migration

	err := fs.WalkDir(from, ".", func(path string, d fs.DirEntry, err error) error {
		// cannot happen
		if err != nil {
			panic(err)
		}

		if d.IsDir() {
			return nil
		}

		b, err := fs.ReadFile(from, path)
		if err != nil {
			return err // or panic or ignore
		}

		ext := filepath.Ext(path)
		if ext != ".sql" {
			return fmt.Errorf("file not ending in .sql found in migrations: %s", path)
		}

		migrations = append(migrations, Migration{
			Name: path,
			SQL:  string(b),
		})

		return nil
	})

	return migrations, err
}
