package trek

import (
	"github.com/fortytw2/lounge"
)

type MigratableDB interface {
	ApplyMigrations(lounge.Log, []Migration) error
}

type Migration struct {
	Name string
	SQL  string
}

func Migrate(db MigratableDB, log lounge.Log, allMigrations []Migration) (err error) {
	err = db.ApplyMigrations(log, allMigrations)
	if err != nil {
		log.Errorf("cannot apply migrations: %s", err)
		return err
	}

	return nil
}

func GetMigrationsAfter(migrations []Migration, latestName string) []Migration {
	var out []Migration
	for _, m := range migrations {
		if m.Name > latestName {
			out = append(out, m)
		}
	}

	return out
}
