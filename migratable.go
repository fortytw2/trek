package trek

import (
	"github.com/fortytw2/lounge"
)

type MigratableDB interface {
	ApplyMigrations([]Migration) error
	LatestMigrationName() (string, error)

	Lock() (bool, error)
	Unlock() error
}

type Migration struct {
	Name string
	SQL  string
}

func Migrate(db MigratableDB, log lounge.Log, allMigrations []Migration) error {
	locked, err := db.Lock()
	if err != nil {
		return err
	}

	if locked {
		log.Infof("migrations lock already held, not running migrations")
		return nil
	}

	latestMigrationName, err := db.LatestMigrationName()
	if err != nil {
		return err
	}

	migrationsToRun := getMigrationsAfter(allMigrations, latestMigrationName)
	err = db.ApplyMigrations(migrationsToRun)
	if err != nil {
		log.Errorf("cannot apply migrations: %s", err)
		return err
	}

	return nil
}

func getMigrationsAfter(migrations []Migration, latestName string) []Migration {
	var out []Migration
	for _, m := range migrations {
		if m.Name > latestName {
			out = append(out, m)
		}
	}

	return out
}
