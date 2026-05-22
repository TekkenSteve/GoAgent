package webapi

import (
	"time"

	"github.com/knadh/koanf/providers/file"
)

// WatchLLMConfig monitors the LLM YAML config file at path for changes.
// onReload is called once with the initial load and again on every file change.
func WatchLLMConfig(path string, onReload func(*LLMConfigFile)) error {
	cfg, err := LoadLLMConfigFile(path)
	if err != nil {
		return err
	}

	onReload(cfg)

	go func() {
		for {
			fp := file.Provider(path)
			if err := fp.Watch(func(_ any, err error) {
				if err != nil {
					return
				}

				cfg, err := LoadLLMConfigFile(path)
				if err != nil {
					return
				}

				onReload(cfg)
			}); err != nil {
				time.Sleep(time.Second)

				continue
			}

			break
		}
	}()

	return nil
}
