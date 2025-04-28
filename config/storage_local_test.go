package config

import (
	"reflect"
	"testing"

	"github.com/minamijoyo/tfmigrate/storage"
	"github.com/minamijoyo/tfmigrate/storage/local"
)

func TestParseLocalStorageBlock(t *testing.T) {
	cases := []struct {
		desc   string
		env    map[string]string
		source string
		want   storage.Config
		ok     bool
	}{
		{
			desc: "valid",
			source: `
tfmigrate {
  history {
    storage "local" {
      path = "tmp/history.json"
    }
  }
}
`,
			want: &local.Config{
				Path: "tmp/history.json",
			},
			ok: true,
		},
		{
			desc: "env vars",
			env: map[string]string{
				"VAR_NAME": "env1",
			},
			source: `
tfmigrate {
  history {
    storage "local" {
      path = "tmp/${env.VAR_NAME}/history.json"
    }
  }
}
`,
			want: &local.Config{
				Path: "tmp/env1/history.json",
			},
			ok: true,
		},
		{
			desc: "missing required attribute (path)",
			source: `
tfmigrate {
  history {
    storage "local" {
    }
  }
}
`,
			want: nil,
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			config, err := ParseConfigurationFile("test.hcl", []byte(tc.source))
			if tc.ok && err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("expected to return an error, but no error, got: %#v", config)
			}
			if tc.ok {
				got := config.History.Storage
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("got: %#v, want: %#v", got, tc.want)
				}
			}
		})
	}
}
